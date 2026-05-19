import os
import time
import random
import threading
import traceback
import requests
from typing import Union, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.shared.dbx_utils import sync_to_dbx_table, scd2_sync_employees

# ── Databricks Logging Setup ───────────────────────────────────────────────────
import builtins
import datetime

_log_buffer = []
_log_lock = threading.Lock()

def dbx_log(msg):
    msg_str = str(msg)
    level = "INFO"
    if msg_str.startswith("[WARN]"):
        level = "WARN"
        builtins.print(msg)
    elif msg_str.startswith("[ERROR]"):
        level = "ERROR"
        builtins.print(msg)
    elif msg_str.startswith("[INFO]"):
        level = "INFO"
        # Suppress printing INFO to terminal to avoid clutter
    else:
        # Default behavior for anything else
        builtins.print(msg)

    with _log_lock:
        _log_buffer.append({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": level,
            "message": msg_str
        })

def flush_dbx_logs():
    with _log_lock:
        if not _log_buffer:
            return
        logs_to_write = list(_log_buffer)
        _log_buffer.clear()
        
    if logs_to_write:
        try:
            from backend.shared.dbx_utils import append_to_dbx_table
            append_to_dbx_table("keka_sync_logs", logs_to_write)
        except Exception as e:
            builtins.print(f"[ERROR] Failed to flush logs to Databricks: {e}")

# Override print in this module
print = dbx_log


# ── Constants ──────────────────────────────────────────────────────────────────
PAGE_SIZE = 200  # Keka max → fewest API calls
MAX_RETRIES = 5  # retry attempts on 429 / 5xx
BACKOFF_BASE = 2.0  # exponential-backoff base (seconds)
MIN_REQUEST_INTERVAL = 1.2  # seconds between requests (~50/min safe zone)


# ── Auth ───────────────────────────────────────────────────────────────────────
def get_keka_access_token() -> str:
    """Fetches a short-lived OAuth access token from Keka."""
    response = requests.post(
        "https://login.keka.com/connect/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "PMOXponent/1.0",
            "Accept": "application/json",
        },
        data={
            "grant_type": "kekaapi",
            "scope": "kekaapi",
            "client_id": os.getenv("KEKA_CLIENT_ID"),
            "client_secret": os.getenv("KEKA_CLIENT_SECRET"),
            "api_key": os.getenv("KEKA_API_KEY"),
        },
        timeout=30,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    print("[INFO] Keka access token acquired successfully.")
    return token


def _keka_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "PMOXponent/1.0",
    }


# ── Rate limiter ───────────────────────────────────────────────────────────────
class _RateLimiter:
    """
    Thread-safe rate limiter.
    Enforces a global minimum interval across ALL threads — including the
    per-employee and per-project sub-fetch thread pools — so the 50 req/min
    Keka limit is never breached regardless of concurrency level.
    """

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_call
            sleep_for = self._interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last_call = time.monotonic()


_rate_limiter = _RateLimiter(MIN_REQUEST_INTERVAL)


# ── Single-page fetch with retry ───────────────────────────────────────────────
def _fetch_page(
    url: str,
    headers: dict,
    page: int,
    extra_params: dict = None,
) -> Optional[Union[dict, list]]:
    """
    Fetch one page with exponential backoff on 429 / 5xx errors.
    Always calls _rate_limiter.wait() before the HTTP request so every
    single API call — regardless of which thread or pipeline makes it —
    goes through the global rate gate.
    """
    params = {"pageNumber": page, "pageSize": PAGE_SIZE}
    if extra_params:
        params.update(extra_params)

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.wait()

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 0))
                wait = max(retry_after, BACKOFF_BASE**attempt + random.uniform(0, 1))
                print(
                    f"[WARN] Rate limited on page {page} "
                    f"(attempt {attempt}/{MAX_RETRIES}). Sleeping {wait:.1f}s …"
                )
                time.sleep(wait)

            elif resp.status_code >= 500:
                wait = BACKOFF_BASE**attempt + random.uniform(0, 1)
                print(
                    f"[WARN] Server error {resp.status_code} on page {page} "
                    f"(attempt {attempt}/{MAX_RETRIES}). Sleeping {wait:.1f}s …"
                )
                time.sleep(wait)

            else:
                resp.raise_for_status()  # 4xx that isn't 429 → fail immediately

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            wait = BACKOFF_BASE**attempt + random.uniform(0, 1)
            print(
                f"[WARN] Connection/Timeout on page {page} "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}. Sleeping {wait:.1f}s …"
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Failed to fetch page {page} from {url} after {MAX_RETRIES} attempts."
    )


# ── Paginated fetcher ──────────────────────────────────────────────────────────
def _fetch_all_pages(
    url: str,
    headers: dict,
    extra_params: dict = None,
) -> list:
    """
    Fetch all pages from a paginated Keka endpoint.
    Routes every request through _fetch_page → _rate_limiter.wait(),
    so callers never need their own sleep() calls.
    """
    all_data = []
    page = 1
    total_pages = None

    while True:
        print(
            f"[INFO] Fetching page {page}"
            + (f"/{total_pages}" if total_pages else "")
            + f" from {url}"
        )

        body = _fetch_page(url, headers, page, extra_params)

        if isinstance(body, list):
            all_data.extend(body)
            break

        elif isinstance(body, dict):
            data_block = body.get("data", body)

            if isinstance(data_block, dict):
                items = data_block.get("results", data_block.get("data", []))
                if total_pages is None:
                    total_pages = (
                        data_block.get("totalPages")
                        or data_block.get("pageCount")
                        or body.get("totalPages")
                        or body.get("pageCount")
                        or 1
                    )
            elif isinstance(data_block, list):
                items = data_block
                if total_pages is None:
                    total_pages = body.get("totalPages") or body.get("pageCount") or 1
            else:
                items = []

            if not items:
                print("[INFO] Empty page received. Stopping pagination.")
                break

            all_data.extend(items)
            print(
                f"[INFO] Page {page}: received {len(items)} records "
                f"(running total: {len(all_data)})"
            )

            if page >= total_pages:
                break

            page += 1

        else:
            print("[WARN] Unexpected response shape. Stopping pagination.")
            break

    return all_data


# ── Helpers ────────────────────────────────────────────────────────────────────
def _get_existing_ids(execute_query, catalog, schema, table, column) -> set:
    """
    Returns the set of distinct values for `column` already stored in `table`.
    Returns an empty set if the table doesn't exist yet (first run).
    Used by delta-sync logic to skip records already in Databricks.
    """
    try:
        rows = execute_query(
            f"SELECT DISTINCT `{column}` FROM `{catalog}`.`{schema}`.`{table}`",
            use_cache=False,
        )
        return {r[column] for r in rows if r.get(column)}
    except Exception:
        return set()  # table doesn't exist yet — first run, fetch everything


# ── Main sync ──────────────────────────────────────────────────────────────────
def _run_sync_keka_data_to_dbx():
    client_id = os.getenv("KEKA_CLIENT_ID")
    if not client_id:
        print("[INFO] Keka credentials not provided. Skipping sync.")
        return

    print("[INFO] Starting Keka data synchronization...")
    base_url = os.getenv("KEKA_BASE_URL", "").rstrip("/")
    catalog = os.getenv("CATALOG_NAME", "")
    schema = os.getenv("SCHEMA_NAME", "")

    try:
        token = get_keka_access_token()
    except Exception as e:
        print(f"[ERROR] Failed to acquire Keka access token: {e}")
        traceback.print_exc()
        return

    headers = _keka_headers(token)
    sync_errors = []
    _errors_lock = threading.Lock()

    from backend.shared.dbx_utils import (
        merge_to_dbx_table,
        invalidate_dbx_cache,
        execute_query,
    )

    def _append_error(name: str) -> None:
        with _errors_lock:
            sync_errors.append(name)

    # ── Pipeline A: Employees → Skills ────────────────────────────────────────
    def _sync_employee_pipeline():
        import json as _json

        # ── A1: Employees (always full fetch — cheap, needed for SCD2) ────────
        emp_data = []
        emp_table = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees")
        try:
            emp_url = f"{base_url}/api/v1/hris/employees"
            print(f"[INFO] Fetching employees from: {emp_url}")
            emp_data = _fetch_all_pages(emp_url, headers)
            print(f"[INFO] Fetched {len(emp_data)} employees.")
            if emp_data:
                scd2_sync_employees(emp_table, emp_data)
            else:
                print("[WARN] No employee data returned. Skipping employee sync.")
        except Exception as e:
            print(f"[ERROR] Employee sync failed: {e}")
            traceback.print_exc()
            _append_error("employees")

        # ── A2: Skills — DELTA sync ───────────────────────────────────────────
        #
        # WHY IT WAS SLOW:
        #   The original code called time.sleep(0.2) + raw requests.get() inside
        #   a ThreadPoolExecutor(max_workers=5).  That bypassed the rate limiter
        #   and fired ~25 req/s — fast but wrong (caused silent 429 drops).
        #   After fixing the rate limiter, every call correctly waits 1.2s, so
        #   300 employees × 1.2s = 360s = 6 minutes just for skills.
        #
        # THE FIX:
        #   Before calling Keka, check which employee IDs already have skills
        #   in Databricks.  Only fetch skills for employees we haven't seen yet.
        #   On first run: fetches all (same as before).
        #   On subsequent runs: skips employees already synced → near-zero calls.
        # ─────────────────────────────────────────────────────────────────────
        try:
            skills_table = os.getenv(
                "KEKA_EMPLOYEE_SKILLS_TABLE", "keka_employee_skills"
            )
            print("[INFO] Starting skill matrix sync...")

            if not emp_data:
                print("[WARN] No employee data for skill sync. Skipping.")
                return

            # Find which employee IDs are already in Databricks
            existing_emp_ids = _get_existing_ids(
                execute_query, catalog, schema, skills_table, "employeeId"
            )

            # Only call Keka for employees whose skills we've never stored
            employees_to_fetch = [
                e
                for e in emp_data
                if str(e.get("id", "")).strip() not in existing_emp_ids
            ]

            skipped_count = len(emp_data) - len(employees_to_fetch)
            print(
                f"[INFO] Skills delta: {len(employees_to_fetch)} new employees to fetch, "
                f"{skipped_count} already synced (skipped)."
            )

            if not employees_to_fetch:
                print("[INFO] All employees already have skills synced. Skipping.")
                return

            def _fetch_employee_skills(emp):
                emp_uuid = str(emp.get("id", "")).strip()
                emp_number = str(emp.get("employeeNumber", "")).strip()
                if not emp_uuid:
                    return []
                skill_url = f"{base_url}/api/v1/hris/employees/{emp_uuid}/skills"
                try:
                    # Routes through _fetch_all_pages → _fetch_page → _rate_limiter.wait()
                    # Handles pagination + retry automatically. No sleep() needed here.
                    skills_raw = _fetch_all_pages(skill_url, headers)
                    rows = []
                    for skill in skills_raw:
                        flat = {
                            k: (
                                _json.dumps(v)
                                if isinstance(v, (dict, list))
                                else (str(v) if v is not None else None)
                            )
                            for k, v in skill.items()
                        }
                        flat["employeeId"] = emp_uuid
                        flat["employeeNumber"] = emp_number
                        rows.append(flat)
                    return rows
                except Exception as exc:
                    print(f"[WARN] Skills fetch failed for {emp_number}: {exc}")
                    return []

            all_skill_rows = []
            total = len(employees_to_fetch)
            done = 0

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {
                    pool.submit(_fetch_employee_skills, emp): emp
                    for emp in employees_to_fetch
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        all_skill_rows.extend(result)
                    done += 1
                    if done % 50 == 0 or done == total:
                        print(
                            f"[INFO] Skills: {done}/{total} employees processed, "
                            f"{len(all_skill_rows)} records so far."
                        )

            print(f"[INFO] Fetched {len(all_skill_rows)} new skill records.")
            if all_skill_rows:
                merge_to_dbx_table(skills_table, all_skill_rows, ["employeeId", "id"])
            else:
                print("[WARN] No new skill data. Skipping merge.")

        except Exception as e:
            print(f"[ERROR] Skill matrix sync failed: {e}")
            traceback.print_exc()
            _append_error("skill_matrix")

    # ── Pipeline B: Projects → Allocations + Tasks ────────────────────────────
    def _sync_project_pipeline():
        import json as _json
        from backend.shared.dbx_utils import scd2_sync_projects

        alloc_table = os.getenv(
            "KEKA_PROJECT_ALLOCATIONS_TABLE", "keka_project_allocations"
        )
        tasks_table = os.getenv("KEKA_PROJECT_TASKS_TABLE", "keka_project_tasks")

        # ── B1: Projects (always full fetch — needed for SCD2) ────────────────
        proj_data = []
        proj_table = os.getenv("KEKA_PROJECTS_TABLE", "keka_projects")
        try:
            proj_url = f"{base_url}/api/v1/psa/projects"
            print(f"[INFO] Fetching projects from: {proj_url}")
            proj_data = _fetch_all_pages(proj_url, headers)
            print(f"[INFO] Fetched {len(proj_data)} projects.")
            if proj_data:
                scd2_sync_projects(proj_table, proj_data)
            else:
                print("[WARN] No project data returned. Skipping project sync.")
        except Exception as e:
            print(f"[ERROR] Project sync failed: {e}")
            traceback.print_exc()
            _append_error("projects")

        if not proj_data:
            print(
                "[WARN] No project data for allocations/tasks. Skipping sub-pipelines."
            )
            return

        # ── B2: Allocations + Tasks — DELTA sync ──────────────────────────────
        #
        # WHY IT WAS SLOW:
        #   time.sleep(0.3) before _fetch_all_pages() for EVERY project on
        #   EVERY sync. For 100 projects × 2 sub-pipelines = 200 × 0.3s = 60s
        #   of pure wasted sleep, on top of the actual rate-limit wait.
        #
        # THE FIX:
        #   1. Removed the sleep() — _fetch_all_pages handles timing internally.
        #   2. Added delta logic: only call Keka for projects not yet in
        #      Databricks.  New projects = fetch.  Existing = skip entirely.
        # ─────────────────────────────────────────────────────────────────────
        existing_alloc_proj_ids = _get_existing_ids(
            execute_query, catalog, schema, alloc_table, "projectId"
        )
        existing_task_proj_ids = _get_existing_ids(
            execute_query, catalog, schema, tasks_table, "projectId"
        )

        projects_needing_allocs = [
            p
            for p in proj_data
            if str(p.get("id", "")).strip() not in existing_alloc_proj_ids
        ]
        projects_needing_tasks = [
            p
            for p in proj_data
            if str(p.get("id", "")).strip() not in existing_task_proj_ids
        ]

        print(
            f"[INFO] Allocations delta: {len(projects_needing_allocs)} new projects to fetch, "
            f"{len(proj_data) - len(projects_needing_allocs)} already synced (skipped)."
        )
        print(
            f"[INFO] Tasks delta: {len(projects_needing_tasks)} new projects to fetch, "
            f"{len(proj_data) - len(projects_needing_tasks)} already synced (skipped)."
        )

        # ── Flatten helpers ───────────────────────────────────────────────────
        def _flatten_allocation(alloc, project_id):
            flat = {
                "id": alloc.get("id", ""),
                "projectId": project_id,
                "employeeId": "",
                "employeeFirstName": "",
                "employeeLastName": "",
                "employeeEmail": "",
                "startDate": alloc.get("startDate", ""),
                "endDate": alloc.get("endDate", ""),
                "allocationPercentage": str(alloc.get("allocationPercentage", "")),
                "billingRoleId": "",
                "billingRoleName": "",
                "billingRateUnit": "",
                "billingRate": "",
            }
            emp = alloc.get("employee")
            if isinstance(emp, dict):
                flat["employeeId"] = emp.get("id", "")
                flat["employeeFirstName"] = emp.get("firstName", "")
                flat["employeeLastName"] = emp.get("lastName", "")
                flat["employeeEmail"] = emp.get("email", "")
            br = alloc.get("billingRole")
            if isinstance(br, dict):
                flat["billingRoleId"] = br.get("id", "")
                flat["billingRoleName"] = br.get("name", "")
            brate = alloc.get("billingRate")
            if isinstance(brate, dict):
                flat["billingRateUnit"] = str(brate.get("unit", ""))
                flat["billingRate"] = str(brate.get("rate", ""))
            return flat

        def _flatten_task(task, project_id):
            flat = {
                "id": task.get("id", ""),
                "projectId": project_id,
                "name": task.get("name", ""),
                "description": task.get("description", ""),
                "taskType": task.get("taskType", ""),
                "billingType": task.get("billingType", ""),
                "status": task.get("status", ""),
                "startDate": task.get("startDate", ""),
                "endDate": task.get("endDate", ""),
                "estimatedHours": str(task.get("estimatedHours", "")),
                "actualHours": str(task.get("actualHours", "")),
                "phaseId": "",
                "phaseName": "",
                "billingRoleId": "",
                "billingRoleName": "",
            }
            phase = task.get("phase")
            if isinstance(phase, dict):
                flat["phaseId"] = phase.get("id", "")
                flat["phaseName"] = phase.get("name", "")
            br = task.get("billingRole")
            if isinstance(br, dict):
                flat["billingRoleId"] = br.get("id", "")
                flat["billingRoleName"] = br.get("name", "")
            assigned = task.get("assignedEmployees") or task.get("employees") or []
            flat["assignedEmployees"] = (
                _json.dumps(assigned)
                if isinstance(assigned, (list, dict))
                else (str(assigned) if assigned else "")
            )
            for k, v in task.items():
                if k not in flat and k not in (
                    "phase",
                    "billingRole",
                    "assignedEmployees",
                    "employees",
                ):
                    flat[k] = (
                        _json.dumps(v)
                        if isinstance(v, (dict, list))
                        else (str(v) if v is not None else None)
                    )
            return flat

        # No sleep() here — _fetch_all_pages handles rate limiting internally
        def _fetch_project_allocations(proj):
            proj_id = str(proj.get("id", "")).strip()
            if not proj_id:
                return []
            try:
                allocs = _fetch_all_pages(
                    f"{base_url}/api/v1/psa/projects/{proj_id}/allocations", headers
                )
                return [
                    _flatten_allocation(a, proj_id)
                    for a in allocs
                    if isinstance(a, dict)
                ]
            except Exception as exc:
                print(f"[WARN] Allocations fetch failed for project {proj_id}: {exc}")
                return []

        def _fetch_project_tasks(proj):
            proj_id = str(proj.get("id", "")).strip()
            if not proj_id:
                return []
            try:
                tasks = _fetch_all_pages(
                    f"{base_url}/api/v1/psa/projects/{proj_id}/tasks", headers
                )
                return [_flatten_task(t, proj_id) for t in tasks if isinstance(t, dict)]
            except Exception as exc:
                print(f"[WARN] Tasks fetch failed for project {proj_id}: {exc}")
                return []

        def _run_allocations():
            if not projects_needing_allocs:
                print("[INFO] No new projects need allocation sync. Skipping.")
                return
            try:
                print("[INFO] Starting project allocations sync...")
                all_alloc_rows = []
                total = len(projects_needing_allocs)
                done = 0
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = {
                        pool.submit(_fetch_project_allocations, p): p
                        for p in projects_needing_allocs
                    }
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            all_alloc_rows.extend(result)
                        done += 1
                        if done % 20 == 0 or done == total:
                            print(
                                f"[INFO] Allocations: {done}/{total} projects, "
                                f"{len(all_alloc_rows)} records."
                            )
                print(f"[INFO] Fetched {len(all_alloc_rows)} allocation records.")
                if all_alloc_rows:
                    merge_to_dbx_table(
                        alloc_table, all_alloc_rows, ["projectId", "employeeId"]
                    )
                else:
                    print("[WARN] No allocation data. Skipping merge.")
            except Exception as e:
                print(f"[ERROR] Project allocations sync failed: {e}")
                traceback.print_exc()
                _append_error("project_allocations")

        def _run_tasks():
            if not projects_needing_tasks:
                print("[INFO] No new projects need task sync. Skipping.")
                return
            try:
                print("[INFO] Starting project tasks sync...")
                all_task_rows = []
                total = len(projects_needing_tasks)
                done = 0
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = {
                        pool.submit(_fetch_project_tasks, p): p
                        for p in projects_needing_tasks
                    }
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            all_task_rows.extend(result)
                        done += 1
                        if done % 20 == 0 or done == total:
                            print(
                                f"[INFO] Tasks: {done}/{total} projects, "
                                f"{len(all_task_rows)} records."
                            )
                print(f"[INFO] Fetched {len(all_task_rows)} task records.")
                if all_task_rows:
                    merge_to_dbx_table(tasks_table, all_task_rows, ["projectId", "id"])
                else:
                    print("[WARN] No task data. Skipping merge.")
            except Exception as e:
                print(f"[ERROR] Project tasks sync failed: {e}")
                traceback.print_exc()
                _append_error("project_tasks")

        # Allocations and tasks run in parallel after projects are loaded
        with ThreadPoolExecutor(max_workers=2) as sub_pool:
            alloc_f = sub_pool.submit(_run_allocations)
            tasks_f = sub_pool.submit(_run_tasks)
            for f in as_completed([alloc_f, tasks_f]):
                f.result()  # exceptions handled inside each function

    # ── Pipeline C: Project Resources ─────────────────────────────────────────
    def _sync_project_resources():
        try:
            res_url = f"{base_url}/api/v1/psa/project/resources"
            print(f"[INFO] Fetching project resources from: {res_url}")
            res_data = _fetch_all_pages(res_url, headers)
            print(f"[INFO] Fetched {len(res_data)} project resources.")
            res_table = os.getenv(
                "KEKA_PROJECT_RESOURCES_TABLE", "keka_project_resources"
            )
            merge_to_dbx_table(
                res_table,
                res_data if res_data else [],
                ["projectId", "employeeId"],
            )
            if not res_data:
                print("[WARN] No project resources returned. Ensuring table exists.")
        except Exception as e:
            print(f"[ERROR] Project resources sync failed: {e}")
            traceback.print_exc()
            _append_error("project_resources")

    # ── Pipeline D: Time Entries — already delta (date window) ────────────────
    def _sync_time_entries():
        try:
            import datetime
            from backend.shared.dbx_utils import merge_timeentries

            time_table = os.getenv("KEKA_TIMEENTRIES_TABLE", "keka_timeentries")
            time_url = f"{base_url}/api/v1/psa/timeentries"
            print(f"[INFO] Fetching time entries from: {time_url}")

            end_date = datetime.date.today()
            start_date = None
            is_backfill = False

            try:
                full_table = f"`{catalog}`.`{schema}`.`{time_table}`"
                res = execute_query(
                    f"SELECT MAX(date) as max_date, COUNT(*) as total_rows FROM {full_table}",
                    use_cache=False,
                )
                if (
                    res
                    and res[0].get("max_date")
                    and int(res[0].get("total_rows", 0) or 0) > 0
                ):
                    md_str = res[0]["max_date"]
                    max_dt = datetime.datetime.strptime(
                        md_str.split("T")[0], "%Y-%m-%d"
                    ).date()
                    start_date = max_dt - datetime.timedelta(days=14)
                    print(
                        f"[INFO] Time entries table has data. "
                        f"Using 14-day buffer from {start_date}."
                    )
                else:
                    is_backfill = True
                    start_date = end_date.replace(year=end_date.year - 2)
                    print(
                        f"[WARN] Time entries table empty. "
                        f"Running 2-year backfill from {start_date}."
                    )
            except Exception:
                is_backfill = True
                start_date = end_date.replace(year=end_date.year - 2)
                print(
                    f"[INFO] Time entries table not found. "
                    f"Running 2-year backfill from {start_date}."
                )

            if start_date > end_date:
                start_date = end_date

            all_time_entries = []
            current_from = start_date
            chunk_count = 0

            while current_from <= end_date:
                current_to = min(current_from + datetime.timedelta(days=50), end_date)
                chunk_count += 1
                print(
                    f"[INFO] Fetching time entries chunk {chunk_count}: "
                    f"{current_from} → {current_to}"
                )
                chunk_data = _fetch_all_pages(
                    time_url,
                    headers,
                    extra_params={
                        "from": current_from.isoformat(),
                        "to": current_to.isoformat(),
                    },
                )
                if chunk_data:
                    all_time_entries.extend(chunk_data)
                current_from = current_to + datetime.timedelta(days=1)

            mode_label = (
                "backfill (2 years)" if is_backfill else "delta (14-day buffer)"
            )
            print(
                f"[INFO] Fetched {len(all_time_entries)} time entries "
                f"({mode_label}, {chunk_count} chunks)."
            )
            if all_time_entries:
                merge_timeentries(time_table, all_time_entries)
            else:
                print("[WARN] No time entry data returned. Skipping merge.")
        except Exception as e:
            print(f"[ERROR] Time entries sync failed: {e}")
            traceback.print_exc()
            _append_error("time_entries")

    # ── Pipeline E: Holidays ──────────────────────────────────────────────────
    def _sync_holidays():
        try:
            import json as _json

            holidays_table = os.getenv("KEKA_HOLIDAYS_TABLE", "keka_holidays")
            calendars_url = f"{base_url}/api/v1/time/holidayscalendar"
            print("[INFO] Starting holidays sync...")

            _rate_limiter.wait()
            cal_resp = requests.get(calendars_url, headers=headers, timeout=30)
            cal_resp.raise_for_status()
            cal_body = cal_resp.json()
            calendars = (
                cal_body.get("data", [])
                if isinstance(cal_body, dict)
                else (cal_body if isinstance(cal_body, list) else [])
            )
            if isinstance(calendars, dict):
                calendars = calendars.get("results", [])

            all_holiday_rows = []
            for cal in calendars:
                cal_id = str(cal.get("id", ""))
                cal_name = str(cal.get("name", ""))
                raw_holidays = cal.get("holidays") or cal.get("holidayList") or []

                if not raw_holidays and cal_id:
                    _rate_limiter.wait()
                    sub_resp = requests.get(
                        f"{base_url}/api/v1/time/holidayscalendar/{cal_id}/holidays",
                        headers=headers,
                        timeout=30,
                    )
                    if sub_resp.status_code == 200:
                        sub_body = sub_resp.json()
                        raw_holidays = (
                            sub_body.get("data", [])
                            if isinstance(sub_body, dict)
                            else (sub_body if isinstance(sub_body, list) else [])
                        )

                for h in raw_holidays:
                    if isinstance(h, dict):
                        flat = {
                            k: (
                                _json.dumps(v)
                                if isinstance(v, (dict, list))
                                else (str(v) if v is not None else None)
                            )
                            for k, v in h.items()
                        }
                        flat["calendarId"] = cal_id
                        flat["calendarName"] = cal_name
                        all_holiday_rows.append(flat)

            if all_holiday_rows:
                merge_to_dbx_table(holidays_table, all_holiday_rows, ["id"])
            print(f"[INFO] Holidays sync completed: {len(all_holiday_rows)} records.")
        except Exception as e:
            print(f"[ERROR] Holidays sync failed: {e}")
            _append_error("holidays")

    # ── Pipeline F: Leave Requests — already delta (rolling window) ───────────
    def _sync_leaves():
        try:
            import datetime as _dt

            leaves_table = os.getenv("KEKA_LEAVES_TABLE", "keka_leave_requests")
            leaves_url = f"{base_url}/api/v1/time/leaverequests"
            print("[INFO] Starting leave requests sync...")

            CHUNK_DAYS = 89
            today = _dt.date.today()
            window_start = today.replace(year=today.year - 1, month=1, day=1)
            window_end = today + _dt.timedelta(days=180)

            all_leave_rows = []
            seen_ids = set()
            chunk_start = window_start

            while chunk_start <= window_end:
                chunk_end = min(
                    chunk_start + _dt.timedelta(days=CHUNK_DAYS), window_end
                )
                chunk_data = _fetch_all_pages(
                    leaves_url,
                    headers,
                    extra_params={
                        "from": chunk_start.strftime("%Y-%m-%d"),
                        "to": chunk_end.strftime("%Y-%m-%d"),
                    },
                )
                for row in chunk_data:
                    # Use only real record IDs for dedup — not Python's id(row)
                    row_id = row.get("id") or row.get("leaveRequestId")
                    if row_id:
                        if row_id not in seen_ids:
                            seen_ids.add(row_id)
                            all_leave_rows.append(row)
                    else:
                        all_leave_rows.append(row)
                chunk_start = chunk_end + _dt.timedelta(days=1)

            if all_leave_rows:
                merge_to_dbx_table(leaves_table, all_leave_rows, ["id"])
            print(f"[INFO] Leaves sync completed: {len(all_leave_rows)} records.")
        except Exception as e:
            print(f"[ERROR] Leave requests sync failed: {e}")
            _append_error("leave_requests")

    # ── Pipeline G: Clients ───────────────────────────────────────────────────
    def _sync_clients():
        try:
            clients_table = os.getenv("KEKA_CLIENTS_TABLE", "keka_clients")
            clients_url = f"{base_url}/api/v1/psa/clients"
            print(f"[INFO] Fetching clients from: {clients_url}")
            client_data = _fetch_all_pages(clients_url, headers)
            if client_data:
                merge_to_dbx_table(clients_table, client_data, ["id"])
            print(f"[INFO] Clients sync completed: {len(client_data)} records.")
        except Exception as e:
            print(f"[ERROR] Clients sync failed: {e}")
            _append_error("clients")

    # ── Execute all pipelines concurrently ────────────────────────────────────
    print("[INFO] Launching all sync pipelines concurrently...")
    pipeline_map = {
        "employee_pipeline": _sync_employee_pipeline,
        "project_pipeline": _sync_project_pipeline,
        "project_resources": _sync_project_resources,
        "time_entries": _sync_time_entries,
        "holidays": _sync_holidays,
        "leaves": _sync_leaves,
        "clients": _sync_clients,
    }

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_name = {
            executor.submit(fn): name for name, fn in pipeline_map.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                future.result()
                print(f"[INFO] Pipeline '{name}' completed.")
            except Exception as exc:
                print(f"[ERROR] Pipeline '{name}' raised an unhandled exception: {exc}")
                traceback.print_exc()

    # ── Summary ───────────────────────────────────────────────────────────────
    try:
        invalidate_dbx_cache()
    except Exception:
        pass

    if sync_errors:
        print(f"[WARN] Keka sync completed with errors in: {', '.join(sync_errors)}")
        return False
    else:
        print("[INFO] Keka sync completed successfully for all modules.")
        return True


def sync_keka_data_to_dbx():
    try:
        return _run_sync_keka_data_to_dbx()
    finally:
        flush_dbx_logs()
