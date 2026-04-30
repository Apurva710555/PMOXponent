import os
import time
import random
import traceback
import requests
from typing import Union, Optional
from backend.shared.dbx_utils import sync_to_dbx_table, scd2_sync_employees

# ── Constants ──────────────────────────────────────────────────────────────────
PAGE_SIZE   = 200          # Keka max → fewest API calls (was 100)
MAX_RETRIES = 5            # retry attempts on 429 / 5xx
BACKOFF_BASE = 2.0         # exponential-backoff base (seconds)
MIN_REQUEST_INTERVAL = 1.2 # seconds between requests (~50/min rate limit safe zone)


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
    """Ensures a minimum interval between requests to stay under 50 req/min."""

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._last_call = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        sleep_for = self._interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_call = time.monotonic()


_rate_limiter = _RateLimiter(MIN_REQUEST_INTERVAL)


# ── Fetch single page with retry ───────────────────────────────────────────────
def _fetch_page(url: str, headers: dict, page: int, extra_params: dict = None) -> Optional[Union[dict, list]]:
    """
    Fetch one page with exponential backoff on 429 / 5xx errors.
    Returns parsed JSON body or raises after MAX_RETRIES.
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
                # Honour Retry-After header if present, else use backoff
                retry_after = int(resp.headers.get("Retry-After", 0))
                wait = max(retry_after, BACKOFF_BASE ** attempt + random.uniform(0, 1))
                print(f"[WARN] Rate limited on page {page} (attempt {attempt}/{MAX_RETRIES}). "
                      f"Sleeping {wait:.1f}s …")
                time.sleep(wait)

            elif resp.status_code >= 500:
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
                print(f"[WARN] Server error {resp.status_code} on page {page} "
                      f"(attempt {attempt}/{MAX_RETRIES}). Sleeping {wait:.1f}s …")
                time.sleep(wait)

            else:
                resp.raise_for_status()  # 4xx that isn't 429 → fail immediately

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            print(f"[WARN] Connection/Timeout error on page {page} (attempt {attempt}/{MAX_RETRIES}): "
                  f"{exc}. Sleeping {wait:.1f}s …")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch page {page} from {url} after {MAX_RETRIES} attempts.")


# ── Paginated fetcher ──────────────────────────────────────────────────────────
def _fetch_all_pages(url: str, headers: dict, extra_params: dict = None) -> list:
    """
    Fetch all pages from a paginated Keka API endpoint.

    Keka response shape:
        { "succeeded": true, "data": { "results": [...], "totalPages": N, "totalRecords": M } }

    Improvements over previous version:
      - pageSize=200  (Keka max — was 100)
      - Proper totalPages termination (no hardcoded page > 20 guard)
      - Exponential backoff + jitter on 429 and 5xx (not just 429)
      - Rate limiter instead of fixed time.sleep(0.2)
    """
    all_data   = []
    page       = 1
    total_pages = None  # discovered on first response

    while True:
        print(f"[INFO] Fetching page {page}" +
              (f"/{total_pages}" if total_pages else "") +
              f" from {url}")

        body = _fetch_page(url, headers, page, extra_params)

        # ── Parse response ────────────────────────────────────────────────────
        if isinstance(body, list):
            # Plain list response → no pagination
            all_data.extend(body)
            break

        elif isinstance(body, dict):
            # Unwrap nested data object if present
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
            print(f"[INFO] Page {page}: received {len(items)} records "
                  f"(running total: {len(all_data)})")

            if page >= total_pages:
                break

            page += 1

        else:
            print("[WARN] Unexpected response shape. Stopping pagination.")
            break

    return all_data


# ── Main sync ──────────────────────────────────────────────────────────────────
def sync_keka_data_to_dbx():
    client_id = os.getenv("KEKA_CLIENT_ID")
    if not client_id:
        print("[INFO] Keka credentials not provided. Skipping sync.")
        return

    print("[INFO] Starting Keka data synchronization...")
    base_url = os.getenv("KEKA_BASE_URL", "").rstrip("/")

    # Fetch token once — reused for all three endpoints
    try:
        token = get_keka_access_token()
    except Exception as e:
        print(f"[ERROR] Failed to acquire Keka access token: {e}")
        traceback.print_exc()
        return

    headers = _keka_headers(token)
    sync_errors = []

    # Hoist imports used across multiple sections to avoid Python's UnboundLocalError
    # (importing a name anywhere in a function makes Python treat it as local everywhere)
    from backend.shared.dbx_utils import sync_to_dbx_table, invalidate_dbx_cache

    # ── 1. Employees ───────────────────────────────────────────────────────────
    try:
        emp_url = f"{base_url}/api/v1/hris/employees"
        print(f"[INFO] Fetching employees from: {emp_url}")
        emp_data = _fetch_all_pages(emp_url, headers)
        print(f"[INFO] Fetched {len(emp_data)} employees.")

        emp_table = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees")
        if emp_data:
            scd2_sync_employees(emp_table, emp_data)
        else:
            print("[WARN] No employee data returned from Keka. Skipping employee sync.")

    except Exception as e:
        print(f"[ERROR] Employee sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("employees")

    # ── 2. Projects ────────────────────────────────────────────────────────────
    try:
        proj_url = f"{base_url}/api/v1/psa/projects"
        print(f"[INFO] Fetching projects from: {proj_url}")
        proj_data = _fetch_all_pages(proj_url, headers)
        print(f"[INFO] Fetched {len(proj_data)} projects.")

        proj_table = os.getenv("KEKA_PROJECTS_TABLE", "keka_projects")
        if proj_data:
            from backend.shared.dbx_utils import scd2_sync_projects
            scd2_sync_projects(proj_table, proj_data)
        else:
            print("[WARN] No project data returned from Keka. Skipping project SCD2 sync.")

    except Exception as e:
        print(f"[ERROR] Project sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("projects")

    # ── 3. Project Resources ───────────────────────────────────────────────────
    try:
        res_url = f"{base_url}/api/v1/psa/project/resources"
        print(f"[INFO] Fetching project resources from: {res_url}")
        res_data = _fetch_all_pages(res_url, headers)
        print(f"[INFO] Fetched {len(res_data)} project resources.")

        res_table = os.getenv("KEKA_PROJECT_RESOURCES_TABLE", "keka_project_resources")
        sync_to_dbx_table(res_table, res_data if res_data else [])
        if not res_data:
            print("[WARN] No project resources data returned from Keka. Ensuring table exists.")

    except Exception as e:
        print(f"[ERROR] Project resources sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("project_resources")

    # ── 4. Time Entries (Timesheets) ───────────────────────────────────────────
    try:
        import datetime
        from backend.shared.dbx_utils import execute_query, merge_timeentries
        
        time_table = os.getenv("KEKA_TIMEENTRIES_TABLE", "keka_timeentries")
        catalog = os.getenv("CATALOG_NAME", "")
        schema = os.getenv("SCHEMA_NAME", "")
        
        time_url = f"{base_url}/api/v1/psa/timeentries"
        print(f"[INFO] Fetching time entries from: {time_url}")
        
        start_date = datetime.date(2026, 1, 1)
        try:
            full_table = f"`{catalog}`.`{schema}`.`{time_table}`"
            res = execute_query(f"SELECT MAX(date) as max_date FROM {full_table}", use_cache=False)
            if res and res[0].get("max_date"):
                # max_date string looks like 'YYYY-MM-DD'
                md_str = res[0]["max_date"]
                max_dt = datetime.datetime.strptime(md_str.split("T")[0], "%Y-%m-%d").date()
                # 7-day buffer to capture retro-active timesheet logs or approvals
                start_date = max_dt - datetime.timedelta(days=7)
        except Exception as e:
            print(f"[INFO] Could not fetch MAX(date) from {time_table}, defaulting to {start_date}. (Table might not exist yet)")
            
        end_date = datetime.date.today()
        if start_date > end_date:
            start_date = end_date
            
        all_time_entries = []
        
        # Keka restricts to 60 days max. Use 50 days to be safe.
        current_from = start_date
        while current_from <= end_date:
            current_to = min(current_from + datetime.timedelta(days=50), end_date)
            
            print(f"[INFO] Fetching time entries from {current_from} to {current_to}")
            
            extra_params = {
                "from": current_from.isoformat(),
                "to": current_to.isoformat()
            }
            
            chunk_data = _fetch_all_pages(time_url, headers, extra_params=extra_params)
            
            if chunk_data:
                # Keka might occasionally return deeply nested data depending on timeentries API behavior
                # Ensure we flatten correctly
                all_time_entries.extend(chunk_data)
                
            current_from = current_to + datetime.timedelta(days=1)
            
        print(f"[INFO] Fetched total {len(all_time_entries)} time entries.")
        
        if all_time_entries:
            merge_timeentries(time_table, all_time_entries)
        else:
            print("[WARN] No time entry data returned from Keka. Skipping merge.")

    except Exception as e:
        print(f"[ERROR] Time entries sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("time_entries")

    # ── 5. Skill Matrix ─────────────────────────────────────────────────────────
    try:
        import json as _json
        import time as _time
        import requests as _req
        from concurrent.futures import ThreadPoolExecutor, as_completed

        skills_table = os.getenv("KEKA_EMPLOYEE_SKILLS_TABLE", "keka_employee_skills")
        print("[INFO] Starting skill matrix sync…")

        # Reuse emp_data already fetched in Section 1 — no need to re-fetch 911 employees.
        # Guard in case Section 1 failed and emp_data was never assigned.
        employees_for_skills = emp_data if "emp_data" in dir() and emp_data else []

        if not employees_for_skills:
            print("[WARN] No employee data available for skill matrix sync. Skipping.")
        else:
            all_skill_rows = []
            _SKILLS_DELAY   = 0.2   # seconds between calls per thread (5 threads → ~25 req/min total)
            _MAX_WORKERS    = 5     # parallel threads

            def _fetch_employee_skills(emp):
                """Fetch skills for one employee. Returns list of flat dicts."""
                emp_uuid   = str(emp.get("id", "")).strip()
                emp_number = str(emp.get("employeeNumber", "")).strip()
                if not emp_uuid:
                    return []

                skill_url = f"{base_url}/api/v1/hris/employees/{emp_uuid}/skills"
                _time.sleep(_SKILLS_DELAY)  # lightweight delay — no shared lock needed

                try:
                    resp = _req.get(skill_url, headers=headers, timeout=30)

                    if resp.status_code == 200:
                        body = resp.json()
                        if isinstance(body, dict):
                            skills = body.get("data", [])
                            if isinstance(skills, dict):
                                skills = skills.get("results", [])
                        elif isinstance(body, list):
                            skills = body
                        else:
                            skills = []

                        rows = []
                        for skill in skills:
                            flat = {}
                            for k, v in skill.items():
                                flat[k] = _json.dumps(v) if isinstance(v, (dict, list)) else (str(v) if v is not None else None)
                            flat["employeeId"]     = emp_uuid
                            flat["employeeNumber"] = emp_number
                            rows.append(flat)
                        return rows

                    elif resp.status_code == 404:
                        return []   # employee has no skills — normal
                    else:
                        print(f"[WARN] Skills API {resp.status_code} for employee {emp_number}. Skipping.")
                        return []

                except Exception as exc:
                    print(f"[WARN] Skills fetch failed for employee {emp_number}: {exc}")
                    return []

            total_emps = len(employees_for_skills)
            done       = 0

            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
                futures = {pool.submit(_fetch_employee_skills, emp): emp for emp in employees_for_skills}
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        all_skill_rows.extend(result)
                    done += 1
                    if done % 50 == 0 or done == total_emps:
                        print(f"[INFO] Skills: {done}/{total_emps} employees processed, "
                              f"{len(all_skill_rows)} skill records collected so far.")

            print(f"[INFO] Fetched {len(all_skill_rows)} total skill records across {total_emps} employees.")

            if all_skill_rows:
                sync_to_dbx_table(skills_table, all_skill_rows)
            else:
                print("[WARN] No skill data returned from Keka. Skipping skills sync.")

    except Exception as e:
        print(f"[ERROR] Skill matrix sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("skill_matrix")

    # ── 6. Holidays ────────────────────────────────────────────────────────────
    try:
        import datetime as _dt
        import json as _json

        holidays_table = os.getenv("KEKA_HOLIDAYS_TABLE", "keka_holidays")
        # Correct endpoint: returns all holiday calendars with their holidays embedded
        calendars_url = f"{base_url}/api/v1/time/holidayscalendar"

        print("[INFO] Starting holidays sync…")
        _rate_limiter.wait()
        cal_resp = requests.get(calendars_url, headers=headers, timeout=30)
        cal_resp.raise_for_status()

        cal_body = cal_resp.json()
        # Unwrap: { "succeeded": true, "data": [...] }  or plain list
        if isinstance(cal_body, dict):
            calendars = cal_body.get("data", [])
            if isinstance(calendars, dict):
                calendars = calendars.get("results", [])
        elif isinstance(cal_body, list):
            calendars = cal_body
        else:
            calendars = []

        all_holiday_rows = []

        for cal in calendars:
            cal_id   = str(cal.get("id", ""))
            cal_name = str(cal.get("name", ""))

            # Holidays are either nested in the calendar object or need a sub-call
            raw_holidays = cal.get("holidays") or cal.get("holidayList") or []

            if not raw_holidays and cal_id:
                # Try the sub-resource pattern if holidays weren't embedded
                _rate_limiter.wait()
                sub_url  = f"{base_url}/api/v1/time/holidayscalendar/{cal_id}/holidays"
                sub_resp = requests.get(sub_url, headers=headers, timeout=30)
                if sub_resp.status_code == 200:
                    sub_body     = sub_resp.json()
                    raw_holidays = (
                        sub_body.get("data", []) if isinstance(sub_body, dict) else
                        (sub_body if isinstance(sub_body, list) else [])
                    )

            for h in raw_holidays:
                if isinstance(h, dict):
                    flat = {}
                    for k, v in h.items():
                        flat[k] = _json.dumps(v) if isinstance(v, (dict, list)) else (str(v) if v is not None else None)
                    flat["calendarId"]   = cal_id
                    flat["calendarName"] = cal_name
                    all_holiday_rows.append(flat)

            print(f"[INFO] Calendar '{cal_name}': {len(raw_holidays)} holidays.")

        print(f"[INFO] Fetched {len(all_holiday_rows)} total holiday records across {len(calendars)} calendars.")

        if all_holiday_rows:
            sync_to_dbx_table(holidays_table, all_holiday_rows)
        else:
            print("[WARN] No holiday data returned from Keka. Skipping holidays sync.")

    except Exception as e:
        print(f"[ERROR] Holidays sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("holidays")

    # ── 7. Leave Requests ──────────────────────────────────────────────────────
    try:
        import datetime as _dt

        leaves_table = os.getenv("KEKA_LEAVES_TABLE", "keka_leave_requests")
        leaves_url   = f"{base_url}/api/v1/time/leaverequests"

        print("[INFO] Starting leave requests sync…")

        # Keka enforces a hard limit of 90 days per request — chunk accordingly.
        # Fetch: 12 months prior → 6 months ahead (covers all realistic leave data).
        CHUNK_DAYS = 89
        today      = _dt.date.today()
        window_start = today.replace(year=today.year - 1, month=1, day=1)
        window_end   = today + _dt.timedelta(days=180)   # 6 months ahead

        all_leave_rows = []
        seen_ids       = set()   # de-duplicate across chunks
        chunk_start    = window_start

        while chunk_start <= window_end:
            chunk_end = min(chunk_start + _dt.timedelta(days=CHUNK_DAYS), window_end)
            from_str  = chunk_start.strftime("%Y-%m-%d")
            to_str    = chunk_end.strftime("%Y-%m-%d")

            print(f"[INFO] Fetching leave requests: {from_str} → {to_str}")

            chunk_data = _fetch_all_pages(
                leaves_url,
                headers,
                extra_params={"from": from_str, "to": to_str}
            )

            new_in_chunk = 0
            for row in chunk_data:
                # De-duplicate using id/leaveRequestId (Keka field name varies)
                row_id = row.get("id") or row.get("leaveRequestId") or id(row)
                if row_id not in seen_ids:
                    seen_ids.add(row_id)
                    all_leave_rows.append(row)
                    new_in_chunk += 1

            print(f"[INFO] Chunk {from_str}→{to_str}: {len(chunk_data)} records, {new_in_chunk} new.")
            chunk_start = chunk_end + _dt.timedelta(days=1)

        print(f"[INFO] Fetched {len(all_leave_rows)} unique leave records across all chunks.")

        if all_leave_rows:
            sync_to_dbx_table(leaves_table, all_leave_rows)
        else:
            print("[WARN] No leave request data returned from Keka. Skipping leaves sync.")

    except Exception as e:
        print(f"[ERROR] Leave requests sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("leave_requests")

    # ── 8. Clients ─────────────────────────────────────────────────────────────
    try:
        clients_table = os.getenv("KEKA_CLIENTS_TABLE", "keka_clients")
        clients_url   = f"{base_url}/api/v1/psa/clients"

        print(f"[INFO] Fetching clients from: {clients_url}")
        client_data = _fetch_all_pages(clients_url, headers)
        print(f"[INFO] Fetched {len(client_data)} clients.")

        if client_data:
            sync_to_dbx_table(clients_table, client_data)
        else:
            print("[WARN] No client data returned from Keka. Skipping clients sync.")

    except Exception as e:
        print(f"[ERROR] Clients sync failed: {e}")
        traceback.print_exc()
        sync_errors.append("clients")

    # ── Summary ────────────────────────────────────────────────────────────────

    # Always invalidate cache — even on partial errors, successfully synced
    # modules must be visible immediately (e.g. skills synced but proj_resources failed).
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