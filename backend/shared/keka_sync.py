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
def _fetch_page(url: str, headers: dict, page: int) -> Optional[Union[dict, list]]:
    """
    Fetch one page with exponential backoff on 429 / 5xx errors.
    Returns parsed JSON body or raises after MAX_RETRIES.
    """
    params = {"pageNumber": page, "pageSize": PAGE_SIZE}

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

        except requests.exceptions.ConnectionError as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            print(f"[WARN] Connection error on page {page} (attempt {attempt}/{MAX_RETRIES}): "
                  f"{exc}. Sleeping {wait:.1f}s …")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch page {page} from {url} after {MAX_RETRIES} attempts.")


# ── Paginated fetcher ──────────────────────────────────────────────────────────
def _fetch_all_pages(url: str, headers: dict) -> list:
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

        body = _fetch_page(url, headers, page)

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

    # ── Summary ────────────────────────────────────────────────────────────────
    if sync_errors:
        print(f"[WARN] Keka sync completed with errors in: {', '.join(sync_errors)}")
        return False
    else:
        print("[INFO] Keka sync completed successfully for all modules.")
        
        # Invalidate Databricks API cache so UI picks up the latest synced data instantly
        from backend.shared.dbx_utils import invalidate_dbx_cache
        invalidate_dbx_cache()
        
        return True