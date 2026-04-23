from flask import Blueprint, jsonify, request
import os
import requests
from backend.shared.dbx_utils import fetch_table_data, scd2_update_status, execute_query
from backend.shared.keka_sync import get_keka_access_token, _keka_headers

employee_bp = Blueprint('employee_bp', __name__)


# ── Route 1: Employee profile data from Databricks ──────────────

@employee_bp.route('/api/employee/data', methods=['GET'])
def get_employee_data():
    """Fetches Employee table data from Databricks — active records only (enddate IS NULL)."""
    try:
        from backend.shared.dbx_utils import execute_query
        catalog    = os.getenv("CATALOG_NAME")
        schema     = os.getenv("SCHEMA_NAME")
        table_name = os.getenv("KEKA_EMPLOYEES_TABLE")
        if not table_name:
            return jsonify({"status": "success", "data": []}), 200

        sql = (
            f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}` "
            f"WHERE (enddate IS NULL OR enddate = '' OR enddate = 'None') "
            f"AND (accountStatus = 1 OR accountStatus = '1')"
        )
        from backend.shared.dbx_utils import execute_query
        active = execute_query(sql)
        return jsonify({"status": "success", "data": active})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 2: Live timesheet (PSA time entries) from Keka API ────────

# Status code mapping for PSA time entries
_TIME_ENTRY_STATUS = {
    0: "Draft",
    1: "Submitted",
    2: "Approved",
    3: "Rejected",
}

@employee_bp.route('/api/employee/timesheet', methods=['GET'])
def get_employee_timesheet():
    """
    Proxies the Keka PSA Time Entries API and enriches records with
    project and task names.
    Endpoint: GET /api/v1/psa/timeentries

    Query params:
        employeeId  — employee UUID (employeeIdentifier from Keka)
        from        — start date (YYYY-MM-DD)
        to          — end date   (YYYY-MM-DD)
    """
    employee_id = request.args.get('employeeId', '').strip()
    from_date   = request.args.get('from', '').strip()
    to_date     = request.args.get('to', '').strip()

    if not employee_id or not from_date or not to_date:
        return jsonify({
            "status": "error",
            "message": "Missing required params: employeeId, from, to"
        }), 400

    base_url = os.getenv("KEKA_BASE_URL", "").rstrip("/")
    url = f"{base_url}/api/v1/psa/timeentries"

    try:
        token   = get_keka_access_token()
        headers = _keka_headers(token)

        # ── Step 1: Fetch all time entries (paginated) ───────────
        all_records = []
        page = 1
        while True:
            params = {
                "employeeIds": employee_id,
                "from":        from_date,
                "to":          to_date,
                "pageNumber":  page,
                "pageSize":    100,
            }
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            chunk = body.get("data", []) if isinstance(body, dict) else (body if isinstance(body, list) else [])
            if not chunk:
                break

            # Basic enrichment: minutes → formatted, status → label
            for rec in chunk:
                minutes = rec.get("totalMinutes")
                if minutes is not None:
                    try:
                        m = int(minutes)
                        rec["hoursFormatted"] = f"{m // 60}h {m % 60:02d}m"
                    except (ValueError, TypeError):
                        rec["hoursFormatted"] = str(minutes)
                else:
                    rec["hoursFormatted"] = ""

                status_raw = rec.get("status")
                if status_raw is not None:
                    try:
                        rec["statusLabel"] = _TIME_ENTRY_STATUS.get(int(status_raw), str(status_raw))
                    except (ValueError, TypeError):
                        rec["statusLabel"] = str(status_raw)
                else:
                    rec["statusLabel"] = ""

            all_records.extend(chunk)

            total_pages = body.get("totalPages", 1) if isinstance(body, dict) else 1
            if page >= total_pages:
                break
            page += 1

        # ── Step 2: Build project name map from Databricks ───────
        # Mst_Project_info has: id (UUID) → name (display name)
        proj_map = {}
        try:
            proj_table = os.getenv("KEKA_PROJECTS_TABLE", "keka_projects")
            projects = fetch_table_data(proj_table)
            for p in projects:
                pid = str(p.get("id", "")).strip()
                pname = str(p.get("name", "")).strip()
                if pid and pname:
                    proj_map[pid] = pname
        except Exception as proj_err:
            print(f"[WARN] Could not load project names: {proj_err}")

        # ── Step 3: Build task name map via Keka API ─────────────
        # Collect unique project IDs that appear in this batch
        unique_project_ids = {
            str(r.get("projectId", "")).strip()
            for r in all_records
            if r.get("projectId")
        }

        task_map = {}  # taskId → taskName
        for proj_id in unique_project_ids:
            if not proj_id:
                continue
            try:
                tasks_url = f"{base_url}/api/v1/psa/projects/{proj_id}/tasks"
                t_resp = requests.get(tasks_url, headers=headers, timeout=20)
                if t_resp.status_code == 200:
                    t_body = t_resp.json()
                    task_list = t_body.get("data", []) if isinstance(t_body, dict) else (t_body if isinstance(t_body, list) else [])
                    for task in task_list:
                        tid   = str(task.get("id", "")).strip()
                        tname = str(task.get("name", "")).strip()
                        if tid and tname:
                            task_map[tid] = tname
            except Exception as task_err:
                print(f"[WARN] Could not load tasks for project {proj_id}: {task_err}")

        # ── Step 4: Enrich records with resolved names ────────────
        for rec in all_records:
            proj_id = str(rec.get("projectId", "")).strip()
            task_id = str(rec.get("taskId", "")).strip()
            rec["projectName"] = proj_map.get(proj_id, proj_id)   # fallback to ID if not found
            rec["taskName"]    = task_map.get(task_id, task_id)   # fallback to ID if not found

        return jsonify({"status": "success", "data": all_records})

    except requests.HTTPError as e:
        return jsonify({
            "status": "error",
            "message": f"Keka API error: {e.response.status_code} — {e.response.text[:200]}"
        }), 502
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



# ── Route 3: SCD2 change-log history for an employee ────────────────────────

# Fields that are diffed between consecutive SCD2 versions
_HISTORY_DIFF_FIELDS = [
    ("jobTitle",        "Job Title"),
    ("department",      "Department"),
    ("businessUnit",    "Business Unit"),
    ("reportsTo",       "L1 Manager"),
    ("location",        "Location"),
    ("employeeStatus",  "Employee Status"),
    ("firstName",       "First Name"),
    ("lastName",        "Last Name"),
    ("displayName",     "Display Name"),
    ("email",           "Email"),
    ("designation",     "Designation"),
    ("status",          "PMO Status"),
    ("comments",        "PMO Comments"),
]

def _extract_title(value):
    """Unwrap {'title': '...'} / {'name': '...'} objects, or return the raw value."""
    if isinstance(value, dict):
        return value.get("title") or value.get("name") or value.get("firstName") or ""
    return str(value) if value not in (None, "null", "NULL", "None") else ""


@employee_bp.route('/api/employee/history', methods=['GET'])
def get_employee_history():
    """
    Returns an ordered change-log derived from SCD2 rows for a given employee.

    Each entry describes:
      - when the version started / ended
      - which fields changed vs the previous version (field-level diff)
      - the PMO status & comments at that point in time

    Query params:
        employeeId — the employeeNumber (human-readable ID, e.g. 100052)
    """
    employee_id = request.args.get('employeeId', '').strip()
    if not employee_id:
        return jsonify({"status": "error", "message": "Missing required param: employeeId"}), 400

    table_name = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees")
    try:
        all_rows = fetch_table_data(table_name)

        # Filter to this employee (all SCD2 versions)
        versions = [
            row for row in all_rows
            if str(row.get("employeeNumber", "")).strip() == employee_id
            or str(row.get("employee_code", "")).strip() == employee_id
        ]

        if not versions:
            return jsonify({"status": "success", "data": []})

        # Sort ascending by startdate so we can diff consecutive versions
        def _sort_key(r):
            sd = r.get("startdate") or ""
            return str(sd) if sd not in ("null", "NULL", "None", None) else ""

        versions.sort(key=_sort_key)

        # Build change-log entries (newest first for the UI)
        changelog = []
        for i, ver in enumerate(versions):
            prev = versions[i - 1] if i > 0 else None

            # Compute field-level diffs vs previous version
            diffs = []
            if prev:
                for field_key, field_label in _HISTORY_DIFF_FIELDS:
                    before = _extract_title(prev.get(field_key, ""))
                    after  = _extract_title(ver.get(field_key, ""))
                    # Normalise None / empty-like values
                    before = before if before not in ("null", "NULL", "None") else ""
                    after  = after  if after  not in ("null", "NULL", "None") else ""
                    if before != after:
                        diffs.append({
                            "field":  field_label,
                            "before": before,
                            "after":  after,
                        })

            entry = {
                "version":    i + 1,
                "startdate":  ver.get("startdate"),
                "enddate":    ver.get("enddate"),
                "modifiedby": ver.get("modifiedby") or "",
                "status":     _extract_title(ver.get("status", "")) or "",
                "comments":   ver.get("comments") or "",
                "diffs":      diffs,
                "is_current": ver.get("enddate") in (None, "", "null", "NULL", "None"),
            }
            changelog.append(entry)

        # Return newest-first
        changelog.reverse()
        return jsonify({"status": "success", "data": changelog})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 4: UI Status update (SCD Type 2) ───────────────────────────────────

@employee_bp.route('/api/employee/status', methods=['POST'])
def update_employee_status():
    """
    Records a manual status/comments change via SCD Type 2.

    Body (JSON):
        employeeNumber  — the human-readable ID (e.g. "0001")
        status          — one of: On Bench | On Hold | Blocked | On Project
        comments        — mandatory comment string
    """
    body            = request.get_json(force=True, silent=True) or {}
    employee_number = str(body.get('employeeNumber', '')).strip()
    new_status      = str(body.get('status',         '')).strip()
    new_comments    = str(body.get('comments',       '')).strip()

    if not employee_number:
        return jsonify({"status": "error", "message": "Missing: employeeNumber"}), 400
    if not new_status:
        return jsonify({"status": "error", "message": "Missing: status"}), 400
    if not new_comments:
        return jsonify({"status": "error", "message": "Comments are required"}), 400

    table_name = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees")
    try:
        scd2_update_status(table_name, employee_number, new_status, new_comments)
        
        from backend.shared.dbx_utils import invalidate_dbx_cache
        invalidate_dbx_cache()

        return jsonify({
            "status":  "success",
            "message": f"Status updated to '{new_status}' for employee {employee_number}."
        })
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 5: Project history for an employee ─────────────────────────────────

@employee_bp.route('/api/employee/project-history', methods=['GET'])
def get_employee_project_history():
    """
    Returns project assignment history for a given employee by joining:
      - KEKA_EMPLOYEE_PROJECT_RESOURCES  (employeeId, projectId, startdate, enddate, comment)
      - KEKA_EMPLOYEES_TABLE             (id → displayName, employeeNumber)
      - KEKA_PROJECTS_TABLE              (id → name, code)

    Status logic: Active when enddate is NULL/empty, Inactive otherwise.

    Query params:
        employeeId — the employeeNumber (human-readable ID, e.g. 100052)
    """
    employee_id = request.args.get('employeeId', '').strip()
    if not employee_id:
        return jsonify({"status": "error", "message": "Missing required param: employeeId"}), 400

    resources_table = os.getenv("KEKA_EMPLOYEE_PROJECT_RESOURCES", "").strip()
    projects_table  = os.getenv("KEKA_PROJECTS_TABLE", "").strip()
    employee_table  = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees").strip()
    time_table      = os.getenv("KEKA_TIMEENTRIES_TABLE", "keka_timeentries").strip()
    catalog         = os.getenv("CATALOG_NAME", "").strip()
    schema          = os.getenv("SCHEMA_NAME", "").strip()

    if not all([resources_table, projects_table, employee_table, catalog, schema]):
        return jsonify({
            "status": "error",
            "message": "Missing required server configuration: KEKA_EMPLOYEE_PROJECT_RESOURCES, KEKA_PROJECTS_TABLE, CATALOG_NAME or SCHEMA_NAME not set."
        }), 500

    safe_emp_id = employee_id.replace("'", "''")

    try:
        # sql = f"""
        #     SELECT
        #         DISTINCT
        #         e.displayName AS employee_name,
        #         e.employeeNumber,
        #         r.employeeId,

        #         p.name AS projectName,
        #         p.code AS projectCode,

        #         r.projectId,
        #         r.startdate,
        #         r.enddate,

        #         r.comment AS comment,

        #         DATEDIFF(
        #             TO_DATE(r.enddate),
        #             TO_DATE(r.startdate)
        #         ) AS days_worked

        #     FROM `{catalog}`.`{schema}`.`{resources_table}` r

        #     JOIN `{catalog}`.`{schema}`.`{employee_table}` e
        #         ON r.employeeId = e.id

        #     JOIN `{catalog}`.`{schema}`.`{projects_table}` p
        #         ON r.projectId = p.id

        #     WHERE e.employeeNumber = '{safe_emp_id}'

        #     ORDER BY
        #         CASE
        #             WHEN (r.enddate IS NULL OR r.enddate = '' OR r.enddate = 'None')
        #             THEN 0 ELSE 1
        #         END ASC,
        #         r.startdate DESC
        # """
        sql = f"""
            WITH emp AS (
                SELECT id, displayName, employeeNumber
                FROM `{catalog}`.`{schema}`.`{employee_table}`
                WHERE employeeNumber = '{safe_emp_id}'
            ),
            time_summary AS (
                SELECT 
                    employeeId, 
                    projectId, 
                    COUNT(DISTINCT date) as actual_days_worked,
                    MIN(date) as first_timesheet_date,
                    MAX(date) as last_timesheet_date
                FROM `{catalog}`.`{schema}`.`{time_table}`
                GROUP BY employeeId, projectId
            )

            SELECT
                e.displayName AS employee_name,
                e.employeeNumber,
                r.employeeId,

                p.name AS projectName,
                p.code AS projectCode,

                r.projectId,
                CASE 
                    WHEN MIN(r.startdate) IS NULL OR MIN(r.startdate) = '' OR MIN(r.startdate) = 'None' THEN MAX(t.first_timesheet_date)
                    ELSE MIN(r.startdate)
                END AS startdate,
                
                CASE 
                    WHEN MAX(r.enddate) IS NULL OR MAX(r.enddate) = '' OR MAX(r.enddate) = 'None' THEN MAX(t.last_timesheet_date)
                    ELSE MAX(r.enddate)
                END AS enddate,
                MAX(r.comment) AS comment,
                
                CASE 
                    WHEN MAX(r.enddate) IS NOT NULL 
                         AND MAX(r.enddate) != '' 
                         AND MAX(r.enddate) != 'None' 
                         AND CAST(SUBSTRING(MAX(r.enddate), 1, 10) AS DATE) < CURRENT_DATE() 
                    THEN 'Inactive'
                    ELSE 'Active'
                END AS project_status,
                
                COALESCE(MAX(t.actual_days_worked), 0) AS days_worked

            FROM emp e

            JOIN `{catalog}`.`{schema}`.`{resources_table}` r
                ON r.employeeId = e.id

            JOIN `{catalog}`.`{schema}`.`{projects_table}` p
                ON r.projectId = p.id
                
            LEFT JOIN time_summary t
                ON LOWER(t.projectId) = LOWER(r.projectId) AND LOWER(t.employeeId) = LOWER(r.employeeId)

            GROUP BY
                e.displayName,
                e.employeeNumber,
                r.employeeId,
                p.name,
                p.code,
                r.projectId

            ORDER BY startdate DESC
            """

        rows = execute_query(sql) or []

        # Databricks SQL calculates exact project_status and dynamic enddate now
        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
