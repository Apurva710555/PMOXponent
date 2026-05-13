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

    try:
        catalog = os.getenv("CATALOG_NAME")
        schema  = os.getenv("SCHEMA_NAME")
        time_table = os.getenv("KEKA_TIMEENTRIES_TABLE", "keka_timeentries")
        proj_table = os.getenv("KEKA_PROJECTS_TABLE", "keka_projects")
        
        safe_emp_id = employee_id.replace("'", "''")
        safe_from   = from_date.replace("'", "''")
        safe_to     = to_date.replace("'", "''")
        
        # Query time entries from Databricks and join with projects for the name
        sql = f"""
            SELECT 
                t.*,
                COALESCE(p.name, t.projectId) AS projectName
            FROM `{catalog}`.`{schema}`.`{time_table}` t
            LEFT JOIN `{catalog}`.`{schema}`.`{proj_table}` p
                ON LOWER(t.projectId) = LOWER(p.id)
                AND (p.enddate IS NULL OR p.enddate = '' OR p.enddate = 'None')
            WHERE LOWER(t.employeeId) = LOWER('{safe_emp_id}')
              AND t.date >= '{safe_from}'
              AND t.date <= '{safe_to}'
            ORDER BY t.date DESC
        """
        
        rows = execute_query(sql) or []
        
        # Enrich formatting (minutes -> hoursFormatted, status -> statusLabel)
        for rec in rows:
            minutes = rec.get("totalMinutes")
            if minutes is not None:
                try:
                    m = int(float(minutes))
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
                
            # Fallback for task name since we don't sync tasks to Databricks yet
            if not rec.get("taskName"):
                rec["taskName"] = rec.get("taskId") or "Task"

        return jsonify({"status": "success", "data": rows})

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
        # Use targeted SQL instead of fetching the entire table (fix L3-8)
        catalog    = os.getenv("CATALOG_NAME")
        schema     = os.getenv("SCHEMA_NAME")
        safe_emp_id = employee_id.replace("'", "''")

        sql = (
            f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}` "
            f"WHERE employeeNumber = '{safe_emp_id}' "
            f"ORDER BY startdate ASC"
        )
        versions = execute_query(sql) or []

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
      - KEKA_PROJECT_RESOURCES_TABLE  (employeeId, projectId, startdate, enddate, comment)
      - KEKA_EMPLOYEES_TABLE             (id → displayName, employeeNumber)
      - KEKA_PROJECTS_TABLE              (id → name, code)
      - KEKA_PROJECT_ALLOCATIONS_TABLE   (per-employee startDate, endDate)

    Start/End date priority: allocation dates → timesheet dates → project dates.

    Query params:
        employeeId — the employeeNumber (human-readable ID, e.g. 100052)
    """
    employee_id = request.args.get('employeeId', '').strip()
    if not employee_id:
        return jsonify({"status": "error", "message": "Missing required param: employeeId"}), 400

    resources_table = os.getenv("KEKA_PROJECT_RESOURCES_TABLE", "").strip()
    projects_table  = os.getenv("KEKA_PROJECTS_TABLE", "").strip()
    employee_table  = os.getenv("KEKA_EMPLOYEES_TABLE", "keka_employees").strip()
    time_table      = os.getenv("KEKA_TIMEENTRIES_TABLE", "keka_timeentries").strip()
    alloc_table     = os.getenv("KEKA_PROJECT_ALLOCATIONS_TABLE", "keka_project_allocations").strip()
    catalog         = os.getenv("CATALOG_NAME", "").strip()
    schema          = os.getenv("SCHEMA_NAME", "").strip()

    if not all([resources_table, projects_table, employee_table, catalog, schema]):
        return jsonify({
            "status": "error",
            "message": "Missing required server configuration: KEKA_PROJECT_RESOURCES_TABLE, KEKA_PROJECTS_TABLE, CATALOG_NAME or SCHEMA_NAME not set."
        }), 500

    safe_emp_id = employee_id.replace("'", "''")

    # Fully-qualified table references
    A = f"`{catalog}`.`{schema}`.`{alloc_table}`"

    try:
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
                    SUM(CAST(totalMinutes AS DOUBLE)) as total_minutes_worked,
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

                -- Per-employee allocation dates only (no fallback)
                MAX(a.startDate) AS startdate,
                MAX(a.endDate)   AS enddate,

                r.name AS comment,
                
                CASE 
                    WHEN MAX(a.endDate) IS NOT NULL 
                         AND MAX(a.endDate) != '' 
                         AND MAX(a.endDate) != 'None'
                         AND CAST(SUBSTRING(MAX(a.endDate), 1, 10) AS DATE) < CURRENT_DATE() 
                    THEN 'Inactive'
                    ELSE 'Active'
                END AS project_status,
                
                COALESCE(MAX(t.actual_days_worked), 0) AS days_worked,
                ROUND(COALESCE(MAX(t.total_minutes_worked), 0) / 60.0, 1) AS hours_worked,
                MAX(a.allocationPercentage) AS allocationPercentage

            FROM emp e

            JOIN `{catalog}`.`{schema}`.`{resources_table}` r
                ON r.employeeId = e.id

            JOIN `{catalog}`.`{schema}`.`{projects_table}` p
                ON r.projectId = p.id
                AND (p.enddate IS NULL OR p.enddate = '' OR p.enddate = 'None')
                
            LEFT JOIN time_summary t
                ON LOWER(t.projectId) = LOWER(r.projectId) AND LOWER(t.employeeId) = LOWER(r.employeeId)

            LEFT JOIN {A} a
                ON LOWER(a.projectId) = LOWER(r.projectId) AND LOWER(a.employeeId) = LOWER(r.employeeId)

            GROUP BY
                e.displayName,
                e.employeeNumber,
                r.employeeId,
                p.name,
                p.code,
                r.projectId,
                r.name

            ORDER BY startdate DESC
            """

        rows = execute_query(sql) or []

        # Databricks SQL calculates exact project_status and dynamic enddate now
        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 6: Skill Matrix for an employee ─────────────────────────────────────

@employee_bp.route('/api/employee/skills', methods=['GET'])
def get_employee_skills():
    """
    Returns skill matrix records for a given employee from the synced
    keka_employee_skills Databricks table.

    Query params:
        employeeId — the employeeNumber (human-readable ID, e.g. 100052)
    """
    employee_number = request.args.get('employeeId', '').strip()
    if not employee_number:
        return jsonify({"status": "error", "message": "Missing required param: employeeId"}), 400

    skills_table   = os.getenv("KEKA_EMPLOYEE_SKILLS_TABLE", "keka_employee_skills").strip()
    catalog        = os.getenv("CATALOG_NAME", "").strip()
    schema         = os.getenv("SCHEMA_NAME", "").strip()

    if not all([skills_table, catalog, schema]):
        return jsonify({
            "status": "error",
            "message": "Server configuration missing: KEKA_EMPLOYEE_SKILLS_TABLE, CATALOG_NAME or SCHEMA_NAME not set."
        }), 500

    safe_emp_number = employee_number.replace("'", "''")

    try:
        sql = f"""
            SELECT *
            FROM `{catalog}`.`{schema}`.`{skills_table}`
            WHERE employeeNumber = '{safe_emp_number}'
        """
        rows = execute_query(sql) or []
        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        # Table might not exist yet (before first sync) — return empty gracefully
        error_str = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str.lower():
            return jsonify({"status": "success", "data": []})
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 7: Get certifications for an employee ───────────────────────────────

@employee_bp.route('/api/employee/certifications', methods=['GET'])
def get_employee_certifications():
    """
    Returns all certifications for a given employee.

    Query params:
        employeeId — the employeeNumber (human-readable ID, e.g. 100052)
    """
    employee_number = request.args.get('employeeId', '').strip()
    if not employee_number:
        return jsonify({"status": "error", "message": "Missing required param: employeeId"}), 400

    cert_table = os.getenv("EMPLOYEE_CERTIFICATIONS_TABLE", "employee_certification").strip()
    catalog    = os.getenv("CATALOG_NAME", "").strip()
    schema     = os.getenv("SCHEMA_NAME",  "").strip()

    if not all([cert_table, catalog, schema]):
        return jsonify({
            "status": "error",
            "message": "Server configuration missing: EMPLOYEE_CERTIFICATIONS_TABLE, CATALOG_NAME or SCHEMA_NAME not set."
        }), 500

    safe_emp_number = employee_number.replace("'", "''")

    try:
        from backend.shared.dbx_utils import get_dbx_connection

        def _table_columns(table_name):
            with get_dbx_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table_name}`")
                    return [row[0] for row in cursor.fetchall()]

        cols = _table_columns(cert_table)
        if "E_Code" in cols:
            id_column = "E_Code"
        elif "employeeNumber" in cols:
            id_column = "employeeNumber"
        else:
            return jsonify({
                "status": "error",
                "message": "Certification table missing employee identifier column (expected E_Code or employeeNumber)."
            }), 500

        # Order using available date field if present
        order_column = None
        for candidate in ["issueDate", "Completion_Date", "Date_of_Expiry"]:
            if candidate in cols:
                order_column = candidate
                break

        sql = f"SELECT * FROM `{catalog}`.`{schema}`.`{cert_table}` WHERE {id_column} = '{safe_emp_number}'"
        if order_column:
            sql += f" ORDER BY {order_column} DESC"

        rows = execute_query(sql) or []
        return jsonify({"status": "success", "data": rows})

    except Exception as e:
        error_str = str(e)
        # Table doesn't exist yet — return empty list gracefully
        if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str.lower():
            return jsonify({"status": "success", "data": []})
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 8: Add a new certification for an employee ─────────────────────────

@employee_bp.route('/api/employee/certifications', methods=['POST'])
def add_employee_certification():
    """
    Inserts a new certification record for an employee.
    Auto-creates the certifications table in Databricks if it does not exist.

    Body (JSON):
        employeeNumber  — human-readable employee ID
        certificateName — name of the certificate (required)
        issuer          — issuing organisation (required)
        certType        — Self | Free | N/A
        issueDate       — YYYY-MM-DD (required)
        expiryDate      — YYYY-MM-DD (optional)
        credentialUrl   — URL (optional)
        description     — text (optional)
    """
    import datetime
    import uuid

    body = request.get_json(force=True, silent=True) or {}

    employee_number  = str(body.get('employeeNumber',  '')).strip()
    certificate_name = str(body.get('certificateName', '')).strip()
    issuer           = str(body.get('issuer',          '')).strip()
    cert_type        = str(body.get('certType',        'Self')).strip()
    technology       = str(body.get('technology',      '')).strip()
    amount           = str(body.get('amountInRs',      '') or body.get('Amount_in_Rs', '')).strip()
    issue_date       = str(body.get('issueDate',       '')).strip()
    expiry_date      = str(body.get('expiryDate',      '')).strip()
    credential_url   = str(body.get('credentialUrl',   '')).strip()
    description      = str(body.get('description',     '')).strip()

    # Validation
    if not employee_number:
        return jsonify({"status": "error", "message": "Missing: employeeNumber"}), 400
    if not certificate_name:
        return jsonify({"status": "error", "message": "Missing: certificateName"}), 400
    if not issuer:
        return jsonify({"status": "error", "message": "Missing: issuer"}), 400
    if not issue_date:
        return jsonify({"status": "error", "message": "Missing: issueDate"}), 400

    cert_table = os.getenv("EMPLOYEE_CERTIFICATIONS_TABLE", "employee_certification").strip()
    catalog    = os.getenv("CATALOG_NAME", "").strip()
    schema     = os.getenv("SCHEMA_NAME",  "").strip()

    if not all([cert_table, catalog, schema]):
        return jsonify({
            "status": "error",
            "message": "Server configuration missing: CATALOG_NAME or SCHEMA_NAME not set."
        }), 500

    full_table = f"`{catalog}`.`{schema}`.`{cert_table}`"
    now_ts     = datetime.datetime.utcnow().isoformat()
    record_id  = str(uuid.uuid4())
    employee_name = str(body.get('employeeName', '')).strip()

    def _esc(v):
        if v is None or v == '':
            return "NULL"
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            id             STRING,
            employeeNumber STRING,
            E_Code         STRING,
            E_Name         STRING,
            certificateName STRING,
            Certification_name STRING,
            issuer         STRING,
            certType       STRING,
            Type           STRING,
            Technology     STRING,
            issueDate      STRING,
            Completion_Date STRING,
            expiryDate     STRING,
            Date_of_Expiry STRING,
            Amount_in_Rs   STRING,
            credentialUrl  STRING,
            credential_url STRING,
            description    STRING,
            Note           STRING,
            addedAt        STRING,
            addedBy        STRING
        )
    """

    def _describe_columns(cursor):
        cursor.execute(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{cert_table}`")
        return [row[0] for row in cursor.fetchall()]

    try:
        from backend.shared.dbx_utils import get_dbx_connection, invalidate_dbx_cache
        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:
                print(f"[INFO] Ensuring certifications table exists: {full_table}")
                cursor.execute(create_sql)

                try:
                    cols = _describe_columns(cursor)
                except Exception:
                    cols = []

                if not cols:
                    cols = _describe_columns(cursor)

                insert_columns = []
                insert_values = []

                def add_col(col_name, value):
                    if col_name in cols:
                        insert_columns.append(col_name)
                        insert_values.append(_esc(value) if value not in (None, "") else 'NULL')

                add_col('id', record_id)
                add_col('employeeNumber', employee_number)
                add_col('E_Code', employee_number)
                add_col('E_Name', employee_name)
                add_col('certificateName', certificate_name)
                add_col('Certification_name', certificate_name)
                add_col('issuer', issuer)
                add_col('certType', cert_type)
                add_col('Type', cert_type)
                add_col('Technology', technology)
                add_col('issueDate', issue_date)
                add_col('Completion_Date', issue_date)
                add_col('expiryDate', expiry_date)
                add_col('Date_of_Expiry', expiry_date)
                add_col('Amount_in_Rs', amount)
                add_col('credentialUrl', credential_url)
                add_col('credential_url', credential_url)
                add_col('description', description)
                add_col('Note', description)
                add_col('addedAt', now_ts)
                add_col('addedBy', 'PMO')

                if not insert_columns:
                    raise Exception("No compatible certification columns found for insert.")

                insert_sql = f"""
                    INSERT INTO {full_table}
                        ({', '.join(insert_columns)})
                    VALUES (
                        {', '.join(insert_values)}
                    )
                """

                print(f"[INFO] Inserting certification for employee {employee_number}: {certificate_name}")
                cursor.execute(insert_sql)
            if hasattr(conn, 'commit'):
                conn.commit()

        # Bust cache so next GET reflects the new row
        invalidate_dbx_cache()

        return jsonify({
            "status":  "success",
            "message": f"Certificate '{certificate_name}' added for employee {employee_number}.",
            "id":      record_id
        })

    except Exception as e:
        print(f"[ERROR] add_employee_certification failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

