from flask import Blueprint, jsonify, request
import os
from backend.shared.dbx_utils import fetch_table_data, execute_query

project_bp = Blueprint('project_bp', __name__)


# ── Route 1: Project master data from Databricks ─────────────────────────────

@project_bp.route('/api/project/data', methods=['GET'])
def get_project_data():
    """Fetches active Project table data from Databricks."""
    try:
        catalog = os.getenv("CATALOG_NAME")
        schema  = os.getenv("SCHEMA_NAME")
        table_name = os.getenv("KEKA_PROJECTS_TABLE")
        if not table_name:
            return jsonify({"status": "success", "data": []}), 200
        
        sql = f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}` WHERE enddate IS NULL OR enddate = '' OR enddate = 'None'"
        data = execute_query(sql)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@project_bp.route('/api/project/account-manager', methods=['POST'])
def update_project_manager():
    """Endpoint to update a project's account manager and add a comment via SCD Type 2."""
    try:
        from backend.shared.dbx_utils import scd2_update_project_manager
        body = request.get_json()
        if not body:
            return jsonify({"status": "error", "message": "No JSON payload provided"}), 400

        project_id = body.get("projectId", "").strip()
        manager    = body.get("accountManager", "").strip()
        comments   = body.get("comments", "").strip()

        if not project_id:
            return jsonify({"status": "error", "message": "projectId is required"}), 400
        if not comments:
            return jsonify({"status": "error", "message": "comments are required sequence"}), 400

        proj_table = os.getenv("KEKA_PROJECTS_TABLE", "keka_projects")
        
        scd2_update_project_manager(proj_table, project_id, manager, comments)
        
        from backend.shared.dbx_utils import invalidate_dbx_cache
        invalidate_dbx_cache()

        return jsonify({"status": "success", "message": "Account Manager updated successfully."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Route 2: Project Resources — resolved via Databricks SQL JOIN ─────────────

@project_bp.route('/api/project/resources', methods=['GET'])
def get_project_resources():
    """
    Returns resource allocations from keka_project_resources enriched with
    human-readable employee and project names using a Databricks SQL JOIN.

    Query params:
        projectId  — Keka project UUID to filter by (optional)
    """
    project_id = request.args.get('projectId', '').strip()

    catalog  = os.getenv("CATALOG_NAME")
    schema   = os.getenv("SCHEMA_NAME")
    res_tbl  = os.getenv("KEKA_PROJECT_RESOURCES_TABLE", "keka_project_resources")
    emp_tbl  = os.getenv("KEKA_EMPLOYEES_TABLE",          "keka_employees")
    proj_tbl = os.getenv("KEKA_PROJECTS_TABLE",           "keka_projects")

    if not all([catalog, schema]):
        return jsonify({"status": "error", "message": "Database env vars missing"}), 500

    # Fully-qualified table references
    R = f"`{catalog}`.`{schema}`.`{res_tbl}`"
    E = f"`{catalog}`.`{schema}`.`{emp_tbl}`"
    P = f"`{catalog}`.`{schema}`.`{proj_tbl}`"

    # Active SCD2 employee filter (enddate is null / empty)
    active_emp_cond  = "(e.enddate IS NULL OR e.enddate = '' OR LOWER(e.enddate) IN ('none','null'))"
    # Active SCD2 project filter — same logic for projects table
    active_proj_cond = "(p.enddate IS NULL OR p.enddate = '' OR LOWER(p.enddate) IN ('none','null'))"

    where = ""
    if project_id:
        safe_id = project_id.replace("'", "''")
        where = f"WHERE LOWER(r.projectid) = LOWER('{safe_id}')"

    sql = f"""
        SELECT
            r.employeeid,
            r.projectid,
            r.name                                                          AS allocation,
            COALESCE(
                NULLIF(e.displayName, ''),
                NULLIF(CONCAT(COALESCE(e.firstName,''), ' ', COALESCE(e.lastName,'')), ' '),
                r.employeeid
            )                                                               AS employeeName,
            COALESCE(NULLIF(p.name, ''), r.projectid)                      AS projectName,
            r.startdate,
            r.enddate,
            DATEDIFF(
                TO_DATE(r.enddate),
                TO_DATE(r.startdate)
            ) AS days_worked
        FROM {R} r
        LEFT JOIN {E} e
            ON LOWER(r.employeeid) = LOWER(e.id)
           AND {active_emp_cond}
        LEFT JOIN {P} p
            ON LOWER(r.projectid) = LOWER(p.id)
           AND {active_proj_cond}
        {where}
    """

    try:
        rows = execute_query(sql)
        result = [
            {
                "employeeName": row.get("employeeName") or row.get("employeeid") or "—",
                "projectName":  row.get("projectName")  or row.get("projectid")  or "—",
                "name":         row.get("allocation")   or "—",
                "startdate":    row.get("startdate"),
                "enddate":      row.get("enddate"),
                "daysWorked":   row.get("days_worked"),
            }
            for row in rows
        ]
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
