import os
import json
import requests
from databricks import sql

ACCOUNT_HOST = os.getenv("ACCOUNT_HOST")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")


def get_dbx_access_token():
    token_url = f"{ACCOUNT_HOST}/oidc/accounts/{ACCOUNT_ID}/v1/token"
    token_payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "all-apis",
    }

    response = requests.post(token_url, data=token_payload)
    response.raise_for_status()

    return response.json().get("access_token")


def get_dbx_connection():
    """
    Returns a connection to Databricks SQL endpoint.
    Strips https:// from WORKSPACE_INSTANCE since sql.connect expects just the hostname.
    """
    host = os.getenv("WORKSPACE_INSTANCE", "")
    # sql.connect expects just the hostname (e.g., "adb-xxx.4.azuredatabricks.net")
    host = host.replace("https://", "").replace("http://", "").rstrip("/")

    token = get_dbx_access_token()
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/endpoints/YOUR_ENDPOINT_ID")

    return sql.connect(server_hostname=host, http_path=http_path, access_token=token)


import time

_QUERY_CACHE = {}
_CACHE_TTL = 300  # 5 minutes

def invalidate_dbx_cache():
    """Clears the in-memory Databricks query cache."""
    global _QUERY_CACHE
    _QUERY_CACHE.clear()
    print("[INFO] Databricks query cache invalidated.")

def fetch_table_data(table_name, use_cache=True):
    """
    Fetches all data from the specified table in the configured dbx catalog/schema.
    Returns a list of dictionaries. Returns [] if table does not exist.
    """
    global _QUERY_CACHE
    catalog = os.getenv("CATALOG_NAME")
    schema = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError(
            "Database configuration missing: Check CATALOG_NAME, SCHEMA_NAME, and TABLE environments."
        )

    query = f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}`"

    if use_cache:
        cached = _QUERY_CACHE.get(query)
        if cached and time.time() - cached['time'] < _CACHE_TTL:
            print(f"[INFO] Using cached result for {table_name}")
            return cached['data']

    try:
        with get_dbx_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
                print(f"[INFO] Fetched {len(data)} rows from {catalog}.{schema}.{table_name}")
                
                if use_cache:
                    _QUERY_CACHE[query] = {'time': time.time(), 'data': data}
                    
                return data
    except Exception as e:
        error_str = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str.lower():
            print(f"[WARN] Table {catalog}.{schema}.{table_name} does not exist yet. Returning empty.")
        else:
            print(f"[ERROR] Database query failed: {e}")
        return []


def execute_query(sql_str, use_cache=True):
    """
    Executes an arbitrary SQL statement against the configured Databricks connection.
    Returns a list of row dicts. Useful for JOIN queries that span multiple tables.
    """
    global _QUERY_CACHE
    if use_cache:
        cached = _QUERY_CACHE.get(sql_str)
        if cached and time.time() - cached['time'] < _CACHE_TTL:
            print(f"[INFO] Using cached SQL result")
            return cached['data']

    try:
        with get_dbx_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_str)
                columns = [col[0] for col in cursor.description]
                rows    = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
                
                if use_cache:
                    _QUERY_CACHE[sql_str] = {'time': time.time(), 'data': data}
                    
                return data
    except Exception as e:
        print(f"[ERROR] execute_query failed: {e}")
        raise



def sync_to_dbx_table(table_name, data):
    """
    Overwrites the specified table with new data. Creates the table if it does not exist.
    'data' should be a list of dictionaries. If empty, still creates the table structure.
    """
    catalog = os.getenv("CATALOG_NAME")
    schema = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError(
            "Database configuration missing: Check CATALOG_NAME, SCHEMA_NAME, and TABLE environments."
        )

    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"

    # If data is empty, we have no columns to infer — just log and return
    if not data:
        print(f"[INFO] No data to write for table {table_name}. Skipping.")
        return

    cols_list = list(data[0].keys())

    def clean_val(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def format_row(row):
        """Format a single row as a SQL VALUES tuple string."""
        parts = []
        for col in cols_list:
            v = clean_val(row.get(col))
            if v is None:
                parts.append("NULL")
            else:
                escaped = v.replace("\\", "\\\\").replace("'", "\\'")
                parts.append(f"'{escaped}'")
        return f"({', '.join(parts)})"

    BATCH_SIZE = 500  # rows per INSERT statement

    try:
        with get_dbx_connection() as connection:
            with connection.cursor() as cursor:
                # Create table if it doesn't exist (all STRING columns for API data safety)
                col_defs = ", ".join([f"`{c}` STRING" for c in cols_list])
                create_qry = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({col_defs})"
                print(f"[INFO] Ensuring table exists: {full_table_name}")
                cursor.execute(create_qry)

                # Truncate table for full sync
                cursor.execute(f"TRUNCATE TABLE {full_table_name}")

                # Batch INSERT — 50 rows per SQL statement
                columns_str = ", ".join([f"`{c}`" for c in cols_list])
                total = len(data)
                inserted = 0

                for i in range(0, total, BATCH_SIZE):
                    batch = data[i : i + BATCH_SIZE]
                    values_list = ", ".join(format_row(row) for row in batch)
                    insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES {values_list}"
                    cursor.execute(insert_sql)
                    inserted += len(batch)
                    print(f"[INFO] Inserted batch {i // BATCH_SIZE + 1} ({inserted}/{total} rows)")

            if hasattr(connection, "commit"):
                connection.commit()

            print(f"[INFO] Successfully synced {inserted} records to {full_table_name}.")

    except Exception as e:
        print(f"[ERROR] Database insertion failed for {table_name}: {e}")
        import traceback
        traceback.print_exc()
        raise


def append_to_dbx_table(table_name, data):
    """
    APPENDS rows to the specified table (does NOT truncate first).
    Automatically adds a '_sync_timestamp' column with today's UTC ISO timestamp.
    Creates the table if it does not exist.
    'data' should be a list of dictionaries.
    """
    import datetime

    catalog = os.getenv("CATALOG_NAME")
    schema = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError(
            "Database configuration missing: Check CATALOG_NAME, SCHEMA_NAME, and TABLE environments."
        )

    if not data:
        print(f"[INFO] No data to append for table {table_name}. Skipping.")
        return

    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    sync_ts = datetime.datetime.utcnow().isoformat()

    # Inject timestamp into every row
    stamped = [{**row, "_sync_timestamp": sync_ts} for row in data]
    cols_list = list(stamped[0].keys())

    def clean_val(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def format_row(row):
        parts = []
        for col in cols_list:
            v = clean_val(row.get(col))
            if v is None:
                parts.append("NULL")
            else:
                escaped = v.replace("\\", "\\\\").replace("'", "\\'")
                parts.append(f"'{escaped}'")
        return f"({', '.join(parts)})"

    BATCH_SIZE = 500

    try:
        with get_dbx_connection() as connection:
            with connection.cursor() as cursor:
                # Create table if it doesn't exist
                col_defs = ", ".join([f"`{c}` STRING" for c in cols_list])
                create_qry = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({col_defs})"
                print(f"[INFO] Ensuring history table exists: {full_table_name}")
                cursor.execute(create_qry)

                # Batch INSERT (no TRUNCATE — append only)
                columns_str = ", ".join([f"`{c}`" for c in cols_list])
                total = len(stamped)
                inserted = 0

                for i in range(0, total, BATCH_SIZE):
                    batch = stamped[i: i + BATCH_SIZE]
                    values_list = ", ".join(format_row(row) for row in batch)
                    insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES {values_list}"
                    cursor.execute(insert_sql)
                    inserted += len(batch)
                    print(f"[INFO] Appended batch {i // BATCH_SIZE + 1} ({inserted}/{total} rows) to {table_name}")

            if hasattr(connection, "commit"):
                connection.commit()

            print(f"[INFO] Successfully appended {inserted} records to {full_table_name}.")

    except Exception as e:
        print(f"[ERROR] Append failed for {table_name}: {e}")
        import traceback
        traceback.print_exc()
        raise


# ── Tracked Keka fields that trigger a new SCD2 version ──────────────────
_SCD2_TRACKED_FIELDS = {
    'firstName', 'lastName', 'displayName', 'email',
    'designation', 'jobTitle', 'department', 'businessUnit',
    'reportsTo', 'location', 'employeeStatus',
}

_SCD2_PROJ_TRACKED_FIELDS = {
    'name', 'code', 'clientId', 'billingType', 'status',
    'projectBudget', 'projectManagers', 'ProjectStartDate', 'ProjectEndDate'
}

# Sentinel values that mean "empty / no end date" (active row)
_ACTIVE_SENTINELS = {None, 'None', '', 'null', 'NULL'}


# def scd2_sync_employees(table_name, incoming_data):
#     """
#     SCD Type 2 sync engine for the employee master table.

#     Rules:
#     - New employee (not in DB)          → INSERT with startdate=NOW, enddate=NULL
#     - Existing, tracked fields changed  → UPDATE old row enddate=NOW,
#                                           INSERT new row (carry over status/comments)
#     - Existing, nothing changed         → skip (no write)
#     - In DB but missing from Keka       → UPDATE enddate=NOW (terminated)
#     """
#     import datetime

#     catalog = os.getenv("CATALOG_NAME")
#     schema  = os.getenv("SCHEMA_NAME")

#     if not all([catalog, schema, table_name]):
#         raise ValueError("Database configuration missing: Check CATALOG_NAME, SCHEMA_NAME, and TABLE environments.")

#     if not incoming_data:
#         print(f"[WARN] No incoming employee data. Skipping SCD2 sync.")
#         return

#     full_table  = f"`{catalog}`.`{schema}`.`{table_name}`"
#     now_ts      = datetime.datetime.utcnow().isoformat()
#     SCD2_COLS   = ['status', 'comments', 'startdate', 'enddate', 'modifiedby']

#     def _clean(v):
#         if isinstance(v, (dict, list)):
#             return json.dumps(v)
#         return str(v) if v is not None else None

#     def _esc(v):
#         if v is None:
#             return "NULL"
#         s = str(v)
#         return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

#     def _is_active(row):
#         return row.get('enddate') in _ACTIVE_SENTINELS

#     try:
#         with get_dbx_connection() as conn:
#             with conn.cursor() as cursor:

#                 # ── Step 1: Ensure table + SCD2 columns exist ────────
#                 keka_cols = list(incoming_data[0].keys())
#                 all_cols  = keka_cols + [c for c in SCD2_COLS if c not in keka_cols]

#                 col_defs  = ", ".join([f"`{c}` STRING" for c in all_cols])
#                 cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")

#                 for col in SCD2_COLS:
#                     try:
#                         cursor.execute(f"ALTER TABLE {full_table} ADD COLUMN `{col}` STRING")
#                         print(f"[INFO] Migration: added column `{col}` to {table_name}")
#                     except Exception:
#                         pass  # column already exists — expected on 2nd+ run

#                 # ── Step 2: Load all active rows from DB ─────────────
#                 cursor.execute(
#                     f"SELECT * FROM {full_table} "
#                     f"WHERE `enddate` IS NULL OR `enddate` = 'None' OR `enddate` = ''"
#                 )
#                 db_cols    = [d[0] for d in cursor.description]
#                 active_map = {}  # employeeNumber → row dict
#                 for row in cursor.fetchall():
#                     rd      = dict(zip(db_cols, row))
#                     emp_key = str(rd.get('employeeNumber') or rd.get('id') or '').strip()
#                     if emp_key:
#                         active_map[emp_key] = rd

#                 # Build incoming map
#                 incoming_map = {}
#                 for emp in incoming_data:
#                     emp_key = str(emp.get('employeeNumber') or emp.get('id') or '').strip()
#                     if emp_key:
#                         incoming_map[emp_key] = emp

#                 new_count = updates = skipped = terminated = 0

#                 # ── Step 3: Process each incoming employee ────────────
#                 for emp_key, incoming_emp in incoming_map.items():

#                     if emp_key not in active_map:
#                         # Brand-new employee
#                         row = {**incoming_emp,
#                                'status': '', 'comments': '',
#                                'startdate': now_ts, 'enddate': None, 'modifiedby': ''}
#                         cols_str = ", ".join([f"`{c}`" for c in all_cols])
#                         vals_str = ", ".join([_esc(_clean(row.get(c))) for c in all_cols])
#                         cursor.execute(f"INSERT INTO {full_table} ({cols_str}) VALUES ({vals_str})")
#                         new_count += 1

#                     else:
#                         current = active_map[emp_key]
#                         changed = any(
#                             str(_clean(incoming_emp.get(f)) or '') != str(current.get(f) or '')
#                             for f in _SCD2_TRACKED_FIELDS
#                         )

#                         if changed:
#                             # Carry over manual fields before closing the row
#                             old_status   = current.get('status',   '') or ''
#                             old_comments = current.get('comments', '') or ''

#                             # Close current row
#                             cursor.execute(
#                                 f"UPDATE {full_table} SET `enddate` = {_esc(now_ts)} "
#                                 f"WHERE `employeeNumber` = {_esc(emp_key)} "
#                                 f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
#                             )

#                             # Insert new active row
#                             row = {**incoming_emp,
#                                    'status': old_status, 'comments': old_comments,
#                                    'startdate': now_ts, 'enddate': None, 'modifiedby': ''}
#                             cols_str = ", ".join([f"`{c}`" for c in all_cols])
#                             vals_str = ", ".join([_esc(_clean(row.get(c))) for c in all_cols])
#                             cursor.execute(f"INSERT INTO {full_table} ({cols_str}) VALUES ({vals_str})")
#                             updates += 1
#                         else:
#                             skipped += 1  # nothing changed — leave row untouched

#                 # ── Step 4: Close rows for terminated employees ───────
#                 for emp_key in active_map:
#                     if emp_key not in incoming_map:
#                         cursor.execute(
#                             f"UPDATE {full_table} SET `enddate` = {_esc(now_ts)} "
#                             f"WHERE `employeeNumber` = {_esc(emp_key)} "
#                             f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
#                         )
#                         terminated += 1
#                         print(f"[SCD2] {emp_key} not found in Keka — record closed.")

#             if hasattr(conn, 'commit'):
#                 conn.commit()

#         print(
#             f"[SCD2] Sync complete — "
#             f"{new_count} new | {updates} updated | {skipped} unchanged | {terminated} terminated"
#         )

#     except Exception as e:
#         print(f"[ERROR] scd2_sync_employees failed: {e}")
#         import traceback
#         traceback.print_exc()
#         raise
def scd2_sync_employees(table_name, incoming_data):
    import datetime
    import json

    catalog = os.getenv("CATALOG_NAME")
    schema  = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError("Database configuration missing")

    if not incoming_data:
        print("[WARN] No incoming employee data.")
        return

    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"
    now_ts = datetime.datetime.utcnow().isoformat()

    SCD2_COLS = ['status', 'comments', 'startdate', 'enddate', 'modifiedby']

    def _clean(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def _esc(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    try:
        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:

                # ── Step 1: Ensure table ───────────────────────────
                keka_cols = list(incoming_data[0].keys())
                all_cols  = keka_cols + [c for c in SCD2_COLS if c not in keka_cols]

                col_defs = ", ".join([f"`{c}` STRING" for c in all_cols])
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")

                # ── Step 2: Load active rows ───────────────────────
                cursor.execute(
                    f"SELECT * FROM {full_table} "
                    f"WHERE enddate IS NULL OR enddate = '' OR enddate = 'None'"
                )

                db_cols = [d[0] for d in cursor.description]
                active_map = {}

                for row in cursor.fetchall():
                    rd = dict(zip(db_cols, row))
                    key = str(rd.get('employeeNumber') or rd.get('id') or '').strip()
                    if key:
                        active_map[key] = rd

                incoming_map = {
                    str(emp.get('employeeNumber') or emp.get('id')).strip(): emp
                    for emp in incoming_data
                }

                # ── Step 3: PREPARE BATCHES ────────────────────────
                inserts = []
                updates_to_close = []
                terminated_keys = []

                for emp_key, incoming_emp in incoming_map.items():

                    if emp_key not in active_map:
                        # NEW
                        row = {
                            **incoming_emp,
                            'status': '',
                            'comments': '',
                            'startdate': now_ts,
                            'enddate': None,
                            'modifiedby': ''
                        }
                        inserts.append(row)

                    else:
                        current = active_map[emp_key]

                        changed = any(
                            str(_clean(incoming_emp.get(f)) or '') != str(current.get(f) or '')
                            for f in _SCD2_TRACKED_FIELDS
                        )

                        if changed:
                            updates_to_close.append(emp_key)

                            row = {
                                **incoming_emp,
                                'status': current.get('status', ''),
                                'comments': current.get('comments', ''),
                                'startdate': now_ts,
                                'enddate': None,
                                'modifiedby': ''
                            }
                            inserts.append(row)

                # terminated employees
                for emp_key in active_map:
                    if emp_key not in incoming_map:
                        terminated_keys.append(emp_key)

                # ── Step 4: BULK UPDATE (close old rows) ───────────
                if updates_to_close:
                    keys = ",".join([_esc(k) for k in updates_to_close])
                    cursor.execute(f"""
                        UPDATE {full_table}
                        SET enddate = {_esc(now_ts)}
                        WHERE employeeNumber IN ({keys})
                        AND (enddate IS NULL OR enddate = '' OR enddate = 'None')
                    """)

                # ── Step 5: BULK TERMINATION ───────────────────────
                if terminated_keys:
                    keys = ",".join([_esc(k) for k in terminated_keys])
                    cursor.execute(f"""
                        UPDATE {full_table}
                        SET enddate = {_esc(now_ts)}
                        WHERE employeeNumber IN ({keys})
                        AND (enddate IS NULL OR enddate = '' OR enddate = 'None')
                    """)

                # ── Step 6: BULK INSERT ────────────────────────────
                if inserts:
                    chunk_size = 100

                    for i in range(0, len(inserts), chunk_size):
                        batch = inserts[i:i+chunk_size]

                        values = []
                        for row in batch:
                            vals = ", ".join([_esc(_clean(row.get(c))) for c in all_cols])
                            values.append(f"({vals})")

                        cursor.execute(f"""
                            INSERT INTO {full_table}
                            ({", ".join([f"`{c}`" for c in all_cols])})
                            VALUES {",".join(values)}
                        """)

            conn.commit()

        print(f"[SCD2] Done: {len(inserts)} inserts | {len(updates_to_close)} updates | {len(terminated_keys)} terminated")

    except Exception as e:
        print(f"[ERROR] SCD2 failed: {e}")
        import traceback
        traceback.print_exc()
        raise

def scd2_update_status(table_name, employee_number, new_status, new_comments):
    """
    SCD Type 2 update triggered by the UI status/comments change.

    Closes the current active row and inserts a new one with the
    updated status and comments, carrying over all Keka fields.
    """
    import datetime

    catalog = os.getenv("CATALOG_NAME")
    schema  = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError("Database configuration missing.")

    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"
    now_ts     = datetime.datetime.utcnow().isoformat()
    SCD2_COLS  = ['status', 'comments', 'startdate', 'enddate', 'modifiedby']

    def _clean(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def _esc(v):
        if v is None:
            return "NULL"
        s = str(v)
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

    try:
        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:

                # ── Migration: add SCD2 columns if they don't exist yet ──
                # This allows status updates to work even before the first
                # SCD2 sync has run (which would normally add these columns).
                for col in SCD2_COLS:
                    try:
                        cursor.execute(f"ALTER TABLE {full_table} ADD COLUMN `{col}` STRING")
                        print(f"[INFO] Migration: added column `{col}` to {table_name}")
                    except Exception:
                        pass  # column already exists

                # Load the current active row
                cursor.execute(
                    f"SELECT * FROM {full_table} "
                    f"WHERE `employeeNumber` = {_esc(employee_number)} "
                    f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
                )
                db_cols = [d[0] for d in cursor.description]
                rows    = cursor.fetchall()

                if not rows:
                    raise ValueError(f"No active record found for employee '{employee_number}'")

                current = dict(zip(db_cols, rows[0]))

                # Close the old row
                cursor.execute(
                    f"UPDATE {full_table} SET `enddate` = {_esc(now_ts)} "
                    f"WHERE `employeeNumber` = {_esc(employee_number)} "
                    f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
                )

                # Build new row — all same Keka fields, new status/comments
                new_row = {
                    **current,
                    'status':     new_status,
                    'comments':   new_comments,
                    'startdate':  now_ts,
                    'enddate':    None,
                    'modifiedby': '',
                }
                cols     = list(new_row.keys())
                cols_str = ", ".join([f"`{c}`" for c in cols])
                vals_str = ", ".join([_esc(_clean(new_row.get(c))) for c in cols])
                cursor.execute(f"INSERT INTO {full_table} ({cols_str}) VALUES ({vals_str})")

            if hasattr(conn, 'commit'):
                conn.commit()

        print(f"[SCD2] Status updated for {employee_number}: '{new_status}'")

    except Exception as e:
        print(f"[ERROR] scd2_update_status failed: {e}")
        import traceback
        traceback.print_exc()
        raise
def scd2_sync_projects(table_name, incoming_data):
    """
    SCD Type 2 sync engine for the project master table (PSA).
    """
    import datetime
    import json

    catalog = os.getenv("CATALOG_NAME")
    schema  = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError("Database configuration missing")

    if not incoming_data:
        print("[WARN] No incoming project data.")
        return

    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"
    now_ts = datetime.datetime.utcnow().isoformat()

    SCD2_COLS = ['accountmanager', 'comments', 'startdate', 'enddate', 'modify']

    def _clean(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def _esc(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    try:
        # Pre-process Keka API data to map timeline dates to the custom DB columns
        for proj in incoming_data:
            if 'startDate' in proj:
                proj['ProjectStartDate'] = proj.pop('startDate')
            if 'endDate' in proj:
                proj['ProjectEndDate'] = proj.pop('endDate')

        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:

                keka_cols = list(incoming_data[0].keys())
                # Filter out any incoming columns that collide case-insensitively with our SCD fields
                safe_keka_cols = [c for c in keka_cols if c.lower() not in [sc.lower() for sc in SCD2_COLS]]
                all_cols  = safe_keka_cols + SCD2_COLS

                col_defs = ", ".join([f"`{c}` STRING" for c in all_cols])
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")

                cursor.execute(
                    f"SELECT * FROM {full_table} "
                    f"WHERE enddate IS NULL OR enddate = '' OR enddate = 'None'"
                )

                db_cols = [d[0] for d in cursor.description]
                active_map = {}

                for row in cursor.fetchall():
                    rd = dict(zip(db_cols, row))
                    key = str(rd.get('id') or '').strip()
                    if key:
                        active_map[key] = rd

                incoming_map = {
                    str(proj.get('id')).strip(): proj
                    for proj in incoming_data if proj.get('id')
                }

                inserts = []
                updates_to_close = []
                terminated_keys = []

                for proj_key, incoming_proj in incoming_map.items():
                    safe_incoming = {k: incoming_proj[k] for k in safe_keka_cols if k in incoming_proj}

                    if proj_key not in active_map:
                        row = {
                            **safe_incoming,
                            'accountmanager': '',
                            'comments': '',
                            'startdate': now_ts,
                            'enddate': None,
                            'modify': ''
                        }
                        inserts.append(row)
                    else:
                        current = active_map[proj_key]
                        changed = any(
                            str(_clean(safe_incoming.get(f)) or '') != str(current.get(f) or '')
                            for f in _SCD2_PROJ_TRACKED_FIELDS if f in safe_incoming
                        )

                        if changed:
                            updates_to_close.append(proj_key)
                            row = {
                                **safe_incoming,
                                'accountmanager': current.get('accountmanager', ''),
                                'comments': current.get('comments', ''),
                                'startdate': now_ts,
                                'enddate': None,
                                'modify': ''
                            }
                            inserts.append(row)

                for proj_key in active_map:
                    if proj_key not in incoming_map:
                        terminated_keys.append(proj_key)

                if updates_to_close:
                    keys_str = ",".join([_esc(k) for k in updates_to_close])
                    cursor.execute(f"""
                        UPDATE {full_table}
                        SET enddate = {_esc(now_ts)}
                        WHERE id IN ({keys_str})
                        AND (enddate IS NULL OR enddate = '' OR enddate = 'None')
                    """)

                if terminated_keys:
                    keys_str = ",".join([_esc(k) for k in terminated_keys])
                    cursor.execute(f"""
                        UPDATE {full_table}
                        SET enddate = {_esc(now_ts)}
                        WHERE id IN ({keys_str})
                        AND (enddate IS NULL OR enddate = '' OR enddate = 'None')
                    """)

                if inserts:
                    chunk_size = 100
                    for i in range(0, len(inserts), chunk_size):
                        batch = inserts[i:i+chunk_size]
                        values = []
                        for row in batch:
                            vals = ", ".join([_esc(_clean(row.get(c))) for c in all_cols])
                            values.append(f"({vals})")
                        cursor.execute(f"""
                            INSERT INTO {full_table}
                            ({", ".join([f"`{c}`" for c in all_cols])})
                            VALUES {",".join(values)}
                        """)

            conn.commit()

        print(f"[SCD2] Projects Done: {len(inserts)} inserts | {len(updates_to_close)} updates | {len(terminated_keys)} terminated")

    except Exception as e:
        print(f"[ERROR] SCD2 Projects failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def scd2_update_project_manager(table_name, project_id, account_manager, comments):
    """
    SCD Type 2 update triggered by UI account manager changes.
    """
    import datetime
    import json

    catalog = os.getenv("CATALOG_NAME")
    schema  = os.getenv("SCHEMA_NAME")

    if not all([catalog, schema, table_name]):
        raise ValueError("Database configuration missing.")

    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"
    now_ts     = datetime.datetime.utcnow().isoformat()
    SCD2_COLS  = ['accountmanager', 'comments', 'startdate', 'enddate', 'modify']

    def _clean(v):
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v) if v is not None else None

    def _esc(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    try:
        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:
                for col in SCD2_COLS:
                    try:
                        cursor.execute(f"ALTER TABLE {full_table} ADD COLUMN `{col}` STRING")
                    except Exception:
                        pass

                cursor.execute(
                    f"SELECT * FROM {full_table} "
                    f"WHERE `id` = {_esc(project_id)} "
                    f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
                )
                db_cols = [d[0] for d in cursor.description]
                rows    = cursor.fetchall()

                if not rows:
                    raise ValueError(f"No active record found for project '{project_id}'")

                current = dict(zip(db_cols, rows[0]))

                cursor.execute(
                    f"UPDATE {full_table} SET `enddate` = {_esc(now_ts)} "
                    f"WHERE `id` = {_esc(project_id)} "
                    f"AND (`enddate` IS NULL OR `enddate` = 'None' OR `enddate` = '')"
                )

                new_row = {
                    **current,
                    'accountmanager': account_manager,
                    'comments':       comments,
                    'startdate':      now_ts,
                    'enddate':        None,
                    'modify':         '',
                }
                cols     = list(new_row.keys())
                cols_str = ", ".join([f"`{c}`" for c in cols])
                vals_str = ", ".join([_esc(_clean(new_row.get(c))) for c in cols])
                cursor.execute(f"INSERT INTO {full_table} ({cols_str}) VALUES ({vals_str})")

            if hasattr(conn, 'commit'):
                conn.commit()

        print(f"[SCD2] Account Manager updated for project {project_id}: '{account_manager}'")

    except Exception as e:
        print(f"[ERROR] scd2_update_project_manager failed: {e}")
        import traceback
        traceback.print_exc()
        raise

def merge_timeentries(table_name, data):
    """
    Upserts (MERGE) Timesheet entries into Databricks using a temporary staging table to 
    prevent full truncation. Matches on Keka's internal `id` key.
    """
    catalog = os.getenv("CATALOG_NAME")
    schema = os.getenv("SCHEMA_NAME")
    
    if not all([catalog, schema, table_name]):
        raise ValueError("Missing database configuration.")
        
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"
    staging_table = f"`{catalog}`.`{schema}`.`{table_name}_staging`"
    
    if not data:
        print("[INFO] No timeentries to merge.")
        return
        
    # Keka pagination can sometimes return duplicate records. 
    # Databricks MERGE strictly requires unique source rows. Deduplicate by `id`.
    unique_data = {}
    for row in data:
        rid = row.get("id")
        if rid:
            unique_data[rid] = row
            
    deduped_data = list(unique_data.values())
    if not deduped_data:
        print("[INFO] No valid timeentries with IDs to merge.")
        return
        
    cols_list = list(deduped_data[0].keys())
    
    def clean_val(v):
        if isinstance(v, (dict, list)):
            import json
            return json.dumps(v)
        return str(v) if v is not None else None
        
    try:
        with get_dbx_connection() as conn:
            with conn.cursor() as cursor:
                # Ensure target table exists
                col_defs = ", ".join([f"`{c}` STRING" for c in cols_list])
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({col_defs})")
                
                # Drop and recreate empty staging table
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                cursor.execute(f"CREATE TABLE {staging_table} LIKE {full_table}")
                
                # Bulk insert into staging using large batch size
                BATCH_SIZE = 500
                columns_str = ", ".join([f"`{c}`" for c in cols_list])
                total = len(deduped_data)
                
                print(f"[INFO] Merging {total} records via staging table...")
                
                for i in range(0, total, BATCH_SIZE):
                    batch = deduped_data[i:i+BATCH_SIZE]
                    parts = []
                    for row in batch:
                        row_parts = []
                        for col in cols_list:
                            v = clean_val(row.get(col))
                            if v is None:
                                row_parts.append("NULL")
                            else:
                                escaped = v.replace("\\", "\\\\").replace("'", "\\'")
                                row_parts.append(f"'{escaped}'")
                        parts.append(f"({', '.join(row_parts)})")
                        
                    values_str = ", ".join(parts)
                    cursor.execute(f"INSERT INTO {staging_table} ({columns_str}) VALUES {values_str}")
                    
                # Perform the MERGE INTO the permanent target table
                merge_sql = f"""
                MERGE INTO {full_table} target
                USING {staging_table} source
                ON target.id = source.id
                WHEN MATCHED THEN
                  UPDATE SET *
                WHEN NOT MATCHED THEN
                  INSERT *
                """
                cursor.execute(merge_sql)
                
                # Clean up staging table
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                
            if hasattr(conn, "commit"):
                conn.commit()
                
        print(f"[INFO] Successfully merged timeentries into {full_table}.")
        
    except Exception as e:
        print(f"[ERROR] merge_timeentries failed: {e}")
        import traceback
        traceback.print_exc()
        raise
