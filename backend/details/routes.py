from flask import Blueprint, jsonify
import os
from backend.shared.dbx_utils import fetch_table_data

details_bp = Blueprint('details_bp', __name__)

@details_bp.route('/api/details/data', methods=['GET'])
def get_details_data():
    """
    Fetches Details table data from Databricks (mapping employees to projects with start/end date and status).
    """
    try:
        print("Inside get details")
        table_name = os.getenv("TRANSACTION_TABLE")
        print(table_name)
        if not table_name:
            return jsonify({"status": "success", "data": []}), 200
            
        data = fetch_table_data(table_name)
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
