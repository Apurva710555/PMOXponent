import os
import logging
import threading
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load environment early
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"[INFO] Loaded .env from {dotenv_path}")
else:
    print("[WARN] .env file not found.")

from backend.shared.keka_sync import sync_keka_data_to_dbx

# Initialize Flask App
app = Flask(__name__, template_folder="frontend", static_folder="frontend")
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev_secret_key")

# Register Blueprints
from backend.employee.routes import employee_bp
from backend.project.routes import project_bp
from backend.details.routes import details_bp
from backend.chatbot.routes import chatbot_bp

app.register_blueprint(employee_bp)
app.register_blueprint(project_bp)
app.register_blueprint(details_bp)
app.register_blueprint(chatbot_bp)

# Track sync status
_sync_lock = threading.Lock()
_sync_running = False

# Base Route
@app.route("/")
def home():
    return render_template("index.html")

# On-demand Keka Sync endpoint
@app.route("/api/sync", methods=["POST"])
def trigger_sync():
    global _sync_running
    if _sync_running:
        return jsonify({"status": "info", "message": "Sync already in progress..."}), 200

    def _run_sync():
        global _sync_running
        with _sync_lock:
            _sync_running = True
            try:
                print("[INFO] Manual sync triggered by user.")
                sync_keka_data_to_dbx()
                print("[INFO] Manual sync completed successfully.")
            except Exception as e:
                print(f"[ERROR] Sync failed: {e}")
            finally:
                _sync_running = False

    threading.Thread(target=_run_sync, daemon=True).start()
    return jsonify({"status": "success", "message": "Sync started in background. Data will refresh shortly."}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)

