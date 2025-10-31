import os, time, json, hmac, hashlib
import requests
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI()

# --- ENV ---
PROJECT_ID = os.environ.get("PROJECT_ID", "")
INSTANCE_CONNECTION_NAME = os.environ["INSTANCE_CONNECTION_NAME"]  # "proj:region:instance"
DB_NAME = os.environ.get("DB_NAME", "staging")
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]

# Cloud SQL connector uses Unix socket
DB_HOST = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"

FINBOX_BASE_URL = os.environ.get("FINBOX_BASE_URL", "https://insights-api-uat.finbox.in")
FINBOX_API_KEY = os.environ["FINBOX_X_API_KEY"]
FINBOX_SERVER_HASH = os.environ["FINBOX_SERVER_HASH"]
FINBOX_VERSION = os.environ.get("FINBOX_VERSION", "6")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "45"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "3"))

class FetchPayload(BaseModel):
    customer_id: str
    request_id: str
    env: str = "uat"  # "uat" | "prod"

def make_salt(server_hash: str, customer_id: str) -> str:
    """
    Many vendors use salt = HMAC-SHA256(server_hash, customer_id) hex.
    CONFIRM with FinBox if format differs.
    """
    return hmac.new(server_hash.encode("utf-8"),
                    customer_id.encode("utf-8"),
                    hashlib.sha256).hexdigest()

def pg_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST
    )

def upsert_result(payload: FetchPayload, resp_json: dict):
    # extract top-level fields safely
    status = resp_json.get("status")
    message = resp_json.get("message")
    requested_at = resp_json.get("requested_at")
    processed_at = resp_json.get("processed_at")
    data = resp_json.get("data")  # FinBox features object (may be None)

    sql = """
    INSERT INTO finbox.insights_raw (customer_id, request_id, status, message, requested_at, processed_at, version, env, data)
    VALUES (%s, %s, %s, %s, %(requested_at)s::timestamptz, %(processed_at)s::timestamptz, %s, %s, %s::jsonb)
    ON CONFLICT (request_id) DO UPDATE
      SET status = EXCLUDED.status,
          message = EXCLUDED.message,
          requested_at = EXCLUDED.requested_at,
          processed_at = EXCLUDED.processed_at,
          version = EXCLUDED.version,
          env = EXCLUDED.env,
          data = EXCLUDED.data,
          received_at = now();
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    payload.customer_id,
                    payload.request_id,
                    status,
                    message,
                    FINBOX_VERSION,
                    payload.env,
                    json.dumps(data) if data is not None else None
                ),
                {"requested_at": requested_at or None, "processed_at": processed_at or None}
            )

@app.post("/fetch-insights")
def fetch_insights(body: FetchPayload):
    # select base URL
    base = "https://insights-api.finbox.in" if body.env == "prod" else FINBOX_BASE_URL

    # auth headers
    salt = make_salt(FINBOX_SERVER_HASH, body.customer_id)
    headers = {
        "x-api-key": FINBOX_API_KEY,
        "salt": salt,
        "version": FINBOX_VERSION,
        "Content-Type": "application/json"
    }

    url = f"{base}/api/v1/predictors/result?request_id={body.request_id}&customer_id={body.customer_id}"

    # poll loop
    deadline = time.time() + POLL_SECONDS
    last = None
    while time.time() < deadline:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 403:
            raise HTTPException(403, f"Unauthorized from FinBox (check x-api-key/salt/version). Body={r.text}")
        if r.status_code >= 400:
            raise HTTPException(502, f"FinBox returned {r.status_code}: {r.text}")

        last = r.json()
        status = (last.get("status") or "").lower()
        # common values: IN-PROGRESS / complete / COMPLETED
        if status in ("complete", "completed"):
            break
        time.sleep(POLL_INTERVAL)

    if not last:
        raise HTTPException(504, "No response from FinBox")

    # store into Postgres
    try:
        upsert_result(body, last)
    except Exception as e:
        raise HTTPException(500, f"DB insert failed: {e}")

    return {"ok": True, "stored_status": last.get("status"), "stored_message": last.get("message")}
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
