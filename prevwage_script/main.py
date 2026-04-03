import os
import json
import requests
# import pandas as pd
import re
import mysql.connector
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

app = FastAPI(title="Prevailing Wage Webhook Service")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_API_URL = "https://api.monday.com/v2"

REQUESTS_BOARD_ID = 18402509426
RESULTS_BOARD_ID = 18402789789

# Requests board columns
REQ_COL_CITY_STATE_ZIP = "text_mm15g654"
REQ_COL_INSTALLER = "person"
REQ_COL_STATUS = "status"
REQ_COL_DATE_NEEDED = "date4"
REQ_COL_NOTES = "text_mm14a2aj"

# Results board columns
RES_COL_PERSON = "person"
RES_COL_STATUS = "status"
RES_COL_EFFECTIVE_DATE = "date4"
RES_COL_CITY_STATE_ZIP = "text_mm19tmfc"
RES_COL_COUNTY = "text_mm19787k"
RES_COL_FIPS = "text_mm192ady"
RES_COL_BASE_RATE = "numeric_mm19nxsc"
RES_COL_FRINGE_RATE = "numeric_mm19b9pk"

REQ_STATUS_LOOKUP_QUEUED = "Lookup Queued"
REQ_STATUS_RATE_FOUND = "Rate Found"
REQ_STATUS_REVIEWED = "Reviewed"

RES_STATUS_DONE = "Done"

#### LOCAL DATA LOADING (commented out for DB approach)
# COUNTY_DF = pd.read_excel(
#     "counties.xlsx",
#     dtype={
#         "State FIPS": str,
#         "County FIPS": str,
#         "FIPS": str,
#     }
# )
# COUNTY_DF.columns = [c.strip() for c in COUNTY_DF.columns]
# COUNTY_DF = COUNTY_DF[COUNTY_DF["County"].notna()].copy()

# COUNTY_DF["State"] = COUNTY_DF["State"].astype(str).str.strip().str.lower()
# COUNTY_DF["County"] = COUNTY_DF["County"].astype(str).str.strip().str.lower()
# COUNTY_DF["FIPS"] = COUNTY_DF["FIPS"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5)

# ZIP_DF = pd.read_csv("zips.csv", dtype={"Zip": str})
# ZIP_DF.columns = [c.strip() for c in ZIP_DF.columns]

# ZIP_DF["Zip"] = ZIP_DF["Zip"].astype(str).str.strip().str.zfill(5)
# ZIP_DF["State"] = ZIP_DF["State"].astype(str).str.strip().str.lower()
# ZIP_DF["County"] = ZIP_DF["County"].astype(str).str.strip().str.lower()

# WAGES_DF = pd.read_csv("wages.csv")
# WAGES_DF.columns = [c.strip() for c in WAGES_DF.columns]
# WAGES_DF["fips"] = (WAGES_DF["fips"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5))

# WAGES_DF["base_rate"] = WAGES_DF["base_rate"].astype(float)
# WAGES_DF["fringe_rate"] = WAGES_DF["fringe_rate"].astype(float)

# WAGES_DF["effective_date"] = pd.to_datetime(WAGES_DF["effective_date"])

# # optional
# if "expiration_date" in WAGES_DF.columns:
#     WAGES_DF["expiration_date"] = pd.to_datetime(
#         WAGES_DF["expiration_date"], errors="coerce"
#     )


class MondayClient:
    def __init__(self, token: str):
        if not token:
            raise ValueError("Missing MONDAY_API_TOKEN environment variable")
        self.token = token

    def _graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }
        payload = {
            "query": query,
            "variables": variables or {},
        }

        response = requests.post(
            MONDAY_API_URL,
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            raise RuntimeError(f"Monday API error: {data['errors']}")

        return data["data"]

    def get_request_item(self, item_id: int) -> Dict[str, Any]:
        query = """
        query ($board_id: ID!, $item_ids: [ID!]) {
          boards(ids: [$board_id]) {
            items_page(limit: 10, query_params: { ids: $item_ids }) {
              items {
                id
                name
                column_values {
                  id
                  text
                }
              }
            }
          }
        }
        """
        data = self._graphql(
            query,
            {
                "board_id": str(REQUESTS_BOARD_ID),
                "item_ids": [str(item_id)],
            },
        )

        boards = data.get("boards", [])
        if not boards:
            raise RuntimeError(f"Requests board {REQUESTS_BOARD_ID} not found")

        items = boards[0]["items_page"]["items"]
        if not items:
            raise RuntimeError(f"Request item {item_id} not found")

        return items[0]

    def create_result_item(
        self,
        project_name: str,
        city_state_zip: str,
        county: str,
        fips: str,
        base_rate: float,
        fringe_rate: float,
        effective_date: str,
    ) -> int:
        column_values = {
            RES_COL_CITY_STATE_ZIP: city_state_zip,
            RES_COL_COUNTY: county,
            RES_COL_FIPS: fips,
            RES_COL_BASE_RATE: base_rate,
            RES_COL_FRINGE_RATE: fringe_rate,
            RES_COL_EFFECTIVE_DATE: {"date": effective_date},
            RES_COL_STATUS: {"label": RES_STATUS_DONE},
        }

        mutation = """
        mutation ($board_id: ID!, $name: String!, $column_values: JSON!) {
          create_item(
            board_id: $board_id,
            item_name: $name,
            column_values: $column_values
          ) {
            id
          }
        }
        """
        data = self._graphql(
            mutation,
            {
                "board_id": str(RESULTS_BOARD_ID),
                "name": project_name,
                "column_values": json.dumps(column_values),
            },
        )

        return int(data["create_item"]["id"])

    def update_request_status(
        self,
        item_id: int,
        new_status: str,
        notes: Optional[str] = None,
    ) -> None:
        values = {
            REQ_COL_STATUS: {"label": new_status},
        }
        if notes:
            values[REQ_COL_NOTES] = notes

        mutation = """
        mutation ($board_id: ID!, $item_id: ID!, $column_values: JSON!) {
          change_multiple_column_values(
            board_id: $board_id,
            item_id: $item_id,
            column_values: $column_values
          ) {
            id
          }
        }
        """
        self._graphql(
            mutation,
            {
                "board_id": str(REQUESTS_BOARD_ID),
                "item_id": str(item_id),
                "column_values": json.dumps(values),
            },
        )

    def create_update(self, item_id: int, body: str) -> None:
        mutation = """
        mutation ($item_id: ID!, $body: String!) {
          create_update(item_id: $item_id, body: $body) {
            id
          }
        }
        """
        self._graphql(
            mutation,
            {
                "item_id": str(item_id),
                "body": body,
            },
        )


def extract_column_text(item: Dict[str, Any], column_id: str) -> str:
    for col in item.get("column_values", []):
        if col["id"] == column_id:
            return col.get("text") or ""
    return ""


def parse_request_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "item_id": int(item["id"]),
        "project_name": item["name"],
        "city_state_zip": extract_column_text(item, REQ_COL_CITY_STATE_ZIP),
        "date_needed": extract_column_text(item, REQ_COL_DATE_NEEDED),
        "notes": extract_column_text(item, REQ_COL_NOTES),
    }


def extract_item_id_from_webhook(payload: Dict[str, Any]) -> Optional[int]:
    """
    Tries several common webhook payload shapes.
    """
    candidates = [
        payload.get("event", {}).get("pulseId"),
        payload.get("event", {}).get("itemId"),
        payload.get("event", {}).get("pulse_id"),
        payload.get("event", {}).get("item_id"),
        payload.get("pulseId"),
        payload.get("itemId"),
        payload.get("pulse_id"),
        payload.get("item_id"),
    ]

    for value in candidates:
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass

    return None


def resolve_location(city_state_zip: str) -> Dict[str, str]:
    zip_code = extract_zip(city_state_zip)

    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute(
            "SELECT state_name, county_name FROM zips WHERE zip = %s",
            (zip_code,)
        )
        zip_row = cur.fetchone()

        if not zip_row:
            raise RuntimeError(f"ZIP {zip_code} not found")

        cur.execute(
            """
            SELECT fips, county_name
            FROM counties
            WHERE state_name = %s AND county_name = %s
            LIMIT 1
            """,
            (zip_row["state_name"], zip_row["county_name"])
        )
        county_row = cur.fetchone()

        if not county_row:
            raise RuntimeError(
                f"No FIPS match for {zip_row['county_name']}, {zip_row['state_name']}"
            )

        return {
            "county": county_row["county_name"],
            "fips": county_row["fips"],
        }
    finally:
        cur.close()
        conn.close()


def lookup_millwright_wage(fips: str, as_of_date: Optional[str] = None) -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if as_of_date:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                  AND effective_date <= %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips, as_of_date)
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM wages
                WHERE fips = %s
                ORDER BY effective_date DESC
                LIMIT 1
                """,
                (fips,)
            )

        row = cur.fetchone()

        if not row:
            raise RuntimeError(f"No wage found for FIPS {fips}")

        return {
            "base_rate": float(row["base_rate"]),
            "fringe_rate": float(row["fringe_rate"]),
            "effective_date": row["effective_date"].strftime("%Y-%m-%d"),
            "source_note": "",
        }
    finally:
        cur.close()
        conn.close()


def process_request_item(item_id: int) -> None:
    monday = MondayClient(MONDAY_API_TOKEN)

    request_item = monday.get_request_item(item_id)
    req = parse_request_item(request_item)

    try:
        location = resolve_location(req["city_state_zip"])
        wage = lookup_millwright_wage(location["fips"], req["date_needed"] or None)

        result_item_id = monday.create_result_item(
            project_name=req["project_name"],
            city_state_zip=req["city_state_zip"],
            county=location["county"],
            fips=location["fips"],
            base_rate=wage["base_rate"],
            fringe_rate=wage["fringe_rate"],
            effective_date=wage["effective_date"],
        )

        monday.update_request_status(
            item_id=req["item_id"],
            new_status=REQ_STATUS_RATE_FOUND,
            notes=f"Rate found and written to results board item {result_item_id}.",
        )

        monday.create_update(
            item_id=req["item_id"],
            body=(
                f"Lookup completed.\n"
                f"County: {location['county']}\n"
                f"FIPS: {location['fips']}\n"
                f"Base: ${wage['base_rate']:.2f}\n"
                f"Fringe: ${wage['fringe_rate']:.2f}\n"
                f"Effective Date: {wage['effective_date']}\n"
                f"{wage.get('source_note', '')}"
            ),
        )

    except Exception as exc:
        monday.update_request_status(
            item_id=req["item_id"],
            new_status=REQ_STATUS_REVIEWED,
            notes=f"Lookup failed: {str(exc)}",
        )
        monday.create_update(
            item_id=req["item_id"],
            body=f"Lookup failed: {str(exc)}",
        )

def extract_zip(text: str) -> str:
    match = re.search(r"\b\d{5}\b", text)
    if not match:
        raise RuntimeError(f"No ZIP code found in '{text}'")
    return match.group(0)

@app.get("/")
def healthcheck() -> Dict[str, Any]:
    return {
        "status": "running",
        "service": "prevailing-wage-webhook",
    }

@app.get("/test-location")
def test_location(input: str):
    try:
        result = resolve_location(input)
        return {
            "input": input,
            "result": result
        }
    except Exception as e:
        return {
            "input": input,
            "error": str(e)
        }

@app.post("/monday/webhook")
async def monday_webhook(request: Request, background_tasks: BackgroundTasks):
    raw = await request.body()
    print("RAW BODY:", raw.decode("utf-8", errors="replace"))

    payload = json.loads(raw.decode("utf-8") or "{}")

    # Monday verification handshake
    if "challenge" in payload:
        return Response(
            content=raw,
            media_type="application/json",
            status_code=200,
        )

    item_id = extract_item_id_from_webhook(payload)
    print(f"[WEBHOOK] Extracted item_id: {item_id}")

    if not item_id:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Could not determine item ID from webhook payload"},
        )

    background_tasks.add_task(process_request_item, item_id)

    return JSONResponse(
        status_code=200,
        content={"ok": True, "message": f"Processing item {item_id}"},
    )