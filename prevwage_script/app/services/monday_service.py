import json
import requests
from typing import Optional, Dict, Any
from app.config import (
    MONDAY_API_TOKEN,
    MONDAY_API_URL,
    REQUESTS_BOARD_ID,
    RESULTS_BOARD_ID,
    REQ_COL_STATUS,
    REQ_COL_NOTES,
    REQ_COL_CITY_STATE_ZIP,
    REQ_COL_DATE_NEEDED,
    RES_COL_CITY_STATE_ZIP,
    RES_COL_COUNTY,
    RES_COL_FIPS,
    RES_COL_BASE_RATE,
    RES_COL_FRINGE_RATE,
    RES_COL_EFFECTIVE_DATE,
    RES_COL_STATUS,
    RES_STATUS_DONE,
)

class MondayClient:
    def __init__(self, token: str = MONDAY_API_TOKEN):
        if not token:
            raise ValueError("Missing MONDAY_API_TOKEN")
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
        query ($item_ids: [ID!]) {
          items(ids: $item_ids) {
            id
            name
            column_values {
              id
              text
            }
          }
        }
        """
        data = self._graphql(query, {"item_ids": [str(item_id)]})
        items = data.get("items", [])
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
        self._graphql(mutation, {"item_id": str(item_id), "body": body})

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