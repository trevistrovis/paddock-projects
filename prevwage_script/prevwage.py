import os
import json
import requests
from typing import Optional, Dict, Any, List

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_API_URL = "https://api.monday.com/v2"

REQUESTS_BOARD_ID = 18402509426
RESULTS_BOARD_ID = 18402789789

REQ_COL_CITY_STATE_ZIP = "text_mm15g654"
REQ_COL_INSTALLER = "person"
REQ_COL_STATUS = "status"
REQ_COL_DATE_NEEDED = "date4"
REQ_COL_NOTES = "text_mm14a2aj"

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

RES_STATUS_WORKING = "Working on it"
RES_STATUS_DONE = "Done"
RES_STATUS_STUCK = "Stuck"


class MondayClient:
    def __init__(self, token: str):
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
        resp = requests.post(MONDAY_API_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Monday API error: {data['errors']}")
        return data["data"]

    def get_queued_requests(self) -> List[Dict[str, Any]]:
        query = """
        query ($board_id: ID!) {
          boards(ids: [$board_id]) {
            items_page(
              limit: 100
              query_params: {
                rules: [
                  {column_id: "status", compare_value: [0], operator: any_of}
                ]
                operator: and
              }
            ) {
              items {
                id
                name
                column_values {
                  id
                  text
                  ... on MirrorValue { display_value }
                }
              }
            }
          }
        }
        """
        data = self._graphql(query, {"board_id": str(REQUESTS_BOARD_ID)})
        boards = data.get("boards", [])
        if not boards:
            return []
        return boards[0]["items_page"]["items"]

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

    def update_request_status(self, item_id: int, new_status: str, notes: Optional[str] = None) -> None:
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


def parse_request(item: Dict[str, Any]) -> Dict[str, str]:
    return {
        "item_id": item["id"],
        "project_name": item["name"],
        "city_state_zip": extract_column_text(item, REQ_COL_CITY_STATE_ZIP),
        "date_needed": extract_column_text(item, REQ_COL_DATE_NEEDED),
        "notes": extract_column_text(item, REQ_COL_NOTES),
    }


def resolve_location(city_state_zip: str) -> Dict[str, str]:
    """
    Placeholder.
    Replace this with:
    - Census geocoder
    - Google Maps API
    - your own county/FIPS lookup table
    """
    # Example dummy response:
    return {
        "county": "Allegheny County",
        "fips": "42003",
    }


def lookup_millwright_wage(fips: str, as_of_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Placeholder.
    Replace this with your actual Davis-Bacon lookup.
    Could be:
    - a local MySQL cache
    - a scraper/parser
    - an internal service
    """
    # Example dummy response:
    return {
        "base_rate": 42.50,
        "fringe_rate": 18.75,
        "effective_date": "2026-03-01",
        "source_note": f"Demo lookup for FIPS {fips}",
    }


def process_one_request(monday: MondayClient, item: Dict[str, Any]) -> None:
    req = parse_request(item)
    item_id = int(req["item_id"])

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
            item_id=item_id,
            new_status=REQ_STATUS_RATE_FOUND,
            notes=f"Rate found and written to results board item {result_item_id}.",
        )

        monday.create_update(
            item_id=item_id,
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

    except Exception as e:
        monday.update_request_status(
            item_id=item_id,
            new_status=REQ_STATUS_REVIEWED,
            notes=f"Lookup failed: {str(e)}",
        )
        monday.create_update(item_id=item_id, body=f"Lookup failed: {str(e)}")


def main() -> None:
    monday = MondayClient(MONDAY_API_TOKEN)
    queued_items = monday.get_queued_requests()

    if not queued_items:
        print("No queued items found.")
        return

    print(f"Found {len(queued_items)} queued request(s).")

    for item in queued_items:
        print(f"Processing request: {item['name']} ({item['id']})")
        process_one_request(monday, item)

    print("Done.")


if __name__ == "__main__":
    main()