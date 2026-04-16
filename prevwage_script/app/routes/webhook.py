import json
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool
from starlette.requests import Request

from app.config import REQ_STATUS_RATE_FOUND, REQ_STATUS_REVIEWED
from app.services.monday_service import (
    MondayClient,
    extract_item_id_from_webhook,
    parse_request_item,
)
from app.services.location_service import resolve_location
from app.services.wage_service import lookup_millwright_wage

router = APIRouter()

def process_request_item(item_id: int) -> None:
    print(f"[PROCESS] Starting item {item_id}")
    monday = MondayClient()

    try:
        request_item = monday.get_request_item(item_id)
        req = parse_request_item(request_item)
        print(f"[PROCESS] Parsed request: {req}")

        location = resolve_location(req["city_state_zip"])
        print(f"[PROCESS] Resolved location: {location}")

        wage = lookup_millwright_wage(location["fips"], req["date_needed"] or None)
        print(f"[PROCESS] Wage lookup result: {wage}")

        result_item_id = monday.create_result_item(
            project_name=req["project_name"],
            city_state_zip=req["city_state_zip"],
            county=location["county"],
            fips=location["fips"],
            base_rate=wage["base_rate"],
            fringe_rate=wage["fringe_rate"],
            effective_date=wage["effective_date"],
        )
        print(f"[PROCESS] Created result item: {result_item_id}")

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
        print(f"[PROCESS][ERROR] Item {item_id} failed: {repr(exc)}")
        try:
            monday.update_request_status(
                item_id=item_id,
                new_status=REQ_STATUS_REVIEWED,
                notes=f"Lookup failed: {str(exc)}",
            )
            monday.create_update(
                item_id=item_id,
                body=f"Lookup failed: {str(exc)}",
            )
        except Exception as inner_exc:
            print(f"[PROCESS][ERROR] Failed reporting error to Monday: {repr(inner_exc)}")

@router.post("/monday/webhook")
async def monday_webhook(request: Request):
    raw = await request.body()
    print("RAW BODY:", raw.decode("utf-8", errors="replace"))

    payload = json.loads(raw.decode("utf-8") or "{}")

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

    await run_in_threadpool(process_request_item, item_id)

    return JSONResponse(
        status_code=200,
        content={"ok": True, "message": f"Processed item {item_id}"},
    )

@router.get("/test-location")
def test_location(input: str):
    try:
        result = resolve_location(input)
        return {"input": input, "result": result}
    except Exception as e:
        return {"input": input, "error": str(e)}