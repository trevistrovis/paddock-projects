import json
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool
from starlette.requests import Request

from app.config import REQ_STATUS_RATE_FOUND, REQ_STATUS_REVIEWED, ALL_WORKER_CLASSIFICATIONS
from app.services.monday_service import (
    MondayClient,
    extract_item_id_from_webhook,
    parse_request_item,
)
from app.services.location_service import resolve_location
from app.services.wage_service import lookup_worker_wage
from app.services.queue_service import RequestQueue

router = APIRouter()

async def process_request_item(item_id: int) -> None:
    print(f"[PROCESS] Starting item {item_id}")
    monday = MondayClient()

    try:
        request_item = monday.get_request_item(item_id)
        req = parse_request_item(request_item)
        print(f"[PROCESS] Parsed request: {req}")

        location = resolve_location(req["city_state_zip"])
        print(f"[PROCESS] Resolved location: {location}")

        # Loop through all worker classifications
        result_item_ids = []
        results_summary = []
        failed_classifications = []

        for worker_classification in ALL_WORKER_CLASSIFICATIONS:
            print(f"[PROCESS] Looking up wage for {worker_classification}")
            
            try:
                wage = await run_in_threadpool(
                    lookup_worker_wage,
                    fips=location["fips"],
                    worker_classification=worker_classification,
                    as_of_date=req["date_needed"] or None,
                )
                print(f"[PROCESS] Wage lookup result for {worker_classification}: {wage}")

                result_item_id = monday.create_result_item(
                    project_name=req["project_name"],
                    city_state_zip=req["city_state_zip"],
                    county=location["county"],
                    fips=location["fips"],
                    worker_classification=worker_classification,
                    base_rate=wage["base_rate"],
                    fringe_rate=wage["fringe_rate"],
                    effective_date=wage["effective_date"],
                )
                print(f"[PROCESS] Created result item for {worker_classification}: {result_item_id}")
                
                result_item_ids.append(result_item_id)
                results_summary.append(
                    f"{worker_classification}: Base=${wage['base_rate']:.2f}, Fringe=${wage['fringe_rate']:.2f}, Effective={wage['effective_date']}"
                )
                
            except Exception as exc:
                print(f"[PROCESS][ERROR] Failed to lookup {worker_classification}: {repr(exc)}")
                failed_classifications.append(f"{worker_classification}: {str(exc)}")
                continue

        # Update request status with all results
        if result_item_ids:
            notes = f"Rates found for {len(result_item_ids)} worker classification(s). Result items: {', '.join(map(str, result_item_ids))}"
            if failed_classifications:
                notes += f"\nFailed classifications: {'; '.join(failed_classifications)}"
            
            monday.update_request_status(
                item_id=req["item_id"],
                new_status=REQ_STATUS_RATE_FOUND,
                notes=notes,
            )

            # Create update with all results for comparison
            update_body = (
                f"Lookup completed for {location['county']} (FIPS: {location['fips']})\n\n"
                f"Results:\n" + "\n".join(f"• {r}" for r in results_summary)
            )
            if failed_classifications:
                update_body += f"\n\nFailed:\n" + "\n".join(f"• {f}" for f in failed_classifications)
            
            monday.create_update(
                item_id=req["item_id"],
                body=update_body,
            )
        else:
            # All classifications failed
            monday.update_request_status(
                item_id=req["item_id"],
                new_status=REQ_STATUS_REVIEWED,
                notes=f"All wage lookups failed: {'; '.join(failed_classifications)}",
            )
            monday.create_update(
                item_id=req["item_id"],
                body=f"All wage lookups failed:\n" + "\n".join(f"• {f}" for f in failed_classifications),
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

    queue = RequestQueue.get_instance()
    enqueued = await queue.enqueue(item_id)

    if not enqueued:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "Queue is full, please try again later"},
        )

    return JSONResponse(
        status_code=200,
        content={"ok": True, "message": f"Enqueued item {item_id} for processing", "queue_size": queue.queue_size()},
    )

@router.get("/test-location")
def test_location(input: str):
    try:
        result = resolve_location(input)
        return {"input": input, "result": result}
    except Exception as e:
        return {"input": input, "error": str(e)}