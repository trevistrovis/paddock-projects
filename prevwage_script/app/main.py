from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routes.webhook import router as webhook_router
from app.services.queue_service import RequestQueue
from app.routes.webhook import process_request_item

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    queue = RequestQueue.get_instance()
    await queue.start_worker(process_request_item)
    print("[APP] Queue worker started")
    yield
    # Shutdown
    await queue.stop_worker()
    print("[APP] Queue worker stopped")

app = FastAPI(title="Prevailing Wage Webhook Service", lifespan=lifespan)

@app.get("/")
def healthcheck():
    queue = RequestQueue.get_instance()
    return {
        "status": "running",
        "service": "prevailing-wage-webhook",
        "queue_size": queue.queue_size(),
    }

app.include_router(webhook_router)