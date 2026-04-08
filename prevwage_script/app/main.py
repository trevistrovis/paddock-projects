from fastapi import FastAPI
from app.routes.webhook import router as webhook_router

app = FastAPI(title="Prevailing Wage Webhook Service")

@app.get("/")
def healthcheck():
    return {
        "status": "running",
        "service": "prevailing-wage-webhook",
    }

app.include_router(webhook_router)