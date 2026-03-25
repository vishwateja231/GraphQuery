"""
main.py
───────
FastAPI application entry point.
Run: uvicorn main:app --reload --port 8000
"""
import logging

from fastapi import FastAPI, Request  # pyre-ignore[21]
from fastapi.middleware.cors import CORSMiddleware  # pyre-ignore[21]
from fastapi.responses import JSONResponse  # pyre-ignore[21]

from database import close_db_pool, init_db_pool  # pyre-ignore[21]
from routers import orders, customers, products, analytics, query  # pyre-ignore[21]
from routers.query import LIVE_SCHEMA  # pyre-ignore[21]

app = FastAPI(
    title="SAP O2C API",
    description="Order-to-Cash analytics API backed by PostgreSQL",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# @app.exception_handler(Exception)
# async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
#     logging.error("Unhandled exception: %s", exc, exc_info=True)
#     return JSONResponse(
#         status_code=500,
#         content={"error": "Internal error", "details": str(exc)},
#     )


app.include_router(customers.router, prefix="/customers", tags=["Customers"])
app.include_router(products.router,  prefix="/products",  tags=["Products"])
app.include_router(orders.router,    prefix="/orders",    tags=["Orders"])
app.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
app.include_router(query.router,     prefix="/query",     tags=["LLM Query"])


@app.on_event("startup")
async def startup_event() -> None:
    init_db_pool()
    logging.info("[STARTUP] Loaded schema with %d tables", len(LIVE_SCHEMA.get("tables", {})))


@app.on_event("shutdown")
async def shutdown_event() -> None:
    close_db_pool()


@app.post("/test-query/")
async def test_query_direct(request: Request):
    print("DEBUG: Direct test query reached")
    return {"status": "ok"}


@app.get("/", tags=["Health"])
def root() -> dict:
    return {"status": "ok", "message": "SAP O2C API is running"}
