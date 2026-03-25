import logging
from fastapi import APIRouter, Request, JSONResponse
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)

class QuestionRequest(BaseModel):
    question: str

@router.post("/")
async def natural_language_query(request: Request):
    print("DEBUG: Minimal query endpoint reached")
    return JSONResponse(content={"status": "ok", "message": "Minimal endpoint works"}, status_code=200)

@router.post("/stream/")
async def stream_query(request: Request):
    print("DEBUG: Minimal stream endpoint reached")
    return JSONResponse(content={"status": "ok", "message": "Minimal stream works"}, status_code=200)

LIVE_SCHEMA = {}
def load_schema_from_db():
    return {"tables": {}}
