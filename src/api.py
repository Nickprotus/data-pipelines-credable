# src/api.py
from fastapi import FastAPI, Query, HTTPException, Depends, Request 
from datetime import datetime
from typing import Optional, List, Dict
from sqlalchemy.orm import sessionmaker, Session
from .models import TaxiTrip
from .ingest import engine, TaxiTripDB
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel

# --- FastAPI Setup ---
app = FastAPI()

# --- Database Session ---
SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Rate Limiting ---
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# --- API Key Authentication (Simple Example) ---
API_KEY = "your_secret_api_key"

def verify_api_key(api_key: str = Query(...)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key

# --- Response Model ---
class TaxiTripResponse(BaseModel):
    data: List[TaxiTrip]
    next_cursor: Optional[int]
    has_more: bool

# --- API Endpoint ---
@app.get("/taxi_trips/", response_model=TaxiTripResponse)
@limiter.limit("100/minute")
async def get_taxi_trips(
    request: Request,  # Inject the Request object
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    cursor: Optional[int] = Query(None),
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db),
    api_key: str = Depends(verify_api_key)
):
    query = db.query(TaxiTripDB)

    if start_date:
        query = query.filter(TaxiTripDB.tpep_pickup_datetime >= start_date)
    if end_date:
        query = query.filter(TaxiTripDB.tpep_pickup_datetime <= end_date)
    if cursor:
        query = query.filter(TaxiTripDB.id > cursor)

    query = query.order_by(TaxiTripDB.id).limit(limit)
    trips = query.all()

    data = [TaxiTrip.from_orm(trip) for trip in trips]
    next_cursor = trips[-1].id if trips else None
    has_more = len(trips) == limit

    return {
        "data": data,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }