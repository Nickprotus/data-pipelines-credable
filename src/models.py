# src/models.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class TaxiTrip(BaseModel):
    vendorid: Optional[int] = Field(None, alias='vendor_id')
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = Field(None, description="Passenger Count, -1 for missing")
    trip_distance: Optional[float] = None
    ratecodeid: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    pulocationid: Optional[int] = None
    dolocationid: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None
    trip_duration: Optional[float] = None

    class Config:
        from_attributes = True 
        populate_by_name = True 
