# src/ingest.py 
import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, DateTime, Float, String, text
from sqlalchemy.orm import sessionmaker, declarative_base
from loguru import logger
from src.models import TaxiTrip  # Imprting the Pydantic Model
from typing import List
from src.utils import sftp_transfer, clean_taxi_data, detect_file_type

# Database setup
DATABASE_URL = 'sqlite:///data/processed/taxi_data.db'
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class TaxiTripDB(Base):
    __tablename__ = 'taxi_trips'

    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer)
    tpep_pickup_datetime = Column(DateTime)
    tpep_dropoff_datetime = Column(DateTime)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    ratecodeid = Column(Integer)
    store_and_fwd_flag = Column(String)
    pulocationid = Column(Integer)
    dolocationid = Column(Integer)
    payment_type = Column(Integer)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    airport_fee = Column(Float)
    trip_duration = Column(Float)


Base.metadata.create_all(engine)  # Create tables


# Load_data with chunking)

def load_data(filepath: str, chunksize: int = 100000) -> pd.DataFrame:
    """Loads data from CSV or JSON file in chunks.

    Args:
        filepath: Path to the file.
        chunksize: Number of rows to read per chunk.

    Yields:
        pd.DataFrame: A chunk of the data.  Iterating over the result
                      of this function gives you DataFrames, each with
                      'chunksize' rows.
    """
    file_type = detect_file_type(filepath)
    try:
        if file_type == "csv":
            for chunk in pd.read_csv(filepath, dtype={'store_and_fwd_flag': str}, chunksize=chunksize):
                yield chunk
        elif file_type == "json":
            for chunk in pd.read_json(filepath, orient="records", lines=True, chunksize=chunksize):
                yield chunk
        else:
            logger.error(f"Unsupported file type: {filepath}")
            #  Yields an empty dataframe so that the caller will not break.
            yield pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading data from {filepath}: {e}")
        yield pd.DataFrame()

def store_data(df: pd.DataFrame):
    """Stores the cleaned DataFrame into the SQLite database."""
    Session = sessionmaker(bind=engine)
    session = Session()

    for _, row in df.iterrows():
        try:
            trip = TaxiTrip(**row.to_dict())
            db_trip = TaxiTripDB(**trip.model_dump(by_alias=True))
            session.add(db_trip)
            session.commit()  # Commit after EACH successful add
        except Exception as e:
            session.rollback() # Rollback if there is an error
            logger.error(f"Error processing row: {row}. Error: {e}")

    session.close()

def main():
    """Main ingestion pipeline logic."""
    logger.add("logs/ingest.log", rotation="500 MB", level="INFO")

    # SFTP Configuration
    SFTP_HOST = 'sftp_server'
    SFTP_PORT = 22
    SFTP_USER = 'testuser'
    SFTP_PASS = 'testpassword'
    SFTP_REMOTE_PATH = '/upload'
    SFTP_KEY_PATH = None
    LOCAL_DESTINATION_DIR = "data/raw"

    # SFTP Transfer
    try:
        sftp_transfer(
            sftp_host=SFTP_HOST,
            sftp_port=SFTP_PORT,
            sftp_user=SFTP_USER,
            sftp_pass=SFTP_PASS,
            sftp_remote_path=SFTP_REMOTE_PATH,
            local_destination_dir=LOCAL_DESTINATION_DIR,
            sftp_key_path=SFTP_KEY_PATH
        )
    except Exception as e:
        logger.error("Failed to transfer the files: %s", e)
        return  # Exit if file transfer fails

    raw_files = [os.path.join(LOCAL_DESTINATION_DIR, f) for f in os.listdir(LOCAL_DESTINATION_DIR)
                    if os.path.isfile(os.path.join(LOCAL_DESTINATION_DIR, f))]

    for filepath in raw_files:
        logger.info(f"Processing file: {filepath}")
        for df_chunk in load_data(filepath):  # Iterate over chunks
            if isinstance(df_chunk, pd.DataFrame) and not df_chunk.empty:
                df_cleaned = clean_taxi_data(df_chunk)
                store_data(df_cleaned)
        logger.info("Finished storing the data") # Moved outside to prevent multiple messages

if __name__ == "__main__":
    main()
