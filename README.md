# Data Engineering: Data Pipeline and API Challenge

This project implements a scalable data pipeline that ingests data from a simulated SFTP location, processes it, and provides access through a FastAPI API with date-based filtering and cursor-based pagination.

## Project Structure

## Assumptions and Technologies

*   **Environment:**  Assumes a Linux-based server environment (suitable for Docker).
*   **SFTP Simulation:** Simulates an SFTP server locally using a directory (`data/raw` initially populated, then `data/sftp_upload` for Dockerized SFTP) and Docker.
*   **Data Source:** Uses publicly available NYC Taxi and Limousine Commission (TLC) Trip Record Data (Yellow Taxi Trip Records). Specifically, January and February 2023 data, converted to CSV and JSON formats. Download Parquet files from: [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
*   **Programming Language:** Python 3.9
*   **API Framework:** FastAPI
*   **Database:** SQLite (for simplicity; production would use PostgreSQL, MySQL, etc.)
*   **SFTP Library:** Paramiko
*   **Data Processing:** Pandas
*   **Data Validation:** Pydantic
*   **Logging:** Loguru
*   **Rate Limiting:** Slowapi
*   **Version Control:** Git (and GitHub/GitLab/Bitbucket)
*   **Containerization:** Docker, docker-compose

## Data Pipeline Stages

1.  **Data Ingestion (`src/ingest.py`, `src/utils.py`):**
    *   Connects to a simulated SFTP server (running in a Docker container) using `paramiko`.
    *   Retrieves files (CSV and JSON) from the specified remote directory.
    *   Handles `FileNotFoundError` and other potential `paramiko` exceptions.  Logs errors using `loguru`.

2.  **Data Processing (`src/ingest.py`, `src/utils.py`, `src/models.py`):**
    *   Loads data from CSV/JSON using pandas (`read_csv` and `read_json`), handling potential file errors.
    *   **Data Cleaning (`utils.py`):**
        *   Standardizes column names (lowercase, underscores).
        *   Converts date/time columns to pandas datetime objects.
        *   Handles missing values (fills numeric with -1, strings with "UNKNOWN").
        *   Removes duplicate rows.
        *   Filters out invalid data (e.g., zero/negative trip distance).
    *   **Data Validation (`models.py`):** Defines a `TaxiTrip` Pydantic model to ensure data consistency.
    *   **Data Storage (`ingest.py`):**
        *   Creates an SQLite database (`data/processed/taxi_data.db`) using SQLAlchemy.
        *   Defines a `TaxiTripDB` SQLAlchemy model mirroring the Pydantic model.
        *   Inserts cleaned data into the database, committing after each row and handling potential database errors (with rollback).
    *   **Chunking:** Reads and processes data in chunks to avoid out-of-memory errors.

3.  **API Development (`src/api.py`):**
    *   Creates a FastAPI application.
    *   Defines a `/taxi_trips/` endpoint.
    *   **Filtering:**
        *   `start_date`: Optional query parameter (ISO 8601 format).
        *   `end_date`: Optional query parameter (ISO 8601 format).
    *   **Pagination:**
        *   `cursor`: Optional query parameter (ID of the last retrieved record).
        *   `limit`: Optional query parameter (defaults to 100, max 500).
    *   **Security:**
        *   Basic API key authentication (`api_key` query parameter).
    *   **Rate Limiting:** Limits requests to 100 per minute using `slowapi`.
    *   **Documentation:** Automatically generates interactive API documentation (Swagger UI) at `/docs` and ReDoc at `/redoc`.
    *   **Response Model:** Uses a `TaxiTripResponse` Pydantic model for consistent API responses.

## Setup and Usage

**Prerequisites:**

*   Python 3.9+
*   pip
*   Git
*   Docker
*   Docker Compose

**Steps:**

1.  **Clone the Repository:**

    ```bash
    git clone <your_repository_url>  # Replace with your repository URL
    cd data_pipeline_project
    ```

2.  **Download and Prepare Data (Initial Setup - Only Once):**

    *   Download Yellow Taxi Trip Records (Parquet format) for January and February 2023 from [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).  Save *outside* the project.
    *   Create a `convert_data.py` script *outside* the project:

        ```python
        # convert_data.py (Outside the project directory)
        import pandas as pd
        import sys

        def convert_parquet(parquet_file):
            try:
                df = pd.read_parquet(parquet_file)
                csv_file = parquet_file.replace(".parquet", ".csv")
                json_file = parquet_file.replace(".parquet", ".json")
                df.to_csv(csv_file, index=False)
                df.to_json(json_file, orient="records")
                print(f"Converted {parquet_file} to CSV and JSON.")
            except Exception as e:
                print(f"Error converting {parquet_file}: {e}")

        if __name__ == "__main__":
            if len(sys.argv) > 1:
                for parquet_file in sys.argv[1:]:
                    convert_parquet(parquet_file)
            else:
                print("Please provide Parquet file paths as arguments.")
        ```

    *   Run `convert_data.py` to create CSV and JSON files:
        ```bash
        python convert_data.py yellow_tripdata_2023-01.parquet yellow_tripdata_2023-02.parquet
        ```

    *   Move the CSV and JSON files to `data/sftp_upload/`:
         ```bash
         mv yellow_tripdata_2023-0*.csv data_pipeline_project/data/sftp_upload/
         mv yellow_tripdata_2023-0*.json data_pipeline_project/data/sftp_upload/

         ```

3.  **Build and Run (Docker Compose):**

    ```bash
    docker-compose up --build
    ```
    This builds the Docker images (if necessary) and starts all services (SFTP server, ingestion, API).

4.  **Access the API:**

    *   Open `http://127.0.0.1:8000/docs` in your browser for the interactive API documentation.

5.  **API Usage Examples:**

    *   **Get all data (first 100 records):**
        ```
        [http://127.0.0.1:8000/taxi_trips/?api_key=your_secret_api_key](http://127.0.0.1:8000/taxi_trips/?api_key=your_secret_api_key)
        ```

    *   **Date filtering:**
        ```
        [http://127.0.0.1:8000/taxi_trips/?start_date=2023-01-01T00:00:00&end_date=2023-01-15T23:59:59&api_key=your_secret_api_key](http://127.0.0.1:8000/taxi_trips/?start_date=2023-01-01T00:00:00&end_date=2023-01-15T23:59:59&api_key=your_secret_api_key)
        ```

    *   **Pagination:**
        ```
        [http://127.0.0.1:8000/taxi_trips/?limit=50&api_key=your_secret_api_key](http://127.0.0.1:8000/taxi_trips/?limit=50&api_key=your_secret_api_key)
        ```
        (Get the `next_cursor` from the response, then use it:)
        ```
        [http://127.0.0.1:8000/taxi_trips/?limit=50&cursor=](http://127.0.0.1:8000/taxi_trips/?limit=50&cursor=)<next_cursor_value>&api_key=your_secret_api_key
        ```

    *   **`curl` example:**
        ```bash
        curl -X 'GET' \
          '[http://127.0.0.1:8000/taxi_trips/?start_date=2023-01-01T00:00:00&end_date=2023-01-05T00:00:00&api_key=your_secret_api_key](https://www.google.com/search?q=http://127.0.0.1:8000/taxi_trips/%3Fstart_date%3D2023-01-01T00:00:00%26end_date%3D2023-01-05T00:00:00%26api_key%3Dyour_secret_api_key)' \
          -H 'accept: application/json'
        ```
      **Important:** Replace `your_secret_api_key` with the actual key you set in `src/api.py`.

6.  **Running Individual Services:**

    *   **Ingestion only:**  `docker-compose up ingest --build`
    *   **API only:** `docker-compose up api --build`

7.  **Stopping Services:**

    ```bash
    docker-compose down
    ```

8.  **Running Tests:**

    ```bash
    pytest
    ```

## Configuration

*   **API Key:**  Change `API_KEY` in `src/api.py`.  **Do not commit secrets to version control.**  Use environment variables in production.
*   **SFTP Credentials:**  SFTP settings are hardcoded in `src/ingest.py` *for the simulation*.  In a real deployment, use a configuration file (e.g., `config.py`) or environment variables.
*   **Database URL:** `DATABASE_URL` in `src/ingest.py` points to a local SQLite file.  Change this for production (PostgreSQL, MySQL, etc.).

## Error Handling and Logging

*   Uses `loguru` for logging.  Logs are written to `logs/ingest.log`.
*   `try...except` blocks handle potential errors in SFTP, data loading, cleaning, and database operations.
*   The API returns appropriate HTTP status codes (401 for invalid API key, 422 for validation errors, 500 for internal server errors).  Rate limiting returns 429.

## Improvements and Future Enhancements

*   **Authentication:** Use OAuth 2.0 or a similar robust system.
*   **Configuration:** Use a config file or environment variables.
*   **Asynchronous Tasks:** Use Celery for long-running ingestion.
*   **Database:** Switch to PostgreSQL or MySQL for production.
*   **Data Validation:** Add more comprehensive validation rules.
*   **Monitoring:** Integrate with Prometheus, Grafana, etc.
*   **Dead-Letter Queue:** Store failed records for reprocessing.
*   **Schema Evolution:** Plan for handling changes to the data schema.
*   **Caching:**  Implement caching for frequently accessed data.
* **Orchestration:** Use Airflow or a similar tool.