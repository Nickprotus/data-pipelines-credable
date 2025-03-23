# src/utils.py 
import paramiko
import os
from loguru import logger
from typing import List
from dateutil import parser
import pandas as pd
import numpy as np  # Import numpy


def sftp_transfer(sftp_host: str, sftp_port: int, sftp_user: str, sftp_pass: str,
                 sftp_remote_path: str, local_destination_dir: str,
                 sftp_key_path: str = None):
    """
    Retrieves files from an SFTP server using paramiko.

    Args:
        sftp_host: SFTP server hostname or IP address.
        sftp_port: SFTP server port (usually 22).
        sftp_user: SFTP username.
        sftp_pass: SFTP password (use key-based authentication if possible).
        sftp_remote_path: Remote path on the SFTP server to retrieve files from.
        local_destination_dir: Local directory to save downloaded files.
        sftp_key_path: (Optional) Path to the private SSH key file.

    Raises:
        paramiko.SSHException: For SSH-related errors.
        FileNotFoundError:  If the remote path doesn't exist.
        Exception: For other errors.
    """
    try:
        # Create an SSH transport
        transport = paramiko.Transport((sftp_host, sftp_port))

        # Authentication (prefer key-based)
        if sftp_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(sftp_key_path)
            transport.connect(username=sftp_user, pkey=private_key)
            logger.info(f"Connecting to {sftp_host} with key {sftp_key_path}")
        else:
            transport.connect(username=sftp_user, password=sftp_pass)
            logger.info(f"Connecting to {sftp_host} with password")

        # Open an SFTP session
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create local destination directory if it doesn't exist
        os.makedirs(local_destination_dir, exist_ok=True)

        # Retrieve files from the remote path
        try:
            for filename in sftp.listdir(sftp_remote_path):
                remote_file_path = os.path.join(sftp_remote_path, filename)
                local_file_path = os.path.join(local_destination_dir, filename)
                sftp.get(remote_file_path, local_file_path)  # Download the file
                logger.info(f"Downloaded: {remote_file_path} -> {local_file_path}")
        except FileNotFoundError:
            logger.error(f"Remote path not found: {sftp_remote_path}")
            raise
        except Exception as e:
             logger.error(f"Error during file transfer from {sftp_remote_path}: {e}")
             raise


        # Close the SFTP session and transport
        sftp.close()
        transport.close()

    except paramiko.SSHException as e:
        logger.error(f"SSH Error: {e}")
        raise
    except Exception as e:
        logger.error(f"General Error during SFTP transfer: {e}")
        raise


# def simulate_sftp(file_paths: List[str], destination_dir: str):
#     """Simulates retrieving files from an SFTP server."""
#     os.makedirs(destination_dir, exist_ok=True)
#     for file_path in file_paths:
#         try:
#             # Copy the file locally.
#             destination_path = os.path.join(destination_dir, os.path.basename(file_path))
#             # Check if the source file exists
#             if os.path.exists(file_path):
#                 os.system(f"cp {file_path} {destination_path}")  # Use system copy
#                 logger.info(f"Simulated SFTP transfer: {file_path} -> {destination_path}")
#             else:
#                 logger.error(f"Source file not found: {file_path}")


#         except Exception as e:
#             logger.error(f"SFTP Simulation Error: {e}")
#             # In production, send an alert (email, Slack, etc.)


# def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
#     """Cleans the taxi trip data."""

#     # Standardize column naming
#     df.columns = [c.lower().replace(" ", "_") for c in df.columns]

#     # Convert date/time columns
#     date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
#     for col in date_cols:
#         if col in df.columns:  # Check if column exists
#             try:
#                 df[col] = pd.to_datetime(df[col])
#             except (ValueError, TypeError) as e:
#                 logger.warning(f"Error converting column {col} to datetime: {e}.  Setting to NaT.")
#                 df[col] = pd.NaT  # Set invalid dates to NaT (Not a Time)
#         else:
#             logger.warning(f"Column {col} not found in DataFrame.")

#     # Handle missing values
#     for col in df.select_dtypes(include=['number']):
#         df[col] = df[col].fillna(-1)  # Numeric columns: fill with -1
#     for col in df.select_dtypes(include=['object']):
#         df[col] = df[col].fillna("UNKNOWN") # String columns: fill with "UNKNOWN"

#     # Remove duplicates
#     df = df.drop_duplicates()

#     # Filter out invalid data (example)
#     df = df[df['trip_distance'] > 0]
#     df = df[df['fare_amount'] > 0]
#     return df

def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and prepares the NYC Taxi trip data.

    Applies the following cleaning steps:
    1. Standardizes column names (lowercase, underscores).
    2. Converts date/time columns to datetime objects.
    3. Handles missing values (imputation and replacement).
    4. Removes duplicate rows.
    5. Filters out invalid data (e.g., zero/negative trip distance, fare amount).
    6. Outlier Handling (using IQR for trip_distance and fare_amount).
    7. Data type consistency checks.
    8. Handles inconsistent string values (e.g., in 'store_and_fwd_flag').
    9. Adds a calculated 'trip_duration' column.

    Args:
        df: The input pandas DataFrame.

    Returns:
        A cleaned pandas DataFrame.
    """

    # 1. Standardize Column Names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # 2. Convert Date/Time Columns
    date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in date_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')  # 'coerce' converts invalid dates to NaT
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting column {col} to datetime: {e}. Setting to NaT.")
                df[col] = pd.NaT
        else:
            logger.warning(f"Column {col} not found in DataFrame.")

    # 3. Handling Missing Values
    #   a) Impute passenger_count with median
    if 'passenger_count' in df.columns:
        median_passengers = df['passenger_count'].median()
        df['passenger_count'] = df['passenger_count'].fillna(median_passengers).astype(int)
    for col in df.select_dtypes(include=['number']):
        df[col] = df[col].fillna(-1)  # Numeric columns: fill with -1
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].fillna("UNKNOWN") # String columns: fill with "UNKNOWN"

    # 4. Remove Duplicates
    df = df.drop_duplicates()

    # 5. Filter Invalid Data
    if 'trip_distance' in df.columns:
        df = df[df['trip_distance'] > 0]
    if 'fare_amount' in df.columns:
        df = df[df['fare_amount'] > 0]
    if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
         df = df[df['tpep_pickup_datetime'] < df['tpep_dropoff_datetime']] # drop rows where pickup time is later than dropoff.

    # 6. Outlier Handling (IQR Method)
    def remove_outliers_iqr(df, column):
        if column not in df.columns:
            return df
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        # Filter values to remove outliers
        df_filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

        # Log how many rows were removed
        rows_removed = len(df) - len(df_filtered)
        logger.info(f"Removed {rows_removed} outliers from column '{column}' using IQR method.")
        return df_filtered

    df = remove_outliers_iqr(df, 'trip_distance')
    df = remove_outliers_iqr(df, 'fare_amount')

    # 7. Ensure correct types after imputation
    if 'vendorid' in df.columns:
        df['vendorid'] = df['vendorid'].astype('Int64')  # Use pandas nullable integer type
    if 'passenger_count' in df.columns:
      df['passenger_count'] = df['passenger_count'].astype('Int64')
    if 'ratecodeid' in df.columns:
      df['ratecodeid'] = df['ratecodeid'].astype('Int64')
    if 'pulocationid' in df.columns:
      df['pulocationid'] = df['pulocationid'].astype('Int64')
    if 'dolocationid' in df.columns:
     df['dolocationid'] = df['dolocationid'].astype('Int64')
    if 'payment_type' in df.columns:
      df['payment_type'] = df['payment_type'].astype('Int64')

    # 8. Handle Inconsistent String Values
    if 'store_and_fwd_flag' in df.columns:
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].str.upper().replace({'N': 'N', 'Y': 'Y'}).fillna('UNKNOWN')
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str)


    # 9. Add Calculated Column (trip_duration)
    if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
        #Remove negative durations
        df = df[df['trip_duration']>=0]

    return df


def detect_file_type(filepath: str) -> str:
    """Detects file type (CSV or JSON) based on extension."""
    _, ext = os.path.splitext(filepath)
    if ext.lower() == ".csv":
        return "csv"
    elif ext.lower() == ".json":
        return "json"
    else:
        return "unknown"