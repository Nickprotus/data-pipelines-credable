# src/utils.py 
import paramiko
import os
from loguru import logger
from typing import List
from dateutil import parser
import pandas as pd


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


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the taxi trip data."""

    # Standardize column naming
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Convert date/time columns
    date_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in date_cols:
        if col in df.columns:  # Check if column exists
            try:
                df[col] = pd.to_datetime(df[col])
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting column {col} to datetime: {e}.  Setting to NaT.")
                df[col] = pd.NaT  # Set invalid dates to NaT (Not a Time)
        else:
            logger.warning(f"Column {col} not found in DataFrame.")

    # Handle missing values
    for col in df.select_dtypes(include=['number']):
        df[col] = df[col].fillna(-1)  # Numeric columns: fill with -1
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].fillna("UNKNOWN") # String columns: fill with "UNKNOWN"

    # Remove duplicates
    df = df.drop_duplicates()

    # Filter out invalid data (example)
    df = df[df['trip_distance'] > 0]
    df = df[df['fare_amount'] > 0]
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