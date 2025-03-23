# Use fficial Python base image
FROM python:3.9

# Setting the working directory inside the container
WORKDIR /app

# Copying the requirements file into the container
COPY requirements.txt .

# Installing the project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

ENV PYTHONPATH=/app

# Exposing port 8000 for the API
EXPOSE 8000

