# Use the official Python 3.10 image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the application code
COPY ./app /app

# Install the dependencies
RUN pip install -r requirements.txt

# Create the /data directory for storing local files
RUN mkdir /data

# Expose port 8080 for the FastAPI app
EXPOSE 8080

# Run the FastAPI app with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]