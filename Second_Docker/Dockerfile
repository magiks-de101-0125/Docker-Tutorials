# Use the official Ubuntu 22.04 base image
FROM ubuntu:22.04

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory to /app inside the container
COPY . /app

# Install Python 3 and pip (Python's package manager)
RUN apt-get update && apt-get install -y python3 python3-pip

# Install FastAPI and Uvicorn using pip
RUN pip3 install fastapi uvicorn

# Expose port 8080 for the FastAPI app
EXPOSE 8080

# Run the FastAPI app using Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]