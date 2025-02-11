# FastAPI Docker Application Notes

This document provides notes and commands for building and running a Docker container for a FastAPI application.

## Application Details

The application is a simple FastAPI server that returns a greeting message.

```python
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello Kotaksakti Students"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
```

## Dockerfile

The Dockerfile uses Ubuntu 22.04 as the base image and sets up the environment for running the FastAPI application.

```dockerfile
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
```

## Docker Commands

### Building the Docker Image

To build the Docker image, use the following command:

```
sudo docker build -t fastapi-app:latest .
```

- `fastapi-app` is the image name
- `latest` is the tag (you can replace it with a version, e.g., `v1.0`, `dev`, etc.)
- `.` refers to the current directory containing the Dockerfile

Note: It's a good practice to use versioned tags for your images. For example:

```
sudo docker build -t fastapi-app:v1.0 .
```

### Running the Docker Container

To run the container, use:

```
sudo docker run -d --name fastapi-container -p 8080:8080 fastapi-app:latest
```

- `-d` runs the container in detached mode (in the background)
- `--name fastapi-container` assigns a custom name to the container
- `-p 8080:8080` maps port 8080 from the container to port 8080 on the host
- `fastapi-app:latest` is the name and tag of the image

Note: The `-p 8080:8080` part is crucial as it exposes the application port to the host machine. Without this, you wouldn't be able to access the FastAPI application from your host machine.

### Accessing the Application

After running the container, you can access the FastAPI application by opening a web browser and navigating to:

```
http://localhost:8080
```

If you're running the Docker container on a remote machine, replace `localhost` with the IP address or domain name of that machine.

### Stopping the Docker Container

To stop the running container:

```
sudo docker stop fastapi-container
```

### Removing the Docker Container

To remove the stopped container:

```
sudo docker rm fastapi-container
```

### Listing All Containers

To see all containers (running and stopped):

```
sudo docker ps -a
```

### Removing the Docker Image

To remove the Docker image:

```
sudo docker rmi fastapi-app:latest
```