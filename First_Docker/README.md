# Docker Commands Cheatsheet

This README provides a quick reference for common Docker commands used in building, running, managing containers, and handling images for a Python application.

## 1. Building the Docker Image

To build the Docker image, use the following command:

```
sudo docker build -t my_python_app:latest .
```

- `my_python_app` is the image name
- `latest` is the tag (you can replace it with a version, e.g., `v1.0`, `dev`, etc.)
- `.` refers to the current directory containing the Dockerfile

### Example: Building with a specific version tag

```
sudo docker build -t my_python_app:v1.0 .
```

This builds the image with the tag `v1.0`.

### Adding multiple tags in one build command

```
sudo docker build -t my_python_app:latest -t my_python_app:v1.0 .
```

This tags the image with both `latest` and `v1.0`.

## 2. Running the Docker Container

Once the image is built, you can run the container with this command:

```
sudo docker run -d --name my_python_app_container my_python_app
```

- `-d` runs the container in detached mode (in the background)
- `--name my_python_app_container` assigns a custom name to the container
- `my_python_app` is the name of the image

## 3. Stopping the Docker Container

To stop a running container, use:

```
sudo docker stop my_python_app_container
```

This stops the container named `my_python_app_container`.

## 4. Removing the Docker Container (Optional)

To remove a stopped container, use:

```
sudo docker rm my_python_app_container
```

This deletes the container.

## 5. Listing All Containers

To see all containers (running and stopped), use the following command:

```
sudo docker ps -a
```

This command shows:
- All containers, regardless of their state (running, stopped, exited)
- Container IDs
- Images used to create the containers
- Command executed in the container
- Creation time
- Status
- Ports (if any are exposed)
- Names of the containers

Use this command to get an overview of all your containers and their current states.

## 6. Removing Docker Images

To remove Docker images, you can use the following commands:

### List all images

First, list all available images:

```
sudo docker images
```

This will show you all images with their repository names, tags, and image IDs.

### Remove a specific image

To remove a specific image, use:

```
sudo docker rmi my_python_app:latest
```

Replace `my_python_app:latest` with the repository name and tag of the image you want to remove.

### Remove an image using its ID

You can also remove an image using its ID:

```
sudo docker rmi image_id
```

Replace `image_id` with the actual ID of the image you want to remove.

### Force removal of an image

If an image is being used by a stopped container or has multiple tags, you might need to force its removal:

```
sudo docker rmi -f my_python_app:latest
```

The `-f` flag forces the removal of the image.

### Remove all unused images

To remove all images that are not associated with a container, use:

```
sudo docker image prune -a
```

The `-a` flag removes all unused images, not just dangling ones.

**Note**: Be cautious when removing images, especially when using force removal or pruning all images. Make sure you don't need the images before removing them.