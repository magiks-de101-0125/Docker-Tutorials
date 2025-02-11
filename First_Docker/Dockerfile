# Use the official Ubuntu 22.04 base image
# This is the OS your container will run on
FROM ubuntu:22.04

# Set the working directory inside the container to /app
# All subsequent commands will be executed in this directory
WORKDIR /app

# Copy the current directory (where Dockerfile is located) on the host
# to the /app directory inside the container
COPY . /app

# Update the package list for Ubuntu and install Python 3
# The -y flag automatically confirms the installation prompts
RUN apt-get update && apt-get install -y python3 python3-pip

# COMMET OUT DULU
# Install icecream package using pip3
RUN pip3 install icecream

# Specify the command to run when the container starts
# In this case, it's running Python 3 and executing the "app.py" file
CMD ["python3", "app.py"]
