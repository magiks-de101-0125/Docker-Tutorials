# Docker Compose Cheatsheet

This README provides a quick reference for common Docker Compose commands to manage multi-container applications.

## Running Docker Compose Services

Start services as defined in the `docker-compose.yml` file:

```bash
sudo docker-compose up
```

Run in detached mode (background):

```bash
sudo docker-compose up -d
```

Build images before starting containers:

```bash
sudo docker-compose up --build
```

## Stopping Services

Stop services gracefully:

```bash
sudo docker-compose stop
```

Stop and remove containers, networks, and volumes:

```bash
sudo docker-compose down
```

Stop and remove containers, networks, volumes, and images:

```bash
sudo docker-compose down --rmi all
```

## Viewing Containers

List running containers:

```bash
sudo docker-compose ps
```

List all containers (including stopped):

```bash
sudo docker-compose ps -a
```

## Viewing Logs

View logs of all services:

```bash
sudo docker-compose logs
```

Follow logs in real-time:

```bash
sudo docker-compose logs -f
```

View logs for a specific service:

```bash
sudo docker-compose logs [service_name]
```

## Executing Commands in Containers

Run a command in a running container:

```bash
sudo docker-compose exec [service_name] [command]
```

Example (open a shell in a service named 'web'):

```bash
sudo docker-compose exec web /bin/bash
```

## Scaling Services

Scale a service to multiple instances:

```bash
sudo docker-compose up --scale [service_name]=[number_of_instances]
```

Example (scale 'web' service to 3 instances):

```bash
sudo docker-compose up --scale web=3
```

## Managing Individual Services

Start a specific service:

```bash
sudo docker-compose start [service_name]
```

Stop a specific service:

```bash
sudo docker-compose stop [service_name]
```

Restart a specific service:

```bash
sudo docker-compose restart [service_name]
```

## Cleaning Up

Remove stopped service containers:

```bash
sudo docker-compose rm
```

Remove all unused containers, networks, images (both dangling and unreferenced), and optionally, volumes:

```bash
sudo docker system prune
```

## Useful Tips

- Use `docker-compose config` to validate and view the composed configuration.
- `docker-compose pull` updates images for services defined in the compose file.
- `docker-compose build` builds or rebuilds services.
- Use `--no-deps` flag with `up` to start a service without starting its dependencies.

Remember to run these commands in the directory containing your `docker-compose.yml` file.