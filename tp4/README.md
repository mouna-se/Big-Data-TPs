# TP04: Containerization and Big Data Environment Deployment Using Docker

- **Docker Hub image:** https://hub.docker.com/r/mouna2004/tp04-analysis
---

## Task 1: Installation

```
docker --version (photo1)
docker login (photo2)
```

An account was created on https://hub.docker.com, then I logged in from the terminal.

---

## Task 2: Build, Push, and Pull the Image

Prepared files: app/analysis.py, data/dataset.csv, requirements.txt, Dockerfile, and .dockerignore.

Commands:

```
docker build -t mouna2004/tp04-analysis:1.0 . (photo3)
docker run --rm mouna2004/tp04-analysis:1.0 (photo4)
docker push mouna2004/tp04-analysis:1.0 (photo5)
docker rmi mouna2004/tp04-analysis:1.0 (photo6)
docker pull mouna2004/tp04-analysis:1.0 (photo6)
docker images (photo7)
```
---

## Task 3: Network and the Three Containers

```
docker network create tp04-net (photo8)
```

```
docker run -d --name worker1 --network tp04-net -p 8001:8000 --memory=128m mouna2004/tp04-analysis:1.0 python -m http.server 8000 --directory /app/data (photo9)
docker run -d --name worker2 --network tp04-net -p 8002:8000 --memory=128m mouna2004/tp04-analysis:1.0 python -m http.server 8000 --directory /app/data (photo9)
docker run -d --name worker3 --network tp04-net -p 8003:8000 --memory=128m mouna2004/tp04-analysis:1.0 python -m http.server 8000 --directory /app/data (photo9)
```

Verification that the three containers are running, and network inspection:

```
docker ps (photo9)
docker network inspect tp04-net photo10
```

Cleanup:

```
docker stop worker1 worker2 worker3 (photo11)
docker rm worker1 worker2 worker3 (photo11)
docker network rm tp04-net (photo11)
```
---

## Task 4: Docker Compose

The file compose.yaml was created, then:

```
docker compose up --build (photo12)
docker compose ps -a (photo13)
docker compose down (photo13)
```

### Questions

**1. The problem Docker Compose solves**

It makes running multi-container applications (e.g., a web app, a database, and a cache) simple. Instead of writing several long docker run commands and manually creating networks and volumes, you define everything in a single docker-compose.yml file and start it with one command.

**2. Compare `docker run` with `docker compose up`**

| docker run | docker compose up |
|---|---|
| Creates one container per command | Creates and starts all services together |
| Configuration is typed in the command line | Configuration is saved in a YAML file |
| Hard to reproduce exactly | Reproducible with a single command |
| Networks and volumes are created manually | Created automatically |
| Does not build images | Can build images (`--build`) |

**3. How could you extend the Compose project to include a database service?**

Add a db service (PostgreSQL) to the compose.yaml file, with environment variables (user, password, database name) and a named volume to persist the data. Add depends_on to the analysis service so it starts after the database. The application connects to the database using db as the hostname, and psycopg2-binary is added to requirements.txt.
---

## Task 5: Report

**1. What is Docker, and why is it useful in data-science projects ?**

Docker is a platform that packages an application together with its code, libraries, and dependencies into a portable unit called a container. In data-science projects, it ensures reproducibility (“it works on my machine” problem disappears), makes environments easy to share with teammates, avoids library version conflicts, and simplifies moving projects from development to deployment.

**2. Explain the difference between a Docker image and a container**

A Docker image is a read-only template that contains the application, its dependencies, and its configuration. A container is a running instance of an image. One image can be used to start many containers, similar to how a class relates to its objects.

**3. Explain the purpose of FROM, WORKDIR, COPY, RUN, and CMD in the Dockerfile**

FROM: sets the base image to build on.
WORKDIR: sets the working directory inside the container for the following instructions.
COPY: copies files from the host machine into the image.
RUN: executes a command during the image build.
CMD: defines the default command that runs when the container starts.
---