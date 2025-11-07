# genmp (Dockerized)

This repository contains an Azure Functions Python app in `genmp/`.

Quick start (PowerShell):

1. Copy the example env file and fill secrets:

   cp .\genmp\.env.example .\genmp\.env
   # Edit .\genmp\.env and add real values

2. Build and run with Docker Compose:

   docker compose build
   docker compose up

The service maps host port 7071 to container port 80 (Azure Functions host).

If you prefer plain Docker:

   docker build -t genmp:local ./genmp
   docker run --env-file ./genmp/.env -p 7071:80 --rm genmp:local

Security notes:
- Do not commit `.env` with secrets. Use a secret manager for production.
- For production images, consider multi-stage builds and smaller base images.

Files added:
- `genmp/Dockerfile` - image for Azure Functions Python app
- `genmp/.env.example` - env template
- `docker-compose.yml` - convenience compose file for local dev

Next steps:
- Add CI/CD pipeline to build and publish the image to a registry.
- Integrate secret storage (Azure Key Vault, HashiCorp Vault, etc.).
