# Tdrive: Personal Media Worker

A high-performance, user-deployable component of the **Tdrive** ecosystem. This worker service is designed to bridge a user's Telegram media channels with their Tdrive cloud storage, allowing for private, secure, and automated media management.

## 🚀 Overview

The Tdrive Personal Worker is an intelligent, containerized service that bridges the gap between messaging platforms and personal cloud storage. It is designed to be deployed by individual users to monitor their own Telegram channels, process media, and securely synchronize with the Tdrive backend.

## 🛠 Tech Stack

- **Framework**: FastAPI (Asynchronous Python)
- **Engine**: Telethon (MTProto API)
- **Containerization**: Docker & Docker Compose
- **Server**: Gunicorn with Uvicorn workers
- **Security**: Secure RSA Exchange for credential management

## 📊 Data Pipeline Flow

The system operates on an event-driven architecture to ensure data integrity and real-time synchronization:

1.  **Ingestion**: An asynchronous listener monitors configured Telegram channels for new media messages.
2.  **Validation**: Upon detection, the service validates file metadata (MIME type, size, and naming conventions).
3.  **Registration**: A secure handshake is performed with the backend to register the incoming asset.
4.  **Processing**: Media is processed using a background task queue to prevent blocking the main event loop.
5.  **Synchronization**: Status updates and asset availability are synchronized with the central database through the Internal API.

## 📦 Deployment & Scaling

This service is fully containerized, making it compatible with modern cloud platforms (e.g., Render, AWS ECS, DigitalOcean).

### Local Development (Docker)

Ensure you have Docker and Docker Compose installed, then run:

```powershell
docker-compose up --build
```
The service will be available at `http://localhost:10000`.

### Cloud Deployment (e.g., Render)

1.  **Connect Repository**: Link this GitHub repository to your Render Web Service.
2.  **Runtime**: Select **Docker** as the environment.
3.  **Environment Variables**:
    - `BACKEND_URL`: URL of your central backend service.
    - `PORT`: 10000
4.  **Deploy**: Render will automatically build the image using the provided `Dockerfile` and start the worker.

## ⚙️ Advanced Features

- **Dynamic Resource Manager**: Automatically adjusts concurrency limits based on system CPU and RAM availability.
- **Secure RSA Exchange**: Decrypts storage credentials on-the-fly using asymmetric encryption, ensuring sensitive API keys are never stored in plain text.
- **Fast Encryption**: Leverages `cryptg` for 10x faster Telegram media processing.

---
*Created for professional deployment and high-scale media orchestration.*
