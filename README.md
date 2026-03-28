# Multi-Tenant API Rate Limiter

A FastAPI-based HTTP middleware for per-tenant, per-endpoint rate limiting.

## Setup & Run

### Using Docker (Recommended)
```bash
docker-compose up --build -d
# API available at http://localhost:8000
```

### Local Development
```bash
# Start Redis
docker run -d -p 6379:6379 redis:alpine

# Install dependencies
pip install -r requirements.txt

# Run server
STORAGE_BACKEND=redis REDIS_URL=redis://localhost:6379/0 uvicorn app.main:app --reload
```
