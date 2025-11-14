# Conversational SQL Agent

A minimal end-to-end project: Streamlit frontend + FastAPI backend that accepts table uploads and answers natural-language queries by generating SQL via Azure OpenAI and executing against Azure SQL.

## Repo layout

- backend/ — FastAPI app, DB helpers, LLM integration, upload utilities
- frontend/ — Streamlit UI
- .env.example — example local environment variables (DO NOT commit secrets)
- Dockerfile.backend — container image for backend
- .github/workflows/azure-deploy.yml — optional GitHub Actions deploy workflow

## Quick start (local development)

1. Clone repository
2. Create and activate virtual environments for backend and frontend:
   - python -m venv .venv
   - source .venv/bin/activate
3. Copy `.env.example` to `.env` and fill values (local dev only; do not commit).
4. Install dependencies:
   - pip install -r backend/requirements.txt
   - pip install -r frontend/requirements.txt
5. Run backend (from repo root):
   - uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
6. Run frontend:
   - streamlit run frontend/app.py
7. Open Streamlit UI (default http://localhost:8501) and interact.

## Environment variables

See `.env.example`. In production, set these in your cloud provider (App Service/Container Apps/Azure Key Vault) — never commit secrets.

## Docker

Build backend image (example):

```bash
docker build -f Dockerfile.backend -t myorg/text2sql-backend:latest .
