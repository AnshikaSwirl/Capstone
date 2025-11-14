# backend/main.py

import os
import shutil
import logging
import asyncio
from typing import List
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
from werkzeug.utils import secure_filename  # pip install Werkzeug

# Import your agent and upload helper
from backend.agent import graph_agent
from backend.upload_utils import upload_new_table

# Configuration
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploaded_files")
os.makedirs(UPLOAD_DIR, exist_ok=True)
MAX_UPLOAD_SIZE_BYTES = int(os.environ.get("MAX_UPLOAD_SIZE_BYTES", 50 * 1024 * 1024))  # 50 MB default

# Logging
logger = logging.getLogger("backend")
logging.basicConfig(level=logging.INFO)

# FastAPI app
app = FastAPI(title="Conversational SQL Agent")

# CORS: adjust origins in production
ALLOW_ORIGINS = os.environ.get("ALLOW_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple in-memory conversation storage (replace with DB for persistence)
conversation_history = {}

# Pydantic models
class AskRequest(BaseModel):
    user_query: str
    session_id: str = "default"

# Helpers
def _save_upload_file_tmp(upload_file: UploadFile, dest_dir: str) -> str:
    """
    Save uploaded file to dest_dir and return the file path.
    Uses secure_filename to avoid path traversal.
    """
    filename = secure_filename(upload_file.filename)
    if not filename:
        raise ValueError("Invalid filename")
    dest_path = Path(dest_dir) / filename

    # Prevent overwriting by appending numeric suffix if existing
    if dest_path.exists():
        stem = dest_path.stem
        suffix = dest_path.suffix
        i = 1
        while True:
            candidate = Path(dest_dir) / f"{stem}_{i}{suffix}"
            if not candidate.exists():
                dest_path = candidate
                break
            i += 1

    # Write file
    with open(dest_path, "wb") as f:
        data = upload_file.file.read()
        if len(data) > MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(status_code=413, detail="Uploaded file too large")
        f.write(data)
    # Reset file pointer for downstream libs (though we wrote content already)
    try:
        upload_file.file.seek(0)
    except Exception:
        pass

    return str(dest_path)

# Endpoint: upload a new table (async wrapper)
@app.post("/upload_new_table")
async def upload_new_table_api(file: UploadFile = File(...), table_name: str = Form(...)):
    try:
        # Basic validation
        if not table_name or not table_name.strip():
            raise HTTPException(status_code=400, detail="table_name is required")

        # Save file safely
        saved_path = await asyncio.to_thread(_save_upload_file_tmp, file, UPLOAD_DIR)

        # Run upload utility in thread to avoid blocking
        result = await asyncio.to_thread(upload_new_table, saved_path, table_name.strip())

        return JSONResponse(status_code=200, content={"status": "success", "message": result})
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Upload failed: %s", e)
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Upload failed: {str(e)}"})

# Endpoint: ask the graph agent
@app.post("/ask_graph_agent")
async def ask_graph_agent(request: AskRequest):
    user_query = request.user_query
    session_id = request.session_id or "default"

    if not user_query or not user_query.strip():
        raise HTTPException(status_code=400, detail="user_query is required")

    # Reconstruct memory from in-memory conversation_history
    previous_conversation = conversation_history.get(session_id, [])
    memory_text = "\n".join([f"User: {turn['user']}\nAI: {turn['bot']}" for turn in previous_conversation])

    # Build initial state
    state = {
        "query": user_query,
        "memory": memory_text,
    }

    try:
        # Run potentially blocking graph_agent.invoke() in threadpool so FastAPI event loop remains responsive
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, graph_agent.invoke, state)

        # Normalize and store response
        answer = result.get("answer") or result.get("response") or "No response generated."
        previous_conversation.append({"user": user_query, "bot": answer})
        conversation_history[session_id] = previous_conversation

        return {"response": answer, "conversation": previous_conversation}
    except Exception as e:
        logger.exception("ask_graph_agent failed: %s", e)
        # Return a 500 with trace information suppressed for security
        return JSONResponse(status_code=500, content={"error": "Internal server error", "detail": str(e)})

# Health-check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}

# Simple endpoint to list sessions (for debugging)
@app.get("/sessions")
async def list_sessions():
    return {"sessions": list(conversation_history.keys())}
