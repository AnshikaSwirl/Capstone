# frontend/app.py
import os
import json
import requests
import streamlit as st
from typing import Dict

# --- API base resolution (robust to missing Streamlit secrets) ---
try:
    API_BASE = st.secrets["API_BASE"]
except Exception:
    API_BASE = os.environ.get("API_BASE", "http://127.0.0.1:8000")
API_BASE = API_BASE.rstrip("/")

# --- Helpers ---
def ask_graph_agent_api(user_query: str, session_id: str) -> Dict:
    url = f"{API_BASE}/ask_graph_agent"
    try:
        resp = requests.post(url, json={"user_query": user_query, "session_id": session_id}, timeout=200)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        return {"error": str(e)}

def upload_new_table_api(file, table_name: str) -> Dict:
    url = f"{API_BASE}/upload_new_table"
    files = {"file": (file.name, file.getvalue(), file.type or "application/octet-stream")}
    data = {"table_name": table_name}
    try:
        resp = requests.post(url, files=files, data=data, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        return {"status": "error", "message": f"Upload failed: {e}"}

def server_status() -> bool:
    try:
        resp = requests.get(f"{API_BASE}/health", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False

# --- Page config ---
st.set_page_config(page_title="Conversational SQL Agent", page_icon="💬", layout="wide")

# --- Sidebar ---
with st.sidebar:
    st.markdown("### Backend")
    ok = server_status()
    if ok:
        st.success(f"Backend reachable: {API_BASE}")
    else:
        st.error("Backend not reachable")

    st.markdown("---")
    st.markdown("### Session")
    if "sessions" not in st.session_state:
        st.session_state.sessions = ["default"]
    if "current_session" not in st.session_state:
        st.session_state.current_session = "default"

    selected = st.selectbox(
        "Choose a session",
        st.session_state.sessions,
        index=st.session_state.sessions.index(st.session_state.current_session),
    )
    if selected != st.session_state.current_session:
        st.session_state.current_session = selected

    new_session_name = st.text_input("Create a new session", value="", placeholder="e.g., analysis-team")
    cols = st.columns(2)
    with cols[0]:
        if st.button("Create"):
            name = new_session_name.strip()
            if name and name not in st.session_state.sessions:
                st.session_state.sessions.append(name)
                st.session_state.current_session = name
                st.success(f"Session '{name}' created")
            elif not name:
                st.warning("Please enter a session name.")
            else:
                st.info("Session already exists.")
    with cols[1]:
        if st.button("Reset conversation"):
            key = f"chat_{st.session_state.current_session}"
            st.session_state.pop(key, None)
            st.success("Conversation reset for this session")

    st.markdown("---")
    st.markdown("### Upload new table")
    upload_table_name = st.text_input("Table name", placeholder="e.g., cancer_encounters_v2")
    uploaded_file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
    if st.button("Upload"):
        if not uploaded_file or not upload_table_name.strip():
            st.warning("Please provide both a file and a table name.")
        else:
            result = upload_new_table_api(uploaded_file, upload_table_name.strip())
            if result.get("status") == "success" or (result.get("status") is None and result.get("message")):
                st.success(result.get("message", "Upload succeeded"))
            else:
                st.error(result.get("message", "Upload failed"))

# --- Main UI ---
st.title("Ask your Queries")

session_key = f"chat_{st.session_state.current_session}"
if session_key not in st.session_state:
    st.session_state[session_key] = []  # list of {"role": "user"/"assistant", "text": str}

# Display messages
chat_container = st.container()
with chat_container:
    if st.session_state[session_key]:
        for msg in st.session_state[session_key]:
            if msg["role"] == "user":
                with st.chat_message("user"):
                    st.write(msg["text"])
            else:
                with st.chat_message("assistant"):
                    st.write(msg["text"])
    else:
        st.info("Start by asking a question about your data!")

# Input
user_query = st.chat_input("Type your question and press Enter")
if user_query:
    st.session_state[session_key].append({"role": "user", "text": user_query})
    with st.chat_message("user"):
        st.write(user_query)

    result = ask_graph_agent_api(user_query=user_query, session_id=st.session_state.current_session)

    if "error" in result:
        bot_text = f"Error: {result['error']}"
        st.session_state[session_key].append({"role": "assistant", "text": bot_text})
        with st.chat_message("assistant"):
            st.error(bot_text)
    else:
        bot_text = result.get("response", result.get("answer", "No response generated."))
        st.session_state[session_key].append({"role": "assistant", "text": bot_text})
        with st.chat_message("assistant"):
            st.write(bot_text)

# Debug expander
with st.expander("Last backend payload (debug)"):
    try:
        if 'result' in locals():
            st.code(json.dumps(result, indent=2), language="json")
        else:
            st.write("No backend payload yet.")
    except Exception:
        st.write("No payload to display.")
