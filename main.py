import os
import shutil
import sqlite3
import hashlib
from datetime import datetime
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

DB_FILE = "chat_memories.db"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            username TEXT UNIQUE,
            password_hash TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            created_by TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            sender_username TEXT,
            content TEXT,
            timestamp TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            uploaded_by TEXT,
            caption TEXT,
            file_url TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

os.makedirs("memories_uploads", exist_ok=True)
app = FastAPI(title="PulseChat & Memories API")
app.mount("/memories_uploads", StaticFiles(directory="memories_uploads"), name="memories")

class ConnectionManager:
    def __init__(self):
        self.active_rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, group_name: str):
        await websocket.accept()
        if group_name not in self.active_rooms:
            self.active_rooms[group_name] = []
        self.active_rooms[group_name].append(websocket)

    def disconnect(self, websocket: WebSocket, group_name: str):
        if group_name in self.active_rooms and websocket in self.active_rooms[group_name]:
            self.active_rooms[group_name].remove(websocket)

    async def broadcast(self, message: dict, group_name: str):
        if group_name in self.active_rooms:
            for connection in self.active_rooms[group_name]:
                await connection.send_json(message)

manager = ConnectionManager()

# --- SECURE REGISTER & LOGIN ---
@app.post("/register")
def register_user(email: str = Form(...), username: str = Form(...), password: str = Form(...)):
    clean_username = username.strip()
    if not clean_username.startswith("@"):
        clean_username = f"@{clean_username}"
    
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (email, username, password_hash) VALUES (?, ?, ?)",
            (email.strip().lower(), clean_username.lower(), hash_password(password))
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(status_code=400, detail="Email or @username already taken!")
    conn.close()
    return {"status": "success", "username": clean_username.lower()}

@app.post("/login")
def login_user(username_or_email: str = Form(...), password: str = Form(...)):
    identifier = username_or_email.strip().lower()
    conn = get_db()
    user = conn.execute(
        "SELECT username, password_hash FROM users WHERE email = ? OR username = ? OR username = ?",
        (identifier, identifier, f"@{identifier}")
    ).fetchone()
    conn.close()

    if not user or user["password_hash"] != hash_password(password):
        raise HTTPException(status_code=401, detail="Wrong @username/email or password!")
    
    return {"status": "success", "username": user["username"]}

# --- SEARCH USERS FOR 1-ON-1 CHAT ---
@app.get("/search_users/{query}")
def search_users(query: str):
    q = query.strip().lower()
    conn = get_db()
    rows = conn.execute(
        "SELECT username FROM users WHERE username LIKE ? LIMIT 10",
        (f"%{q}%",)
    ).fetchall()
    conn.close()
    return [row["username"] for row in rows]

@app.post("/groups")
def create_group(name: str = Form(...), created_by: str = Form(...)):
    conn = get_db()
    try:
        conn.execute("INSERT INTO groups (name, created_by) VALUES (?, ?)", (name, created_by))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()
    return {"status": "joined", "group": name}

@app.get("/messages/{group_name}")
def get_chat_history(group_name: str):
    conn = get_db()
    rows = conn.execute(
        "SELECT sender_username, content, timestamp FROM messages WHERE group_name = ? ORDER BY id ASC",
        (group_name,)
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.post("/memories/{group_name}")
async def upload_memory(
    group_name: str,
    uploaded_by: str = Form(...),
    caption: str = Form(...),
    file: UploadFile = File(...)
):
    safe_name = f"{int(datetime.utcnow().timestamp())}_{file.filename}"
    file_path = f"memories_uploads/{safe_name}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    file_url = f"/{file_path}"
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    
    conn = get_db()
    conn.execute(
        "INSERT INTO memories (group_name, uploaded_by, caption, file_url, created_at) VALUES (?, ?, ?, ?, ?)",
        (group_name, uploaded_by, caption, file_url, now)
    )
    conn.commit()
    conn.close()

    await manager.broadcast({
        "type": "new_memory",
        "sender": uploaded_by,
        "caption": caption,
        "file_url": file_url
    }, group_name)

    return {"status": "saved", "file_url": file_url}

@app.get("/memories/{group_name}")
def get_group_memories(group_name: str):
    conn = get_db()
    rows = conn.execute(
        "SELECT uploaded_by, caption, file_url, created_at FROM memories WHERE group_name = ? ORDER BY id DESC",
        (group_name,)
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.websocket("/ws/{group_name}/{username}")
async def websocket_endpoint(websocket: WebSocket, group_name: str, username: str):
    await manager.connect(websocket, group_name)
    try:
        while True:
            data = await websocket.receive_text()
            now = datetime.utcnow().strftime("%H:%M")
            conn = get_db()
            conn.execute(
                "INSERT INTO messages (group_name, sender_username, content, timestamp) VALUES (?, ?, ?, ?)",
                (group_name, username, data, now)
            )
            conn.commit()
            conn.close()

            await manager.broadcast({
                "type": "chat",
                "sender": username,
                "content": data,
                "time": now
            }, group_name)
    except WebSocketDisconnect:
        manager.disconnect(websocket, group_name)

@app.get("/", response_class=HTMLResponse)
def get_ui():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
