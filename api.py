from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import hmac
import hashlib
import json
import os
import asyncpg
from typing import Optional, Dict, List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Настройки
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = 989062605

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем статические файлы
app.mount("/", StaticFiles(directory="mini_app", html=True), name="static")

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
    
    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)
        logger.info("✅ API подключен к БД")
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            return dict(row) if row else None
    
    async def get_unanswered_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE user_id = $1 AND is_answered = FALSE
            ''', user_id)
    
    async def save_message(self, user_id: int, text: str) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                UPDATE message_counter SET last_message_id = last_message_id + 1 WHERE id = 1 RETURNING last_message_id
            ''')
            message_id = result['last_message_id']
            
            await conn.execute('''
                INSERT INTO messages (message_id, user_id, content_type, text)
                VALUES ($1, $2, 'text', $3)
            ''', message_id, user_id, text)
            
            return message_id
    
    async def get_user_inbox(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.message_id, m.answered_at, m.answered_by, m.answer_text,
                       a.first_name as answered_by_name,
                       orig.text as original_text
                FROM messages m
                JOIN messages orig ON m.message_id = orig.message_id
                LEFT JOIN users a ON m.answered_by = a.user_id
                WHERE m.user_id = $1 AND m.is_answered = TRUE
                ORDER BY m.answered_at DESC
            ''', user_id)
            return [dict(row) for row in rows]
    
    async def get_user_sent(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.*, 
                       a.first_name as answered_by_name
                FROM messages m
                LEFT JOIN users a ON m.answered_by = a.user_id
                WHERE m.user_id = $1
                ORDER BY m.forwarded_at DESC
            ''', user_id)
            return [dict(row) for row in rows]
    
    async def is_admin(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return True
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM admins WHERE user_id = $1 AND is_active = TRUE)',
                user_id
            )
            return exists

db = Database(DATABASE_URL)

@app.on_event("startup")
async def startup():
    await db.connect()

@app.on_event("shutdown")
async def shutdown():
    await db.close()

def validate_telegram_data(init_data: str) -> Optional[Dict]:
    try:
        data = {}
        for item in init_data.split('&'):
            if '=' in item:
                key, value = item.split('=', 1)
                data[key] = value
        
        hash_check = data.pop('hash', '')
        data_check_string = '\n'.join(f"{k}={v}" for k, v in sorted(data.items()))
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        
        if h.hexdigest() == hash_check:
            return data
        return None
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return None

@app.get("/")
async def root():
    return JSONResponse({"message": "Mini App Bot API is running"})

# ИСПРАВЛЕНИЕ: Добавлен POST-эндпоинт для /api/auth
@app.post("/api/auth")
async def auth(request: Request):
    try:
        body = await request.json()
        init_data = body.get('initData')
        
        if not init_data:
            return JSONResponse({"error": "No init data"}, status_code=400)
        
        user_data = validate_telegram_data(init_data)
        if not user_data:
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        user = json.loads(user_data.get('user', '{}'))
        user_id = int(user.get('id'))
        
        is_admin = await db.is_admin(user_id)
        unanswered = await db.get_unanswered_count(user_id) if not is_admin else 0
        
        return {
            "ok": True,
            "user": {
                "id": user_id,
                "is_admin": is_admin,
                "first_name": user.get('first_name'),
                "username": user.get('username'),
                "unanswered": unanswered
            }
        }
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/messages/inbox")
async def get_inbox(request: Request):
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    user_id = int(user.get('id'))
    
    messages = await db.get_user_inbox(user_id)
    
    for msg in messages:
        if msg.get('answered_at'):
            msg['answered_at'] = msg['answered_at'].isoformat()
    
    return {"messages": messages}

@app.get("/api/messages/sent")
async def get_sent(request: Request):
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    user_id = int(user.get('id'))
    
    messages = await db.get_user_sent(user_id)
    
    for msg in messages:
        if msg.get('forwarded_at'):
            msg['forwarded_at'] = msg['forwarded_at'].isoformat()
        if msg.get('answered_at'):
            msg['answered_at'] = msg['answered_at'].isoformat()
    
    return {"messages": messages}

@app.post("/api/send")
async def send_message(request: Request):
    try:
        body = await request.json()
        init_data = body.get('initData')
        text = body.get('text', '').strip()
        
        if not text:
            return JSONResponse({"error": "Empty message"}, status_code=400)
        
        if len(text) > 4096:
            return JSONResponse({"error": "Message too long"}, status_code=400)
        
        user_data = validate_telegram_data(init_data)
        if not user_data:
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        user = json.loads(user_data.get('user', '{}'))
        user_id = int(user.get('id'))
        
        message_id = await db.save_message(user_id, text)
        
        return {"ok": True, "message_id": message_id}
    except Exception as e:
        logger.error(f"Send error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
