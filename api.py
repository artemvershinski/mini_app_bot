from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
import json
import os
import asyncpg
from datetime import datetime
import logging
from urllib.parse import parse_qs, unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS - разрешаем всё
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем статические файлы
app.mount("/static", StaticFiles(directory="mini_app"), name="static")

DATABASE_URL = os.getenv("DATABASE_URL")

class Database:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None
    
    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)
        logger.info("✅ База данных подключена")
        await self.init_db()
    
    async def init_db(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    user_name TEXT,
                    message_text TEXT NOT NULL,
                    answer_text TEXT,
                    is_answered BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    answered_at TIMESTAMP
                )
            ''')
            logger.info("✅ Таблица messages создана/проверена")
    
    async def save_message(self, user_id, user_name, text):
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                INSERT INTO messages (user_id, user_name, message_text)
                VALUES ($1, $2, $3)
                RETURNING id
            ''', user_id, user_name, text)
            return result['id']
    
    async def get_user_messages(self, user_id):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, message_text, answer_text, is_answered, created_at, answered_at
                FROM messages 
                WHERE user_id = $1
                ORDER BY created_at DESC
            ''', user_id)
            return [dict(row) for row in rows]
    
    async def get_unanswered_count(self, user_id):
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE user_id = $1 AND is_answered = FALSE
            ''', user_id) or 0

db = Database(DATABASE_URL)

@app.on_event("startup")
async def startup():
    await db.connect()

@app.on_event("shutdown")
async def shutdown():
    await db.pool.close()

@app.get("/api/auth")
async def auth(request: Request):
    """Аутентификация - работает всегда и со всем"""
    try:
        # Пробуем получить initData из query параметров
        init_data = request.query_params.get('initData')
        
        # Если нет в query, пробуем из заголовков
        if not init_data:
            init_data = request.headers.get('X-Telegram-Init-Data')
        
        logger.info(f"Auth request, init_data exists: {bool(init_data)}")
        
        if not init_data:
            return JSONResponse(
                content={"ok": False, "error": "No init data"},
                status_code=400
            )
        
        # Парсим пользователя
        try:
            decoded = unquote(init_data)
            parsed = parse_qs(decoded)
            user_str = parsed.get('user', ['{}'])[0]
            user = json.loads(user_str)
            user_id = int(user.get('id', 0))
            user_name = user.get('first_name', '') or user.get('username', 'User')
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return JSONResponse(
                content={"ok": False, "error": "Failed to parse user data"},
                status_code=400
            )
        
        # Получаем статистику
        unanswered = await db.get_unanswered_count(user_id)
        
        return JSONResponse(content={
            "ok": True,
            "user": {
                "id": user_id,
                "first_name": user_name,
                "username": user.get('username', ''),
                "unanswered": unanswered
            }
        })
        
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse(
            content={"ok": False, "error": str(e)},
            status_code=500
        )

@app.post("/api/auth")
async def auth_post(request: Request):
    """Аутентификация через POST"""
    return await auth(request)

@app.get("/api/messages")
async def get_messages(request: Request):
    """Получение сообщений пользователя"""
    try:
        init_data = request.query_params.get('initData')
        
        if not init_data:
            return JSONResponse(
                content={"error": "No init data"},
                status_code=401
            )
        
        # Парсим пользователя
        decoded = unquote(init_data)
        parsed = parse_qs(decoded)
        user_str = parsed.get('user', ['{}'])[0]
        user = json.loads(user_str)
        user_id = int(user.get('id', 0))
        
        if not user_id:
            return JSONResponse(
                content={"error": "Invalid user"},
                status_code=400
            )
        
        # Получаем сообщения
        messages = await db.get_user_messages(user_id)
        
        # Конвертируем datetime в строки
        for msg in messages:
            if msg['created_at']:
                msg['created_at'] = msg['created_at'].isoformat()
            if msg['answered_at']:
                msg['answered_at'] = msg['answered_at'].isoformat()
        
        return JSONResponse(content={"messages": messages})
        
    except Exception as e:
        logger.error(f"Messages error: {e}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )

@app.post("/api/send")
async def send_message(request: Request):
    """Отправка сообщения"""
    try:
        data = await request.json()
        init_data = data.get('initData')
        text = data.get('text', '').strip()
        
        if not text:
            return JSONResponse(
                content={"ok": False, "error": "Empty message"},
                status_code=400
            )
        
        if not init_data:
            return JSONResponse(
                content={"ok": False, "error": "No init data"},
                status_code=400
            )
        
        # Парсим пользователя
        decoded = unquote(init_data)
        parsed = parse_qs(decoded)
        user_str = parsed.get('user', ['{}'])[0]
        user = json.loads(user_str)
        user_id = int(user.get('id', 0))
        user_name = user.get('first_name', '') or user.get('username', 'User')
        
        if not user_id:
            return JSONResponse(
                content={"ok": False, "error": "Invalid user"},
                status_code=400
            )
        
        # Сохраняем сообщение
        message_id = await db.save_message(user_id, user_name, text)
        
        return JSONResponse(content={
            "ok": True,
            "message_id": message_id
        })
        
    except Exception as e:
        logger.error(f"Send error: {e}")
        return JSONResponse(
            content={"ok": False, "error": str(e)},
            status_code=500
        )

@app.get("/")
async def root():
    """Главная страница"""
    return FileResponse("mini_app/index.html")

@app.get("/{full_path:path}")
async def catch_all(full_path: str):
    """Для всех остальных путей отдаем index.html"""
    try:
        return FileResponse("mini_app/index.html")
    except:
        return JSONResponse(
            content={"error": "Not found"},
            status_code=404
        )
