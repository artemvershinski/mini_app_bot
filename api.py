from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
import hmac
import hashlib
import json
import os
import asyncpg
from typing import Optional, Dict, List
from datetime import datetime
import logging
from urllib.parse import parse_qs

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

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
    
    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)
        logger.info("✅ API подключен к БД")
        await self.init_db()
    
    async def init_db(self):
        """Инициализация таблиц"""
        async with self.pool.acquire() as conn:
            # Таблица сообщений (упрощенная)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    message_text TEXT NOT NULL,
                    answer_text TEXT,
                    is_answered BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    answered_at TIMESTAMP
                )
            ''')
            
            # Создаем индексы для быстрого поиска
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)
            ''')
            
            logger.info("✅ Таблицы созданы/проверены")
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    async def save_message(self, user_id: int, text: str) -> int:
        """Сохранение сообщения"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                INSERT INTO messages (user_id, message_text)
                VALUES ($1, $2)
                RETURNING id
            ''', user_id, text)
            return result['id']
    
    async def get_user_messages(self, user_id: int) -> List[Dict]:
        """Получение всех сообщений пользователя"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, message_text, answer_text, is_answered, 
                       created_at, answered_at
                FROM messages 
                WHERE user_id = $1
                ORDER BY created_at DESC
            ''', user_id)
            return [dict(row) for row in rows]
    
    async def get_unanswered_count(self, user_id: int) -> int:
        """Количество неотвеченных сообщений"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE user_id = $1 AND is_answered = FALSE
            ''', user_id)

db = Database(DATABASE_URL)

@app.on_event("startup")
async def startup():
    await db.connect()

@app.on_event("shutdown")
async def shutdown():
    await db.close()

def validate_telegram_data(init_data: str) -> Optional[Dict]:
    """Проверка подписи от Telegram"""
    try:
        # Парсим initData
        parsed_data = parse_qs(init_data)
        data = {k: v[0] for k, v in parsed_data.items()}
        
        hash_check = data.pop('hash', '')
        
        # Сортируем и создаем строку для проверки
        data_check_string = '\n'.join(f"{k}={v}" for k, v in sorted(data.items()))
        
        # Создаем секретный ключ из токена бота
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
        
        # Вычисляем HMAC
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        
        if h.hexdigest() == hash_check:
            return data
        return None
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return None

def get_user_from_init_data(init_data: str) -> Optional[Dict]:
    """Извлечение пользователя из initData"""
    try:
        parsed_data = parse_qs(init_data)
        user_str = parsed_data.get('user', ['{}'])[0]
        user = json.loads(user_str)
        return user
    except Exception as e:
        logger.error(f"Error parsing user: {e}")
        return None

@app.get("/api/auth")
async def auth_get(request: Request):
    """Аутентификация через GET параметры"""
    try:
        init_data = request.query_params.get('initData')
        logger.info(f"Auth GET request, initData present: {bool(init_data)}")
        
        if not init_data:
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        
        # Получаем данные пользователя
        unanswered = await db.get_unanswered_count(user_id)
        
        return {
            "ok": True,
            "user": {
                "id": user_id,
                "first_name": user.get('first_name', ''),
                "username": user.get('username', ''),
                "unanswered": unanswered
            }
        }
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/auth")
async def auth_post(request: Request):
    """Аутентификация через POST JSON"""
    try:
        body = await request.json()
        init_data = body.get('initData')
        logger.info(f"Auth POST request, initData present: {bool(init_data)}")
        
        if not init_data:
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        
        # Получаем данные пользователя
        unanswered = await db.get_unanswered_count(user_id)
        
        return {
            "ok": True,
            "user": {
                "id": user_id,
                "first_name": user.get('first_name', ''),
                "username": user.get('username', ''),
                "unanswered": unanswered
            }
        }
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/api/messages")
async def get_messages(request: Request):
    """Получение всех сообщений пользователя"""
    try:
        init_data = request.query_params.get('initData')
        logger.info(f"Get messages request, initData present: {bool(init_data)}")
        
        if not init_data:
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            return JSONResponse({"error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        
        # Получаем сообщения
        messages = await db.get_user_messages(user_id)
        
        # Форматируем даты
        for msg in messages:
            if msg.get('created_at'):
                msg['created_at'] = msg['created_at'].isoformat()
            if msg.get('answered_at'):
                msg['answered_at'] = msg['answered_at'].isoformat()
        
        return {"messages": messages}
    except Exception as e:
        logger.error(f"Get messages error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/send")
async def send_message(request: Request):
    """Отправка сообщения"""
    try:
        body = await request.json()
        init_data = body.get('initData')
        text = body.get('text', '').strip()
        
        logger.info(f"Send message request, text length: {len(text)}")
        
        if not text:
            return JSONResponse({"ok": False, "error": "Empty message"}, status_code=400)
        
        if len(text) > 4096:
            return JSONResponse({"ok": False, "error": "Message too long"}, status_code=400)
        
        if not init_data:
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        
        # Сохраняем сообщение
        message_id = await db.save_message(user_id, text)
        logger.info(f"Message saved with ID: {message_id}")
        
        return {"ok": True, "message_id": message_id}
    except Exception as e:
        logger.error(f"Send error: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# Для отдачи index.html
@app.get("/")
async def get_index():
    return FileResponse("mini_app/index.html")

# Подключаем статические файлы (CSS, JS)
app.mount("/", StaticFiles(directory="mini_app", html=True), name="static")
