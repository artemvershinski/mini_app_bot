from fastapi import FastAPI, Request, HTTPException
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

# CORS для Telegram Mini App
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене можно заменить на конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем статические файлы Mini App
app.mount("/", StaticFiles(directory="mini_app", html=True), name="static")

# Подключение к БД
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
    
    async def get_user_messages(self, user_id: int, limit: int = 50) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.*, 
                       CASE WHEN m.is_answered THEN a.first_name ELSE NULL END as answered_by_name
                FROM messages m
                LEFT JOIN users a ON m.answered_by = a.user_id
                WHERE m.user_id = $1
                ORDER BY m.forwarded_at DESC
                LIMIT $2
            ''', user_id, limit)
            return [dict(row) for row in rows]
    
    async def get_unanswered_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE user_id = $1 AND is_answered = FALSE
            ''', user_id)
    
    async def save_message(self, user_id: int, text: str) -> int:
        """Сохранение сообщения из Mini App"""
        async with self.pool.acquire() as conn:
            # Получаем следующий ID
            result = await conn.fetchrow('''
                UPDATE message_counter SET last_message_id = last_message_id + 1 WHERE id = 1 RETURNING last_message_id
            ''')
            message_id = result['last_message_id']
            
            # Сохраняем сообщение
            await conn.execute('''
                INSERT INTO messages (message_id, user_id, content_type, text)
                VALUES ($1, $2, 'text', $3)
            ''', message_id, user_id, text)
            
            return message_id
    
    async def get_message(self, message_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_id = $1', message_id)
            return dict(row) if row else None
    
    async def mark_message_answered(self, message_id: int, answered_by: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE messages 
                SET is_answered = TRUE, answered_by = $2, answered_at = CURRENT_TIMESTAMP
                WHERE message_id = $1
            ''', message_id, answered_by)
    
    async def is_admin(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return True
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM admins WHERE user_id = $1 AND is_active = TRUE)',
                user_id
            )
            return exists
    
    async def get_all_messages(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Получение всех сообщений для админа"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.*, u.username, u.first_name, u.last_name, u.is_banned,
                       a.first_name as answered_by_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                LEFT JOIN users a ON m.answered_by = a.user_id
                ORDER BY m.forwarded_at DESC
                LIMIT $1 OFFSET $2
            ''', limit, offset)
            return [dict(row) for row in rows]
    
    async def get_users_list(self) -> List[Dict]:
        """Получение списка пользователей для админа"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT u.*, 
                       COUNT(m.message_id) as messages_count,
                       COUNT(CASE WHEN m.is_answered = FALSE THEN 1 END) as unanswered_count
                FROM users u
                LEFT JOIN messages m ON u.user_id = m.user_id
                GROUP BY u.user_id
                ORDER BY u.created_at DESC
            ''')
            return [dict(row) for row in rows]
    
    async def get_stats(self) -> Dict:
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow('SELECT * FROM stats WHERE id = 1')
            
            # Дополнительная статистика
            today = await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE forwarded_at > CURRENT_DATE
            ''')
            
            active_users = await conn.fetchval('''
                SELECT COUNT(DISTINCT user_id) FROM messages 
                WHERE forwarded_at > CURRENT_DATE
            ''')
            
            result = dict(stats) if stats else {}
            result['messages_today'] = today
            result['active_users_today'] = active_users
            return result

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
        # Парсим данные из строки
        data = {}
        for item in init_data.split('&'):
            if '=' in item:
                key, value = item.split('=', 1)
                data[key] = value
        
        hash_check = data.pop('hash', '')
        
        # Создаем строку для проверки
        data_check_string = '\n'.join(f"{k}={v}" for k, v in sorted(data.items()))
        
        # Создаем секретный ключ из токена бота
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
        
        # Вычисляем HMAC
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        
        # Сравниваем с полученным hash
        if h.hexdigest() == hash_check:
            return data
        return None
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return None

@app.post("/api/auth")
async def auth(request: Request):
    """Аутентификация пользователя"""
    try:
        body = await request.json()
        init_data = body.get('initData')
        
        logger.info(f"Auth request received")
        
        if not init_data:
            logger.error("No init data provided")
            return JSONResponse({"error": "No init data"}, status_code=400)
        
        user_data = validate_telegram_data(init_data)
        if not user_data:
            logger.error("Invalid signature")
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        # Парсим данные пользователя
        user = json.loads(user_data.get('user', '{}'))
        user_id = int(user.get('id'))
        
        logger.info(f"User authenticated: {user_id}")
        
        # Получаем информацию о пользователе из БД
        db_user = await db.get_user(user_id)
        is_admin = await db.is_admin(user_id)
        unanswered = await db.get_unanswered_count(user_id) if not is_admin else 0
        
        return {
            "ok": True,
            "user": {
                "id": user_id,
                "is_admin": is_admin,
                "first_name": user.get('first_name'),
                "last_name": user.get('last_name'),
                "username": user.get('username'),
                "unanswered": unanswered
            }
        }
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/messages")
async def get_messages(request: Request):
    """Получение сообщений пользователя"""
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        logger.error("No init data in headers")
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        logger.error("Invalid signature in messages request")
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    user_id = int(user.get('id'))
    
    messages = await db.get_user_messages(user_id)
    
    # Форматируем даты
    for msg in messages:
        if msg.get('forwarded_at'):
            msg['forwarded_at'] = msg['forwarded_at'].isoformat()
        if msg.get('answered_at'):
            msg['answered_at'] = msg['answered_at'].isoformat()
    
    return {"messages": messages}

@app.post("/api/send")
async def send_message(request: Request):
    """Отправка сообщения из Mini App"""
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
            logger.error("Invalid signature in send request")
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        user = json.loads(user_data.get('user', '{}'))
        user_id = int(user.get('id'))
        
        # Проверяем бан
        db_user = await db.get_user(user_id)
        if db_user and db_user.get('is_banned'):
            ban_until = db_user.get('ban_until')
            if ban_until and datetime.now() > ban_until:
                await db.unban_user(user_id)
            else:
                return JSONResponse({"error": "User is banned"}, status_code=403)
        
        # Сохраняем сообщение
        message_id = await db.save_message(user_id, text)
        
        return {
            "ok": True,
            "message_id": message_id
        }
    except Exception as e:
        logger.error(f"Send error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# Admin endpoints
@app.get("/api/admin/messages")
async def admin_get_messages(request: Request, limit: int = 100, offset: int = 0):
    """Получение всех сообщений для админа"""
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    admin_id = int(user.get('id'))
    
    if not await db.is_admin(admin_id):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    
    messages = await db.get_all_messages(limit, offset)
    
    # Форматируем даты
    for msg in messages:
        if msg.get('forwarded_at'):
            msg['forwarded_at'] = msg['forwarded_at'].isoformat()
        if msg.get('answered_at'):
            msg['answered_at'] = msg['answered_at'].isoformat()
    
    return {"messages": messages}

@app.get("/api/admin/users")
async def admin_get_users(request: Request):
    """Получение списка пользователей для админа"""
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    admin_id = int(user.get('id'))
    
    if not await db.is_admin(admin_id):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    
    users = await db.get_users_list()
    
    # Форматируем даты
    for u in users:
        if u.get('created_at'):
            u['created_at'] = u['created_at'].isoformat()
        if u.get('last_message_time'):
            u['last_message_time'] = u['last_message_time'].isoformat()
    
    return {"users": users}

@app.get("/api/admin/stats")
async def admin_get_stats(request: Request):
    """Получение статистики для админа"""
    init_data = request.headers.get('X-Telegram-Init-Data')
    if not init_data:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    
    user_data = validate_telegram_data(init_data)
    if not user_data:
        return JSONResponse({"error": "Invalid signature"}, status_code=403)
    
    user = json.loads(user_data.get('user', '{}'))
    admin_id = int(user.get('id'))
    
    if not await db.is_admin(admin_id):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    
    stats = await db.get_stats()
    return stats

@app.post("/api/admin/reply")
async def admin_reply(request: Request):
    """Ответ на сообщение от админа"""
    try:
        body = await request.json()
        init_data = body.get('initData')
        message_id = body.get('message_id')
        answer = body.get('answer', '').strip()
        
        if not message_id or not answer:
            return JSONResponse({"error": "Missing data"}, status_code=400)
        
        user_data = validate_telegram_data(init_data)
        if not user_data:
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        user = json.loads(user_data.get('user', '{}'))
        admin_id = int(user.get('id'))
        
        if not await db.is_admin(admin_id):
            return JSONResponse({"error": "Forbidden"}, status_code=403)
        
        # Получаем сообщение
        message = await db.get_message(message_id)
        if not message:
            return JSONResponse({"error": "Message not found"}, status_code=404)
        
        if message['is_answered']:
            return JSONResponse({"error": "Message already answered"}, status_code=400)
        
        # Отмечаем как отвеченное
        await db.mark_message_answered(message_id, admin_id)
        
        return {"ok": True, "message_id": message_id}
    except Exception as e:
        logger.error(f"Reply error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
