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
from urllib.parse import parse_qs, unquote

# Настройка подробного логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Настройки
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = 989062605

# Проверка токена
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не найден в переменных окружения!")
else:
    logger.info(f"✅ BOT_TOKEN загружен, длина: {len(BOT_TOKEN)}")
    logger.info(f"✅ BOT_TOKEN первые символы: {BOT_TOKEN[:10]}...")

if not DATABASE_URL:
    logger.error("❌ DATABASE_URL не найден в переменных окружения!")
else:
    logger.info(f"✅ DATABASE_URL загружен")

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
        try:
            self.pool = await asyncpg.create_pool(self.dsn)
            logger.info("✅ API подключен к БД")
            await self.init_db()
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
    
    async def init_db(self):
        """Инициализация таблиц"""
        try:
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
                
                # Проверяем созданные таблицы
                tables = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                logger.info(f"✅ Существующие таблицы: {[t['table_name'] for t in tables]}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("✅ Соединение с БД закрыто")
    
    async def save_message(self, user_id: int, text: str) -> int:
        """Сохранение сообщения"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow('''
                    INSERT INTO messages (user_id, message_text)
                    VALUES ($1, $2)
                    RETURNING id
                ''', user_id, text)
                message_id = result['id']
                logger.info(f"✅ Сообщение сохранено: ID={message_id}, user_id={user_id}")
                return message_id
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сообщения: {e}")
            raise
    
    async def get_user_messages(self, user_id: int) -> List[Dict]:
        """Получение всех сообщений пользователя"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT id, message_text, answer_text, is_answered, 
                           created_at, answered_at
                    FROM messages 
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                ''', user_id)
                messages = [dict(row) for row in rows]
                logger.info(f"✅ Загружено сообщений для user_id={user_id}: {len(messages)}")
                return messages
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сообщений: {e}")
            return []
    
    async def get_unanswered_count(self, user_id: int) -> int:
        """Количество неотвеченных сообщений"""
        try:
            async with self.pool.acquire() as conn:
                count = await conn.fetchval('''
                    SELECT COUNT(*) FROM messages 
                    WHERE user_id = $1 AND is_answered = FALSE
                ''', user_id)
                logger.info(f"✅ Неотвеченных сообщений для user_id={user_id}: {count}")
                return count or 0
        except Exception as e:
            logger.error(f"❌ Ошибка подсчета неотвеченных: {e}")
            return 0

db = Database(DATABASE_URL)

@app.on_event("startup")
async def startup():
    logger.info("🚀 Запуск API сервера...")
    await db.connect()
    logger.info("✅ API сервер запущен")

@app.on_event("shutdown")
async def shutdown():
    logger.info("🛑 Остановка API сервера...")
    await db.close()
    logger.info("✅ API сервер остановлен")

def validate_telegram_data(init_data: str) -> Optional[Dict]:
    """Проверка подписи от Telegram с подробным логированием"""
    try:
        logger.info("=" * 50)
        logger.info("🔍 НАЧАЛО ВАЛИДАЦИИ TELEGRAM DATA")
        logger.info(f"📥 Получен initData (первые 200 символов): {init_data[:200]}")
        
        # Декодируем URL-кодированную строку
        decoded_data = unquote(init_data)
        logger.info(f"📥 Декодированные данные (первые 200 символов): {decoded_data[:200]}")
        
        # Парсим initData
        parsed_data = parse_qs(decoded_data)
        data = {k: v[0] for k, v in parsed_data.items()}
        
        logger.info(f"📊 Ключи в данных: {list(data.keys())}")
        
        # Проверяем наличие user
        if 'user' in data:
            logger.info(f"👤 Данные пользователя: {data['user'][:100]}...")
        
        hash_check = data.pop('hash', '')
        logger.info(f"🔑 Hash из данных: {hash_check[:50]}...")
        
        if not hash_check:
            logger.error("❌ Hash не найден в данных!")
            return None
        
        # Сортируем и создаем строку для проверки
        items = sorted(data.items())
        data_check_string = '\n'.join(f"{k}={v}" for k, v in items)
        logger.info(f"📝 Строка для проверки (первые 200 символов): {data_check_string[:200]}")
        
        # Создаем секретный ключ из токена бота
        logger.info(f"🔐 Используем BOT_TOKEN: {BOT_TOKEN[:10]}...")
        secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
        logger.info(f"🔐 Секретный ключ (hex): {secret_key.hex()[:50]}...")
        
        # Вычисляем HMAC
        h = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        calculated_hash = h.hexdigest()
        
        logger.info(f"✅ Вычисленный hash: {calculated_hash[:50]}...")
        logger.info(f"📥 Полученный hash: {hash_check[:50]}...")
        logger.info(f"🔍 Совпадение: {calculated_hash == hash_check}")
        
        if calculated_hash == hash_check:
            logger.info("✅ Подпись верна!")
            logger.info("=" * 50)
            return data
        else:
            logger.error("❌ Подпись не совпадает!")
            logger.info("=" * 50)
            return None
            
    except Exception as e:
        logger.error(f"❌ Ошибка валидации: {e}", exc_info=True)
        logger.info("=" * 50)
        return None

def get_user_from_init_data(init_data: str) -> Optional[Dict]:
    """Извлечение пользователя из initData"""
    try:
        # Декодируем URL-кодированную строку
        decoded_data = unquote(init_data)
        parsed_data = parse_qs(decoded_data)
        
        user_str = parsed_data.get('user', ['{}'])[0]
        logger.info(f"📦 Строка пользователя: {user_str[:100]}...")
        
        user = json.loads(user_str)
        logger.info(f"👤 Распарсенный пользователь: ID={user.get('id')}, username={user.get('username')}")
        
        return user
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга пользователя: {e}", exc_info=True)
        return None

@app.get("/api/auth")
async def auth_get(request: Request):
    """Аутентификация через GET параметры"""
    try:
        logger.info("=" * 50)
        logger.info("🔐 ЗАПРОС АУТЕНТИФИКАЦИИ GET")
        
        init_data = request.query_params.get('initData')
        logger.info(f"📥 initData в query параметрах: {'есть' if init_data else 'нет'}")
        
        if not init_data:
            logger.error("❌ Нет initData в запросе")
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        logger.info(f"📥 Длина initData: {len(init_data)}")
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            logger.error("❌ Неверная подпись")
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            logger.error("❌ Нет данных пользователя")
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        logger.info(f"👤 ID пользователя: {user_id}")
        
        # Получаем данные пользователя
        unanswered = await db.get_unanswered_count(user_id)
        logger.info(f"📊 Неотвеченных сообщений: {unanswered}")
        
        response = {
            "ok": True,
            "user": {
                "id": user_id,
                "first_name": user.get('first_name', ''),
                "username": user.get('username', ''),
                "unanswered": unanswered
            }
        }
        
        logger.info(f"✅ Ответ: {response}")
        logger.info("=" * 50)
        return JSONResponse(response)
        
    except Exception as e:
        logger.error(f"❌ Ошибка аутентификации: {e}", exc_info=True)
        logger.info("=" * 50)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/auth")
async def auth_post(request: Request):
    """Аутентификация через POST JSON"""
    try:
        logger.info("=" * 50)
        logger.info("🔐 ЗАПРОС АУТЕНТИФИКАЦИИ POST")
        
        body = await request.json()
        init_data = body.get('initData')
        logger.info(f"📥 initData в теле запроса: {'есть' if init_data else 'нет'}")
        
        if not init_data:
            logger.error("❌ Нет initData в запросе")
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        logger.info(f"📥 Длина initData: {len(init_data)}")
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            logger.error("❌ Неверная подпись")
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            logger.error("❌ Нет данных пользователя")
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        logger.info(f"👤 ID пользователя: {user_id}")
        
        # Получаем данные пользователя
        unanswered = await db.get_unanswered_count(user_id)
        logger.info(f"📊 Неотвеченных сообщений: {unanswered}")
        
        response = {
            "ok": True,
            "user": {
                "id": user_id,
                "first_name": user.get('first_name', ''),
                "username": user.get('username', ''),
                "unanswered": unanswered
            }
        }
        
        logger.info(f"✅ Ответ: {response}")
        logger.info("=" * 50)
        return JSONResponse(response)
        
    except Exception as e:
        logger.error(f"❌ Ошибка аутентификации: {e}", exc_info=True)
        logger.info("=" * 50)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/api/messages")
async def get_messages(request: Request):
    """Получение всех сообщений пользователя"""
    try:
        logger.info("=" * 50)
        logger.info("📬 ЗАПРОС СООБЩЕНИЙ")
        
        init_data = request.query_params.get('initData')
        logger.info(f"📥 initData в query параметрах: {'есть' if init_data else 'нет'}")
        
        if not init_data:
            logger.error("❌ Нет initData в запросе")
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        
        logger.info(f"📥 Длина initData: {len(init_data)}")
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            logger.error("❌ Неверная подпись")
            return JSONResponse({"error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            logger.error("❌ Нет данных пользователя")
            return JSONResponse({"error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        logger.info(f"👤 ID пользователя: {user_id}")
        
        # Получаем сообщения
        messages = await db.get_user_messages(user_id)
        
        # Форматируем даты
        for msg in messages:
            if msg.get('created_at'):
                if hasattr(msg['created_at'], 'isoformat'):
                    msg['created_at'] = msg['created_at'].isoformat()
            if msg.get('answered_at'):
                if hasattr(msg['answered_at'], 'isoformat'):
                    msg['answered_at'] = msg['answered_at'].isoformat()
        
        logger.info(f"✅ Отправляем {len(messages)} сообщений")
        logger.info("=" * 50)
        
        return {"messages": messages}
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения сообщений: {e}", exc_info=True)
        logger.info("=" * 50)
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/send")
async def send_message(request: Request):
    """Отправка сообщения"""
    try:
        logger.info("=" * 50)
        logger.info("📤 ОТПРАВКА СООБЩЕНИЯ")
        
        body = await request.json()
        init_data = body.get('initData')
        text = body.get('text', '').strip()
        
        logger.info(f"📥 Длина initData: {len(init_data) if init_data else 0}")
        logger.info(f"📝 Текст сообщения: {text[:50]}... (длина: {len(text)})")
        
        if not text:
            logger.error("❌ Пустое сообщение")
            return JSONResponse({"ok": False, "error": "Empty message"}, status_code=400)
        
        if len(text) > 4096:
            logger.error("❌ Сообщение слишком длинное")
            return JSONResponse({"ok": False, "error": "Message too long"}, status_code=400)
        
        if not init_data:
            logger.error("❌ Нет initData")
            return JSONResponse({"ok": False, "error": "No init data"}, status_code=400)
        
        # Проверяем подпись
        valid_data = validate_telegram_data(init_data)
        if not valid_data:
            logger.error("❌ Неверная подпись")
            return JSONResponse({"ok": False, "error": "Invalid signature"}, status_code=403)
        
        # Получаем пользователя
        user = get_user_from_init_data(init_data)
        if not user:
            logger.error("❌ Нет данных пользователя")
            return JSONResponse({"ok": False, "error": "No user data"}, status_code=400)
        
        user_id = int(user.get('id'))
        logger.info(f"👤 ID пользователя: {user_id}")
        
        # Сохраняем сообщение
        message_id = await db.save_message(user_id, text)
        logger.info(f"✅ Сообщение сохранено с ID: {message_id}")
        
        response = {"ok": True, "message_id": message_id}
        logger.info(f"✅ Ответ: {response}")
        logger.info("=" * 50)
        
        return JSONResponse(response)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки: {e}", exc_info=True)
        logger.info("=" * 50)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/")
async def get_index():
    """Главная страница"""
    return FileResponse("mini_app/index.html")

# Для отладки - проверка доступности API
@app.get("/api/health")
async def health_check():
    """Проверка здоровья API"""
    return {
        "status": "ok",
        "bot_token_loaded": bool(BOT_TOKEN),
        "database_connected": db.pool is not None,
        "timestamp": datetime.now().isoformat()
    }

# Подключаем статические файлы
app.mount("/static", StaticFiles(directory="mini_app"), name="static")
app.mount("/", StaticFiles(directory="mini_app", html=True), name="root")
