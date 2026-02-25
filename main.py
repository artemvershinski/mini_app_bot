import asyncio, logging, os, sys, signal, asyncpg
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, Update
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
import re
import json
import traceback
import urllib.parse
import hmac
import hashlib

# Configuration
OWNER_ID = 989062605
RATE_LIMIT_MINUTES = 10
MAX_BAN_HOURS = 720
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN")
APP_URL = os.getenv("APP_URL", "https://mini-app-bot-lzya.onrender.com")
PORT = int(os.getenv("PORT", 10000))
MESSAGE_ID_START = 100569

# Настройка логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Флаг для детального логирования (по умолчанию включен)
DEBUG_MODE = True

def log_user_action(action: str, user_id: int, user_data: dict = None, extra: str = ""):
    """Детальное логирование действий пользователя"""
    if not DEBUG_MODE:
        return
    
    username = user_data.get('username', 'NoUsername') if user_data else 'Unknown'
    first_name = user_data.get('first_name', 'NoName') if user_data else 'Unknown'
    
    log_msg = f"👤 USER ACTION [{action}] | ID: {user_id} | @{username} | {first_name}"
    if extra:
        log_msg += f" | {extra}"
    
    logger.info(log_msg)

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self.admin_cache = []
        self.admin_cache_time = 0
    
    async def create_pool(self):
        """Create database connection pool"""
        logger.info("🔄 Connecting to PostgreSQL...")
        self.pool = await asyncpg.create_pool(self.dsn, min_size=10, max_size=20)
        await self.init_db()
        logger.info("✅ PostgreSQL connection established")
    
    async def init_db(self):
        """Initialize database tables with indexes"""
        logger.info("🔄 Initializing database tables...")
        async with self.pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    last_message_time TIMESTAMP,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_until TIMESTAMP,
                    ban_reason TEXT,
                    messages_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица сообщений с индексами
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    message_id INTEGER PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    content_type TEXT NOT NULL,
                    file_id TEXT,
                    caption TEXT,
                    text TEXT,
                    forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_answered BOOLEAN DEFAULT FALSE,
                    answered_by BIGINT,
                    answered_at TIMESTAMP,
                    answer_text TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Индексы для ускорения запросов
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_is_answered ON messages(is_answered)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_forwarded_at ON messages(forwarded_at)')
            
            # Таблица администраторов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY,
                    added_by BIGINT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Таблица статистики
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    total_messages INTEGER DEFAULT 0,
                    successful_forwards INTEGER DEFAULT 0,
                    failed_forwards INTEGER DEFAULT 0,
                    bans_issued INTEGER DEFAULT 0,
                    rate_limit_blocks INTEGER DEFAULT 0,
                    answers_sent INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица для хранения последнего ID сообщения
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_counter (
                    id INTEGER PRIMARY KEY,
                    last_message_id INTEGER NOT NULL
                )
            ''')
            
            await conn.execute('''
                INSERT INTO message_counter (id, last_message_id) 
                VALUES (1, $1) 
                ON CONFLICT (id) DO NOTHING
            ''', MESSAGE_ID_START)
            
            # Создаем пользователя-владельца
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name) 
                VALUES ($1, 'owner', 'Owner')
                ON CONFLICT (user_id) DO UPDATE SET
                    username = 'owner',
                    first_name = 'Owner'
            ''', OWNER_ID)
            
            # Добавляем владельца как администратора
            await conn.execute('''
                INSERT INTO admins (user_id, added_by) 
                VALUES ($1, $1) 
                ON CONFLICT (user_id) DO NOTHING
            ''', OWNER_ID)
            
            # Инициализируем статистику
            await conn.execute('''
                INSERT INTO stats (id, total_messages, successful_forwards, failed_forwards, bans_issued, rate_limit_blocks, answers_sent)
                VALUES (1, 0, 0, 0, 0, 0, 0)
                ON CONFLICT (id) DO NOTHING
            ''')
        
        logger.info("✅ Database tables initialized with indexes")
    
    async def get_next_message_id(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                UPDATE message_counter 
                SET last_message_id = last_message_id + 1 
                WHERE id = 1 
                RETURNING last_message_id
            ''')
            message_id = result['last_message_id'] if result else MESSAGE_ID_START
            logger.debug(f"📝 Generated new message ID: {message_id}")
            return message_id
    
    async def save_message(self, message_id: int, user_id: int, content_type: str, 
                          file_id: str = None, caption: str = None, text: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (message_id, user_id, content_type, file_id, caption, text)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', message_id, user_id, content_type, file_id, caption, text)
            logger.debug(f"💾 Message #{message_id} saved for user {user_id}")
    
    async def get_message(self, message_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_id = $1', message_id)
            return dict(row) if row else None
    
    async def mark_message_answered(self, message_id: int, answered_by: int, answer_text: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE messages 
                SET is_answered = TRUE, 
                    answered_by = $2, 
                    answered_at = CURRENT_TIMESTAMP,
                    answer_text = $3
                WHERE message_id = $1
            ''', message_id, answered_by, answer_text)
            logger.info(f"✅ Message #{message_id} marked as answered by admin {answered_by}")
    
    async def get_user_inbox(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.message_id, m.answered_at, m.answered_by, m.answer_text,
                       u.first_name as answered_by_name, orig.text as original_text
                FROM messages m
                JOIN messages orig ON m.message_id = orig.message_id
                LEFT JOIN users u ON m.answered_by = u.user_id
                WHERE m.user_id = $1 AND m.is_answered = TRUE
                ORDER BY m.answered_at DESC
            ''', user_id)
            logger.debug(f"📥 Loaded {len(rows)} inbox messages for user {user_id}")
            return [dict(row) for row in rows]
    
    async def get_user_sent(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.*, 
                       u.first_name as answered_by_name
                FROM messages m
                LEFT JOIN users u ON m.answered_by = u.user_id
                WHERE m.user_id = $1
                ORDER BY m.forwarded_at DESC
            ''', user_id)
            logger.debug(f"📤 Loaded {len(rows)} sent messages for user {user_id}")
            return [dict(row) for row in rows]
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            return dict(row) if row else None
    
    async def get_unanswered_count(self, user_id: int) -> int:
        """Get count of unanswered messages for user"""
        async with self.pool.acquire() as conn:
            count = await conn.fetchval('''
                SELECT COUNT(*) FROM messages 
                WHERE user_id = $1 AND is_answered = FALSE
            ''', user_id)
            return count if count else 0
    
    async def save_user(self, user_id: int, **kwargs):
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)', user_id)
            
            if exists:
                set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys())])
                set_clause += ", updated_at = CURRENT_TIMESTAMP"
                query = f'UPDATE users SET {set_clause} WHERE user_id = $1'
                await conn.execute(query, user_id, *kwargs.values())
                logger.debug(f"🔄 Updated user {user_id} in database")
            else:
                fields = ['user_id'] + list(kwargs.keys())
                values = [user_id] + list(kwargs.values())
                placeholders = ', '.join([f'${i+1}' for i in range(len(values))])
                query = f'INSERT INTO users ({", ".join(fields)}) VALUES ({placeholders})'
                await conn.execute(query, *values)
                logger.info(f"➕ New user {user_id} saved to database")
    
    async def update_user_stats(self, user_id: int, increment_messages: bool = True):
        async with self.pool.acquire() as conn:
            if increment_messages:
                await conn.execute('''
                    UPDATE users SET messages_sent = messages_sent + 1, 
                                     updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
                logger.debug(f"📊 Incremented message count for user {user_id}")
            else:
                await conn.execute('''
                    UPDATE users SET updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
    
    async def update_user_last_message(self, user_id: int, message_time: datetime):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET last_message_time = $1, updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $2
            ''', message_time, user_id)
            logger.debug(f"⏱ Updated last message time for user {user_id}")
    
    async def ban_user(self, user_id: int, reason: str, ban_until: Optional[datetime] = None):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = TRUE, 
                                 ban_reason = $1, 
                                 ban_until = $2,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $3
            ''', reason, ban_until, user_id)
            logger.warning(f"🔨 User {user_id} banned. Reason: {reason}")
            # Сбрасываем кэш админов при бане
            self.admin_cache = []
    
    async def unban_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = FALSE, 
                                 ban_reason = NULL, 
                                 ban_until = NULL,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $1
            ''', user_id)
            logger.info(f"✅ User {user_id} unbanned")
    
    async def get_all_users(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY created_at DESC')
            return [dict(row) for row in rows]
    
    async def add_admin(self, user_id: int, added_by: int) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO admins (user_id, added_by) 
                    VALUES ($1, $2) 
                    ON CONFLICT (user_id) DO UPDATE SET 
                        is_active = TRUE,
                        added_by = EXCLUDED.added_by,
                        added_at = CURRENT_TIMESTAMP
                ''', user_id, added_by)
                logger.info(f"👑 User {user_id} added as admin by {added_by}")
                # Сбрасываем кэш админов
                self.admin_cache = []
                return True
        except Exception as e:
            logger.error(f"Error adding admin {user_id}: {e}")
            return False
    
    async def remove_admin(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            logger.warning(f"Attempted to remove owner {user_id} from admins")
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
                logger.info(f"👑 User {user_id} removed from admins")
                # Сбрасываем кэш админов
                self.admin_cache = []
                return True
        except Exception as e:
            logger.error(f"Error removing admin {user_id}: {e}")
            return False
    
    async def get_admins(self) -> List[int]:
        # Кэшируем список админов на 5 минут
        if self.admin_cache and (datetime.now().timestamp() - self.admin_cache_time) < 300:
            return self.admin_cache
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM admins WHERE is_active = TRUE')
            self.admin_cache = [row['user_id'] for row in rows]
            self.admin_cache_time = datetime.now().timestamp()
            return self.admin_cache
    
    async def is_admin(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return True
        admins = await self.get_admins()
        return user_id in admins
    
    async def update_stats(self, **kwargs):
        async with self.pool.acquire() as conn:
            set_clause = ', '.join([f"{k} = {k} + ${i+1}" for i, k in enumerate(kwargs.keys())])
            set_clause += ", updated_at = CURRENT_TIMESTAMP"
            query = f'UPDATE stats SET {set_clause} WHERE id = 1'
            await conn.execute(query, *kwargs.values())
            logger.debug(f"📈 Stats updated: {kwargs}")
    
    async def get_stats(self) -> Dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE id = 1')
            if not row:
                return {
                    'total_messages': 0,
                    'successful_forwards': 0,
                    'failed_forwards': 0,
                    'bans_issued': 0,
                    'rate_limit_blocks': 0,
                    'answers_sent': 0
                }
            return dict(row)
    
    async def get_users_count(self) -> Dict:
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users')
            banned = await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_banned = TRUE')
            active_today = await conn.fetchval('''
                SELECT COUNT(*) FROM users 
                WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ''')
            return {
                'total': total,
                'banned': banned,
                'active_today': active_today
            }
    
    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("🔒 Database connection closed")

class MessageForwardingBot:
    def __init__(self, token: str, db: Database):
        self.token = token
        self.db = db
        self.storage = MemoryStorage()
        self.bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        self.is_running = True
        self.register_handlers()
        logger.info("🤖 Bot instance created")
    
    async def notify_admins(self, message: str, exclude_user_id: int = None):
        admins = await self.db.get_admins()
        for admin_id in admins:
            if exclude_user_id and admin_id == exclude_user_id:
                continue
            try:
                await self.bot.send_message(admin_id, message)
                logger.debug(f"📨 Notification sent to admin {admin_id}")
            except Exception as e:
                logger.error(f"Failed to send notification to admin {admin_id}: {e}")
    
    def get_user_info(self, user_data: Dict) -> str:
        if user_data and user_data.get('username'):
            return f"@{user_data['username']}"
        elif user_data and (user_data.get('first_name') or user_data.get('last_name')):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        elif user_data:
            return f"ID: {user_data['user_id']}"
        return "Unknown User"
    
    async def save_user_from_message(self, message: Message):
        user = message.from_user
        user_id = user.id
        await self.db.save_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        log_user_action("SAVED_FROM_MESSAGE", user_id, {
            'username': user.username,
            'first_name': user.first_name
        })
    
    async def check_ban_status(self, user_id: int) -> tuple[bool, str]:
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('is_banned'):
            return False, ""
        
        ban_until = user_data.get('ban_until')
        if ban_until:
            if hasattr(ban_until, 'tzinfo') and ban_until.tzinfo:
                ban_until = ban_until.replace(tzinfo=None)
            if datetime.now() > ban_until:
                await self.db.unban_user(user_id)
                logger.info(f"⏰ Auto-unban for user {user_id}")
                return False, ""
            return True, f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
        return True, "навсегда"
    
    async def check_rate_limit(self, user_id: int) -> tuple[bool, int]:
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('last_message_time'):
            return True, 0
        
        last_time = user_data['last_message_time']
        if hasattr(last_time, 'tzinfo') and last_time.tzinfo:
            last_time = last_time.replace(tzinfo=None)
        
        time_diff = (datetime.now() - last_time).total_seconds() / 60
        if time_diff < RATE_LIMIT_MINUTES:
            remaining = RATE_LIMIT_MINUTES - int(time_diff)
            logger.debug(f"⏳ Rate limit for user {user_id}: {remaining} minutes remaining")
            return False, remaining
        return True, 0
    
    async def forward_message_to_admins(self, message: Message, user_data: Dict, message_id: int):
        content_preview = ""
        if message.text:
            content_preview = f"\n💬 {message.text[:100]}{'...' if len(message.text) > 100 else ''}"
        elif message.caption:
            content_preview = f"\n📝 {message.caption[:100]}{'...' if len(message.caption) > 100 else ''}"
        elif message.photo:
            content_preview = "\n🖼 Фото"
        elif message.video:
            content_preview = "\n🎬 Видео"
        elif message.voice:
            content_preview = "\n🎤 Голосовое"
        elif message.sticker:
            content_preview = "\n😊 Стикер"
        
        text = (
            f"📩 <b>Новое сообщение #{message_id}</b>\n"
            f"<b>От:</b> {self.get_user_info(user_data)}\n"
            f"<b>ID:</b> <code>{user_data['user_id']}</code>\n"
            f"<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"{content_preview}\n\n"
            f"<i>Ответьте через: #ID текст</i>"
        )
        
        admins = await self.db.get_admins()
        success_count = 0
        
        for admin_id in admins:
            try:
                await self.bot.send_message(admin_id, text)
                logger.info(f"📨 Message #{message_id} forwarded to admin {admin_id}")
                
                if message.photo:
                    await self.bot.send_photo(admin_id, message.photo[-1].file_id)
                elif message.video:
                    await self.bot.send_video(admin_id, message.video.file_id)
                elif message.voice:
                    await self.bot.send_voice(admin_id, message.voice.file_id)
                elif message.sticker:
                    await self.bot.send_sticker(admin_id, message.sticker.file_id)
                elif message.document:
                    await self.bot.send_document(admin_id, message.document.file_id)
                
                success_count += 1
            except Exception as e:
                logger.error(f"Error sending to admin {admin_id}: {e}")
        
        logger.info(f"📊 Message #{message_id} forwarded to {success_count}/{len(admins)} admins")
        return success_count
    
    def register_handlers(self):
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            user = message.from_user
            logger.info(f"🚀 /start command from user {user.id} (@{user.username})")
            
            await self.save_user_from_message(message)
            
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="📱 Открыть приложение", 
                        web_app=WebAppInfo(url=APP_URL)
                    )
                ]]
            )
            
            await message.answer(
                f"<b>Привет, {message.from_user.first_name or 'пользователь'}.</b>\n\n"
                f"<b>Это бот для отправки сообщений.</b>\n\n"
                f"Отправляй сообщения в приложении\n"
                f"Лимит: {RATE_LIMIT_MINUTES} минут между сообщениями",
                reply_markup=keyboard
            )
            
            user_data = await self.db.get_user(message.from_user.id)
            log_user_action("START_COMMAND", message.from_user.id, {
                'username': user.username,
                'first_name': user.first_name
            })
            
            await self.notify_admins(
                f"<b>Новый пользователь:</b> {self.get_user_info(user_data)}",
                exclude_user_id=message.from_user.id
            )
        
        @self.router.message(Command("app"))
        async def cmd_app(message: Message):
            user = message.from_user
            logger.info(f"📱 /app command from user {user.id}")
            
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="📱 Открыть приложение", 
                        web_app=WebAppInfo(url=APP_URL)
                    )
                ]]
            )
            await message.answer(
                "📱 <b>Нажми кнопку ниже, чтобы открыть приложение</b>",
                reply_markup=keyboard
            )
            
            log_user_action("APP_COMMAND", user.id, {
                'username': user.username,
                'first_name': user.first_name
            })
        
        @self.router.message(Command("help"))
        async def cmd_help(message: Message):
            user = message.from_user
            logger.info(f"❓ /help command from user {user.id}")
            
            if await self.db.is_admin(user.id):
                await message.answer(
                    "<b>Команды администратора:</b>\n\n"
                    "• /app - открыть Mini App\n"
                    "• /stats - статистика\n"
                    "• /users - список пользователей\n"
                    "• #ID текст - ответить на сообщение\n"
                    "• /ban ID причина - заблокировать\n"
                    "• /unban ID - разблокировать\n"
                    "• /admin - управление админами"
                )
            else:
                await message.answer(
                    "🤖 <b>Команды:</b>\n\n"
                    "• /start - начать работу\n"
                    "• /app - открыть приложение\n"
                    "• /help - помощь\n\n"
                    f"📱 Используй Mini App для отправки сообщений"
                )
        
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            user = message.from_user
            logger.info(f"📊 /stats command from user {user.id}")
            
            if not await self.db.is_admin(user.id):
                logger.warning(f"⛔ Non-admin {user.id} attempted to access /stats")
                return
            
            stats = await self.db.get_stats()
            user_stats = await self.db.get_users_count()
            admins = await self.db.get_admins()
            
            text = (
                f"📊 <b>Статистика</b>\n\n"
                f"<b>Пользователи:</b>\n"
                f"• Всего: {user_stats['total']}\n"
                f"• Активных (24ч): {user_stats['active_today']}\n"
                f"• Заблокировано: {user_stats['banned']}\n"
                f"• Админов: {len(admins)}\n\n"
                f"<b>Сообщения:</b>\n"
                f"• Всего: {stats['total_messages']}\n"
                f"• Ответов: {stats['answers_sent']}\n"
                f"• Банов: {stats['bans_issued']}"
            )
            
            await message.answer(text)
            logger.info(f"📊 Stats sent to admin {user.id}")
        
        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            user = message.from_user
            logger.info(f"👥 /users command from user {user.id}")
            
            if not await self.db.is_admin(user.id):
                logger.warning(f"⛔ Non-admin {user.id} attempted to access /users")
                return
            
            users = await self.db.get_all_users()
            if not users:
                return await message.answer("📭 Нет пользователей")
            
            text = "👥 <b>Пользователи:</b>\n\n"
            for i, user_data in enumerate(users[:20], 1):
                status = '🚫' if user_data.get('is_banned') else '✅'
                is_admin = await self.db.is_admin(user_data['user_id'])
                admin_star = '👑 ' if is_admin else ''
                text += f"{i}. {status} {admin_star}{self.get_user_info(user_data)} | {user_data.get('messages_sent', 0)} msg\n"
            
            if len(users) > 20:
                text += f"\n<i>Показано 20 из {len(users)}</i>"
            
            await message.answer(text)
            logger.info(f"👥 User list sent to admin {user.id}")
        
        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            user = message.from_user
            logger.info(f"🔨 /ban command from user {user.id}")
            
            if not await self.db.is_admin(user.id):
                logger.warning(f"⛔ Non-admin {user.id} attempted to use /ban")
                return await message.answer("❌ Нет прав")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    return await message.answer("❌ /ban ID причина [часы]")
                
                peer_id = int(args[0])
                
                if await self.db.is_admin(peer_id):
                    logger.warning(f"⛔ Attempt to ban admin {peer_id} by {user.id}")
                    return await message.answer("❌ Нельзя заблокировать админа")
                
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    return await message.answer(f"❌ Часы: 1-{MAX_BAN_HOURS}")
                
                ban_until = datetime.now() + timedelta(hours=hours) if hours else None
                await self.db.ban_user(peer_id, reason, ban_until)
                await self.db.update_stats(bans_issued=1)
                
                ban_duration = f"на {hours} ч" if hours else "навсегда"
                await message.answer(f"✅ Пользователь {peer_id} заблокирован {ban_duration}")
                logger.info(f"🔨 User {peer_id} banned by {user.id}. Reason: {reason}")
                
            except Exception as e:
                logger.error(f"Error in /ban command: {e}")
                await message.answer(f"❌ Ошибка: {e}")
        
        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            user = message.from_user
            logger.info(f"✅ /unban command from user {user.id}")
            
            if not await self.db.is_admin(user.id):
                logger.warning(f"⛔ Non-admin {user.id} attempted to use /unban")
                return await message.answer("❌ Нет прав")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    return await message.answer("❌ /unban ID")
                
                peer_id = int(args[0])
                await self.db.unban_user(peer_id)
                await message.answer(f"✅ Пользователь {peer_id} разблокирован")
                logger.info(f"✅ User {peer_id} unbanned by {user.id}")
                
            except Exception as e:
                logger.error(f"Error in /unban command: {e}")
                await message.answer(f"❌ Ошибка: {e}")
        
        @self.router.message(Command("admin"))
        async def cmd_admin(message: Message):
            user = message.from_user
            logger.info(f"👑 /admin command from user {user.id}")
            
            if not await self.db.is_admin(user.id):
                logger.warning(f"⛔ Non-admin {user.id} attempted to use /admin")
                return await message.answer("❌ Нет прав")
            
            text = message.text.split()
            if len(text) == 1:
                await message.answer(
                    "👑 <b>Управление админами</b>\n\n"
                    "• /admin add ID - добавить\n"
                    "• /admin remove ID - удалить\n"
                    "• /admin list - список"
                )
            elif len(text) >= 3:
                action = text[1].lower()
                try:
                    target_id = int(text[2])
                    
                    if action == "add":
                        if target_id == OWNER_ID:
                            return await message.answer("👑 Владелец уже админ")
                        
                        if await self.db.add_admin(target_id, user.id):
                            await message.answer(f"✅ Админ {target_id} добавлен")
                            logger.info(f"👑 Admin {target_id} added by {user.id}")
                        else:
                            await message.answer("❌ Ошибка")
                    
                    elif action == "remove":
                        if target_id == OWNER_ID:
                            return await message.answer("❌ Нельзя удалить владельца")
                        
                        if await self.db.remove_admin(target_id):
                            await message.answer(f"✅ Админ {target_id} удален")
                            logger.info(f"👑 Admin {target_id} removed by {user.id}")
                        else:
                            await message.answer("❌ Ошибка")
                
                except ValueError:
                    await message.answer("❌ Неверный ID")
            
            elif len(text) == 2 and text[1].lower() == "list":
                admins = await self.db.get_admins()
                text = "👑 <b>Администраторы:</b>\n\n"
                for i, admin_id in enumerate(admins, 1):
                    user_data = await self.db.get_user(admin_id) or {}
                    if admin_id == OWNER_ID:
                        text += f"{i}. 👑 {self.get_user_info(user_data)} (владелец)\n"
                    else:
                        text += f"{i}. {self.get_user_info(user_data)}\n"
                await message.answer(text)
        
        @self.router.message()
        async def handle_message(message: Message):
            user = message.from_user
            logger.info(f"💬 Message from user {user.id}")
            
            user_id = user.id
            is_admin = await self.db.is_admin(user_id)
            
            if message.text and message.text.startswith('#'):
                logger.info(f"#️⃣ Reply command from user {user_id}: {message.text[:50]}")
                if is_admin:
                    await self.handle_answer_command(message)
                else:
                    await message.answer("❌ Только администраторы могут отвечать")
                return
            
            if is_admin:
                logger.debug(f"👑 Admin {user_id} sent non-command message")
                await message.answer(
                    "👑 <b>Для ответа используйте:</b>\n"
                    "<code>#ID текст ответа</code>\n\n"
                    "Например: #100569 Спасибо за обращение!"
                )
                return
            
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="📱 Открыть приложение", 
                        web_app=WebAppInfo(url=APP_URL)
                    )
                ]]
            )
            
            await message.answer(
                "<b>Отправка сообщений доступна только через приложение ниже.</b>\n\n",
                reply_markup=keyboard
            )
    
    async def handle_answer_command(self, message: Message):
        user = message.from_user
        text = message.text.strip()
        match = re.match(r'^#(\d+)\s+(.+)$', text, re.DOTALL)
        
        if not match:
            await message.answer("❌ Неверный формат. Используйте: #ID текст ответа")
            return
        
        message_id = int(match.group(1))
        answer_text = match.group(2).strip()
        
        logger.info(f"#️⃣ Processing reply to #{message_id} from admin {user.id}")
        
        original = await self.db.get_message(message_id)
        if not original:
            await message.answer(f"❌ Сообщение #{message_id} не найдено")
            logger.warning(f"❌ Message #{message_id} not found for reply")
            return
        
        user_id = original['user_id']
        
        is_banned, _ = await self.check_ban_status(user_id)
        if is_banned:
            await message.answer("❌ Пользователь заблокирован")
            logger.warning(f"⛔ Attempt to reply to banned user {user_id}")
            return
        
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="📱 Открыть переписку", 
                    web_app=WebAppInfo(url=APP_URL)
                )
            ]]
        )
        
        try:
            admin_name = self.get_user_info(await self.db.get_user(user.id))
            
            await self.bot.send_message(
                user_id,
                f"🔔 <b>Вам поступил ответ на сообщение #{message_id}</b>\n\n"
                f"{answer_text}\n\n"
                f"<i>Ответил: {admin_name}</i>",
                reply_markup=keyboard
            )
            
            logger.info(f"✅ Reply to #{message_id} sent to user {user_id}")
            
            await self.db.mark_message_answered(message_id, user.id, answer_text)
            await self.db.update_stats(answers_sent=1)
            
            await message.answer(f"✅ Ответ на #{message_id} отправлен")
            
            user_info = await self.db.get_user(user_id)
            await self.notify_admins(
                f"💬 Админ {admin_name} ответил на #{message_id} пользователю {self.get_user_info(user_info)}",
                exclude_user_id=user.id
            )
            
        except Exception as e:
            logger.error(f"Reply error: {e}")
            logger.error(traceback.format_exc())
            await message.answer("❌ Не удалось отправить ответ")
    
    async def process_web_app_message(self, user_id: int, text: str):
        logger.info(f"📱 WebApp message from user {user_id}: {text[:50]}...")
        
        user_data = await self.db.get_user(user_id)
        if user_data and user_data.get('is_banned'):
            ban_until = user_data.get('ban_until')
            if ban_until and datetime.now() > ban_until:
                await self.db.unban_user(user_id)
                logger.info(f"⏰ Auto-unban for user {user_id} during webapp message")
            else:
                logger.warning(f"⛔ Banned user {user_id} attempted to send message")
                return False, "banned"
        
        # Проверяем rate limit для обычных пользователей
        if not await self.db.is_admin(user_id):
            can_send, remaining = await self.check_rate_limit(user_id)
            if not can_send:
                logger.warning(f"⏳ Rate limit for user {user_id}: {remaining} minutes remaining")
                return False, f"rate_limit:{remaining}"
        
        try:
            message_id = await self.db.get_next_message_id()
            
            await self.db.save_message(
                message_id=message_id,
                user_id=user_id,
                content_type='text',
                text=text
            )
            
            class TempMessage:
                def __init__(self, text, user_id):
                    self.text = text
                    self.caption = None
                    self.content_type = 'text'
                    self.from_user = type('User', (), {'id': user_id})()
                    self.photo = None
                    self.video = None
                    self.voice = None
                    self.sticker = None
                    self.document = None
            
            temp_msg = TempMessage(text, user_id)
            user_data = await self.db.get_user(user_id)
            
            success_count = await self.forward_message_to_admins(temp_msg, user_data, message_id)
            
            if success_count > 0:
                await self.db.update_user_last_message(user_id, datetime.now())
                await self.db.update_user_stats(user_id, increment_messages=True)
                await self.db.update_stats(
                    total_messages=1,
                    successful_forwards=success_count
                )
                
                logger.info(f"✅ WebApp message #{message_id} from user {user_id} processed")
                return True, message_id
            else:
                logger.error(f"❌ No admins to forward message #{message_id}")
                return False, "no_admins"
                
        except Exception as e:
            logger.error(f"Process web app error: {e}")
            logger.error(traceback.format_exc())
            await self.db.update_stats(failed_forwards=1)
            return False, "error"
    
    async def shutdown(self, sig=None):
        logger.info(f"🛑 Shutting down... Signal: {sig}")
        self.is_running = False
        await self.bot.session.close()
        await self.dp.stop_polling()
        await self.db.close()
        logger.info("✅ Shutdown complete")
    
    async def run_polling(self):
        try:
            # Жесткий сброс вебхуков и ожидание
            await self.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
            
            logger.info("🤖 Bot started (polling mode)")
            logger.info(f"👑 Owner: {OWNER_ID}")
            logger.info(f"📱 Mini App URL: {APP_URL}")
            logger.info(f"🔍 Debug mode: {'ON' if DEBUG_MODE else 'OFF'}")
            
            while self.is_running:
                try:
                    await self.dp.start_polling(self.bot)
                except Exception as e:
                    logger.error(f"Polling error: {e}")
                    logger.error(traceback.format_exc())
                    if self.is_running:
                        logger.info("🔄 Restarting polling in 5 seconds...")
                        await asyncio.sleep(5)
        finally:
            await self.bot.session.close()
            await self.db.close()

# Функция для валидации данных от Telegram
def validate_telegram_data(init_data: str) -> Optional[Dict]:
    """Проверка подписи от Telegram"""
    try:
        # Парсим данные
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
            # Парсим пользователя из строки
            if 'user' in data:
                try:
                    data['user'] = json.loads(urllib.parse.unquote(data['user']))
                except:
                    pass
            return data
        return None
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return None

async def main():
    logger.info("🚀 Starting application...")
    logger.info(f"📊 Log level: {logging.getLevelName(logger.level)}")
    
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("❌ Missing BOT_TOKEN or DATABASE_URL environment variables")
        return
    
    logger.info(f"🤖 BOT_TOKEN: {'*' * 8}{BOT_TOKEN[-4:] if BOT_TOKEN else 'None'}")
    logger.info(f"🗄️ DATABASE_URL: {'*' * 8}{DATABASE_URL[-10:] if DATABASE_URL else 'None'}")
    logger.info(f"📱 APP_URL: {APP_URL}")
    logger.info(f"🔌 PORT: {PORT}")
    
    db = Database(DATABASE_URL)
    await db.create_pool()
    
    bot = MessageForwardingBot(BOT_TOKEN, db)
    
    app = web.Application()
    
    # Раздача статических файлов из папки mini_app
    async def static_files_handler(request: web.Request) -> web.Response:
        filename = request.match_info['filename']
        file_path = os.path.join(os.path.dirname(__file__), 'mini_app', filename)
        
        # Проверяем, существует ли файл и не пытаемся ли выйти за пределы папки
        if '..' in filename or not os.path.exists(file_path):
            logger.warning(f"❌ Static file not found: {filename}")
            return web.Response(status=404, text="File not found")
        
        # Определяем content-type по расширению
        content_types = {
            '.js': 'application/javascript',
            '.css': 'text/css',
            '.html': 'text/html',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.svg': 'image/svg+xml'
        }
        ext = os.path.splitext(filename)[1]
        content_type = content_types.get(ext, 'text/plain')
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"✅ Serving static file: {filename}")
            return web.Response(text=content, content_type=content_type)
        except Exception as e:
            logger.error(f"❌ Error serving static file {filename}: {e}")
            return web.Response(status=500, text="Internal server error")
    
    async def root_handler(request: web.Request) -> web.Response:
        logger.info(f"🌐 Root path accessed from {request.remote}")
        html_file_path = os.path.join(os.path.dirname(__file__), 'mini_app', 'index.html')
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            logger.info(f"✅ Serving index.html to {request.remote}")
            return web.Response(text=html_content, content_type='text/html')
        except FileNotFoundError:
            logger.error(f"❌ index.html not found at {html_file_path}")
            return web.Response(text="Mini App is running (HTML file not found)", content_type='text/plain')
    
    async def webhook_handler(request: web.Request) -> web.Response:
        try:
            update_data = await request.json()
            logger.debug(f"📨 Webhook received: {str(update_data)[:200]}...")
            update = Update(**update_data)
            await bot.dp.feed_update(bot.bot, update)
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            logger.error(traceback.format_exc())
            return web.Response(text="Error", status=500)
    
    async def web_app_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            logger.debug(f"📱 WebApp request: {str(data)[:200]}...")
            
            init_data = data.get('initData')
            if not init_data:
                logger.error("❌ No initData in webapp request")
                return web.json_response({'ok': False, 'error': 'No initData'})
            
            # Валидируем данные от Telegram
            validated_data = validate_telegram_data(init_data)
            if not validated_data:
                logger.error("❌ Invalid Telegram data signature")
                return web.json_response({'ok': False, 'error': 'Invalid signature'})
            
            user_info = validated_data.get('user', {})
            user_id = user_info.get('id')
            
            if not user_id:
                logger.error("❌ Could not determine user_id from webapp request")
                return web.json_response({'ok': False, 'error': 'User ID not found'})
            
            text = data.get('text', '').strip()
            
            if not text:
                return web.json_response({'ok': False, 'error': 'Empty message'})
            
            # Логируем действие пользователя
            user_data = await db.get_user(user_id)
            log_user_action("WEBAPP_MESSAGE", user_id, user_data, f"Text: {text[:50]}...")
            
            success, result = await bot.process_web_app_message(user_id, text)
            
            if success:
                logger.info(f"✅ WebApp message from user {user_id} processed, ID: {result}")
                return web.json_response({'ok': True, 'message_id': result})
            else:
                logger.warning(f"❌ WebApp message from user {user_id} failed: {result}")
                return web.json_response({'ok': False, 'error': result})
        except Exception as e:
            logger.error(f"Web app handler error: {e}")
            logger.error(traceback.format_exc())
            return web.json_response({'ok': False, 'error': str(e)})
    
    async def api_auth_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            init_data = data.get('initData')
            logger.info(f"🔐 Auth request received, initData length: {len(init_data) if init_data else 0}")
            
            if not init_data:
                logger.error("❌ No initData in auth request")
                return web.json_response({'ok': False, 'error': 'No initData'})
            
            # Валидируем данные от Telegram
            validated_data = validate_telegram_data(init_data)
            if not validated_data:
                logger.error("❌ Invalid Telegram data signature")
                return web.json_response({'ok': False, 'error': 'Invalid signature'})
            
            user_info = validated_data.get('user', {})
            user_id = user_info.get('id')
            
            if not user_id:
                logger.error("❌ Could not determine user_id from auth request")
                return web.json_response({'ok': False, 'error': 'User ID not found'})
            
            logger.info(f"👤 Auth from user {user_id} (@{user_info.get('username', 'N/A')})")
            
            # Сохраняем пользователя в БД
            await db.save_user(
                user_id=user_id,
                username=user_info.get('username'),
                first_name=user_info.get('first_name'),
                last_name=user_info.get('last_name')
            )
            
            log_user_action("AUTH", user_id, {
                'username': user_info.get('username'),
                'first_name': user_info.get('first_name')
            })
            
            # Проверяем, не забанен ли пользователь
            user_data = await db.get_user(user_id)
            is_banned = user_data and user_data.get('is_banned') if user_data else False
            
            if is_banned:
                ban_until = user_data.get('ban_until')
                if ban_until and datetime.now() > ban_until.replace(tzinfo=None):
                    await db.unban_user(user_id)
                    is_banned = False
                    ban_info = None
                else:
                    ban_info = {
                        'reason': user_data.get('ban_reason'),
                        'until': ban_until.isoformat() if ban_until else None
                    }
                    logger.warning(f"⛔ Banned user {user_id} attempted to auth")
                    
                    return web.json_response({
                        'ok': False,
                        'error': 'banned',
                        'ban_info': ban_info
                    }, status=403)
            
            is_admin = await db.is_admin(user_id)
            unanswered = await db.get_unanswered_count(user_id) if not is_admin else 0
            
            return web.json_response({
                'ok': True,
                'user': {
                    'id': user_id,
                    'is_admin': is_admin,
                    'is_banned': is_banned,
                    'first_name': user_info.get('first_name'),
                    'username': user_info.get('username'),
                    'unanswered': unanswered
                }
            })
        except Exception as e:
            logger.error(f"Auth handler error: {e}")
            logger.error(traceback.format_exc())
            return web.json_response({'ok': False, 'error': str(e)})
    
    async def api_messages_inbox_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                logger.error("❌ No initData in inbox request headers")
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            validated_data = validate_telegram_data(init_data)
            if not validated_data:
                logger.error("❌ Invalid signature in inbox request")
                return web.json_response({'error': 'Invalid signature'}, status=403)
            
            user_info = validated_data.get('user', {})
            user_id = user_info.get('id')
            
            if not user_id:
                return web.json_response({'error': 'User ID not found'}, status=400)
            
            logger.debug(f"📥 Inbox request from user {user_id}")
            
            messages = await db.get_user_inbox(user_id)
            
            # Форматируем даты для JSON
            for msg in messages:
                if msg.get('answered_at'):
                    if hasattr(msg['answered_at'], 'isoformat'):
                        msg['answered_at'] = msg['answered_at'].isoformat()
            
            return web.json_response({'messages': messages})
                
        except Exception as e:
            logger.error(f"Inbox handler error: {e}")
            logger.error(traceback.format_exc())
            return web.json_response({'messages': []})
    
    async def api_messages_sent_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                logger.error("❌ No initData in sent request headers")
                return web.json_response({'error': 'Unauthorized'}, status=401)
            
            validated_data = validate_telegram_data(init_data)
            if not validated_data:
                logger.error("❌ Invalid signature in sent request")
                return web.json_response({'error': 'Invalid signature'}, status=403)
            
            user_info = validated_data.get('user', {})
            user_id = user_info.get('id')
            
            if not user_id:
                return web.json_response({'error': 'User ID not found'}, status=400)
            
            logger.debug(f"📤 Sent request from user {user_id}")
            
            messages = await db.get_user_sent(user_id)
            
            # Форматируем даты для JSON
            for msg in messages:
                if msg.get('forwarded_at'):
                    if hasattr(msg['forwarded_at'], 'isoformat'):
                        msg['forwarded_at'] = msg['forwarded_at'].isoformat()
                if msg.get('answered_at'):
                    if hasattr(msg['answered_at'], 'isoformat'):
                        msg['answered_at'] = msg['answered_at'].isoformat()
            
            return web.json_response({'messages': messages})
                
        except Exception as e:
            logger.error(f"Sent handler error: {e}")
            logger.error(traceback.format_exc())
            return web.json_response({'messages': []})
    
    async def health_handler(request: web.Request) -> web.Response:
        logger.debug(f"💓 Health check from {request.remote}")
        return web.Response(text="OK")
    
    # Обработка сигналов для graceful shutdown
    async def shutdown_handler(sig):
        logger.info(f"📡 Received signal {sig}, starting graceful shutdown...")
        await bot.shutdown(sig)
        await asyncio.sleep(1)
    
    # Регистрируем маршруты - ВАЖНО: сначала статические файлы, потом корневой
    app.router.add_get('/{filename:.*\.js$}', static_files_handler)
    app.router.add_get('/{filename:.*\.css$}', static_files_handler)
    app.router.add_get('/{filename:.*\.png$}', static_files_handler)
    app.router.add_get('/{filename:.*\.jpg$}', static_files_handler)
    app.router.add_get('/{filename:.*\.svg$}', static_files_handler)
    app.router.add_get('/', root_handler)
    app.router.add_post('/webhook', webhook_handler)
    app.router.add_post('/api/send', web_app_handler)
    app.router.add_post('/api/auth', api_auth_handler)
    app.router.add_get('/api/messages/inbox', api_messages_inbox_handler)
    app.router.add_get('/api/messages/sent', api_messages_sent_handler)
    app.router.add_get('/health', health_handler)
    
    logger.info("✅ Routes registered:")
    logger.info("  - GET  /{filename}.js/.css (static files)")
    logger.info("  - GET  /")
    logger.info("  - POST /webhook")
    logger.info("  - POST /api/send")
    logger.info("  - POST /api/auth")
    logger.info("  - GET  /api/messages/inbox")
    logger.info("  - GET  /api/messages/sent")
    logger.info("  - GET  /health")
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    logger.info(f"🚀 HTTP server started on port {PORT}")
    
    # Настраиваем обработку сигналов
    if sys.platform != 'win32':
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
    
    try:
        await bot.run_polling()
    except KeyboardInterrupt:
        logger.info("🛑 Keyboard interrupt received")
    finally:
        await runner.cleanup()
        logger.info("✅ HTTP server stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot interrupted by user")
    except Exception as e:
        logger.critical(f"💥 Fatal error: {e}")
        logger.critical(traceback.format_exc())
