import asyncio, logging, os, sys, signal, asyncpg, random, string
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from aiogram import Bot, Dispatcher, Router, types
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, Update, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
import re
import json
import traceback
import urllib.parse

# ==================== Configuration ====================
OWNER_ID = 989062605
RATE_LIMIT_MINUTES = 10
MAX_BAN_HOURS = 720
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN")
APP_URL = os.getenv("APP_URL", "https://mini-app-bot-lzya.onrender.com")
PORT = int(os.getenv("PORT", 10000))
MESSAGE_ID_START = 100569

# ==================== Logging ====================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DEBUG_MODE = True

def log_user_action(action: str, user_id: int, user_data: dict = None, extra: str = ""):
    if not DEBUG_MODE:
        return
    username = user_data.get('username', 'NoUsername') if user_data else 'Unknown'
    first_name = user_data.get('first_name', 'NoName') if user_data else 'Unknown'
    log_msg = f"USER ACTION [{action}] | ID: {user_id} | @{username} | {first_name}"
    if extra:
        log_msg += f" | {extra}"
    logger.info(log_msg)

# ==================== Database Class ====================
class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self.admin_cache = []
        self.admin_cache_time = 0
        self.delete_confirmations = {}
        self.remove_data_confirmations = {}

    async def create_pool(self):
        logger.info("Connecting to PostgreSQL...")
        self.pool = await asyncpg.create_pool(self.dsn, min_size=10, max_size=20)
        await self.init_db()
        logger.info("PostgreSQL connection established")

    async def init_db(self):
        async with self.pool.acquire() as conn:
            # Users table
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
                    accepted_tos BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Check and add accepted_tos column
            try:
                await conn.execute('SELECT accepted_tos FROM users LIMIT 1')
            except asyncpg.UndefinedColumnError:
                logger.info("Adding column accepted_tos to users table...")
                await conn.execute('ALTER TABLE users ADD COLUMN accepted_tos BOOLEAN DEFAULT FALSE')
                logger.info("Column accepted_tos added")
            
            # Messages table
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
            
            # Check answer_text column
            try:
                await conn.execute('SELECT answer_text FROM messages LIMIT 1')
            except asyncpg.UndefinedColumnError:
                logger.info("Adding column answer_text to messages table...")
                await conn.execute('ALTER TABLE messages ADD COLUMN answer_text TEXT')
                logger.info("Column answer_text added")

            # Indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_is_answered ON messages(is_answered)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_forwarded_at ON messages(forwarded_at)')

            # Admins table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY,
                    added_by BIGINT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Stats table
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
            
            # Message counter
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_counter (
                    id INTEGER PRIMARY KEY,
                    last_message_id INTEGER NOT NULL
                )
            ''')
            await conn.execute('''
                INSERT INTO message_counter (id, last_message_id) 
                VALUES (1, $1) ON CONFLICT (id) DO NOTHING
            ''', MESSAGE_ID_START)
            
            # Owner user
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, accepted_tos) 
                VALUES ($1, 'owner', 'Owner', TRUE)
                ON CONFLICT (user_id) DO UPDATE SET username='owner', first_name='Owner', accepted_tos = TRUE
            ''', OWNER_ID)
            
            # Owner as admin
            await conn.execute('''
                INSERT INTO admins (user_id, added_by) VALUES ($1, $1) ON CONFLICT DO NOTHING
            ''', OWNER_ID)
            
            # Initial stats
            await conn.execute('''
                INSERT INTO stats (id, total_messages, successful_forwards, failed_forwards, bans_issued, rate_limit_blocks, answers_sent)
                VALUES (1,0,0,0,0,0,0) ON CONFLICT DO NOTHING
            ''')

    async def accept_tos(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute('UPDATE users SET accepted_tos = TRUE, updated_at = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)
            return result.split()[1] == '1'

    async def has_accepted_tos(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            accepted = await conn.fetchval('SELECT accepted_tos FROM users WHERE user_id = $1', user_id)
            return accepted is True

    async def get_next_message_id(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                UPDATE message_counter SET last_message_id = last_message_id + 1 WHERE id = 1 RETURNING last_message_id
            ''')
            return result['last_message_id'] if result else MESSAGE_ID_START

    async def save_message(self, message_id: int, user_id: int, content_type: str,
                           file_id: str = None, caption: str = None, text: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (message_id, user_id, content_type, file_id, caption, text)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', message_id, user_id, content_type, file_id, caption, text)

    async def get_message(self, message_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_id = $1', message_id)
            return dict(row) if row else None

    async def get_message_with_details(self, message_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT m.*, 
                       u.username, u.first_name as user_first_name, u.last_name as user_last_name,
                       a.first_name as answered_by_name
                FROM messages m
                LEFT JOIN users u ON m.user_id = u.user_id
                LEFT JOIN users a ON m.answered_by = a.user_id
                WHERE m.message_id = $1
            ''', message_id)
            return dict(row) if row else None

    async def delete_message(self, message_id: int) -> bool:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM messages WHERE message_id = $1)', message_id)
            if not exists:
                return False
            result = await conn.execute('DELETE FROM messages WHERE message_id = $1', message_id)
            return result.split()[1] == '1'

    async def delete_all_user_data(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM messages WHERE user_id = $1', user_id)
            result = await conn.execute('DELETE FROM users WHERE user_id = $1', user_id)
            return result.split()[1] == '1'

    async def get_user_full_data(self, user_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            user_row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            if not user_row:
                return None
            user_data = dict(user_row)
            messages_rows = await conn.fetch('''
                SELECT message_id, text, forwarded_at, is_answered, answered_at, answer_text
                FROM messages 
                WHERE user_id = $1 
                ORDER BY forwarded_at DESC
            ''', user_id)
            user_data['messages'] = [dict(row) for row in messages_rows]
            user_data['unanswered_count'] = len([m for m in user_data['messages'] if not m['is_answered']])
            return user_data

    async def get_unanswered_requests(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.message_id, m.text, m.forwarded_at, 
                       u.user_id, u.username, u.first_name, u.last_name
                FROM messages m
                JOIN users u ON m.user_id = u.user_id
                WHERE m.is_answered = FALSE
                ORDER BY m.forwarded_at ASC
            ''')
            return [dict(row) for row in rows]

    async def mark_message_answered(self, message_id: int, answered_by: int, answer_text: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE messages SET is_answered = TRUE, answered_by = $2, answered_at = CURRENT_TIMESTAMP, answer_text = $3
                WHERE message_id = $1
            ''', message_id, answered_by, answer_text)

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
            return [dict(row) for row in rows]

    async def get_user_sent(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT m.*, u.first_name as answered_by_name
                FROM messages m
                LEFT JOIN users u ON m.answered_by = u.user_id
                WHERE m.user_id = $1
                ORDER BY m.forwarded_at DESC
            ''', user_id)
            return [dict(row) for row in rows]

    async def get_user(self, user_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            return dict(row) if row else None

    async def get_unanswered_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            count = await conn.fetchval('SELECT COUNT(*) FROM messages WHERE user_id = $1 AND is_answered = FALSE', user_id)
            return count if count else 0

    async def save_user(self, user_id: int, **kwargs):
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)', user_id)
            if exists:
                set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys())])
                set_clause += ", updated_at = CURRENT_TIMESTAMP"
                await conn.execute(f'UPDATE users SET {set_clause} WHERE user_id = $1', user_id, *kwargs.values())
            else:
                fields = ['user_id'] + list(kwargs.keys())
                values = [user_id] + list(kwargs.values())
                placeholders = ', '.join([f'${i+1}' for i in range(len(values))])
                await conn.execute(f'INSERT INTO users ({", ".join(fields)}) VALUES ({placeholders})', *values)

    async def update_user_stats(self, user_id: int, increment_messages: bool = True):
        async with self.pool.acquire() as conn:
            if increment_messages:
                await conn.execute('UPDATE users SET messages_sent = messages_sent + 1, updated_at = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)
            else:
                await conn.execute('UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)

    async def update_user_last_message(self, user_id: int, message_time: datetime):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET last_message_time = $1, updated_at = CURRENT_TIMESTAMP WHERE user_id = $2', message_time, user_id)

    async def ban_user(self, user_id: int, reason: str, ban_until: Optional[datetime] = None):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET is_banned = TRUE, ban_reason = $1, ban_until = $2, updated_at = CURRENT_TIMESTAMP WHERE user_id = $3', reason, ban_until, user_id)
            self.admin_cache = []

    async def unban_user(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE users SET is_banned = FALSE, ban_reason = NULL, ban_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)

    async def get_all_users(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY created_at DESC')
            return [dict(row) for row in rows]

    async def add_admin(self, user_id: int, added_by: int) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO admins (user_id, added_by) VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET is_active = TRUE, added_by = EXCLUDED.added_by, added_at = CURRENT_TIMESTAMP
                ''', user_id, added_by)
                self.admin_cache = []
                return True
        except Exception as e:
            logger.error(f"Error adding admin {user_id}: {e}")
            return False

    async def remove_admin(self, user_id: int) -> bool:
        if user_id == OWNER_ID:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
                self.admin_cache = []
                return True
        except Exception as e:
            logger.error(f"Error removing admin {user_id}: {e}")
            return False

    async def get_admins(self) -> List[int]:
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
            await conn.execute(f'UPDATE stats SET {set_clause} WHERE id = 1', *kwargs.values())

    async def get_stats(self) -> Dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE id = 1')
            if not row:
                return {'total_messages':0, 'successful_forwards':0, 'failed_forwards':0, 'bans_issued':0, 'rate_limit_blocks':0, 'answers_sent':0}
            return dict(row)

    async def get_users_count(self) -> Dict:
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users')
            banned = await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_banned = TRUE')
            active_today = await conn.fetchval('SELECT COUNT(*) FROM users WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL \'24 hours\'')
            return {'total': total, 'banned': banned, 'active_today': active_today}

    async def clear_database(self):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM messages')
            await conn.execute('UPDATE message_counter SET last_message_id = $1 WHERE id = 1', MESSAGE_ID_START)
            await conn.execute('UPDATE stats SET total_messages = 0, successful_forwards = 0, failed_forwards = 0, answers_sent = 0 WHERE id = 1')
            await conn.execute('UPDATE users SET messages_sent = 0')
            logger.warning("Database cleared by admin command")

    async def close(self):
        if self.pool:
            await self.pool.close()

# ==================== Bot Class ====================
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
        logger.info("Bot instance created")

    async def notify_admins(self, message: str, exclude_user_id: int = None):
        admins = await self.db.get_admins()
        for admin_id in admins:
            if exclude_user_id and admin_id == exclude_user_id:
                continue
            try:
                await self.bot.send_message(admin_id, message)
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

    def get_user_info_with_id(self, user_data: Dict) -> str:
        if user_data and user_data.get('username'):
            return f"@{user_data['username']} (ID: {user_data['user_id']})"
        elif user_data and (user_data.get('first_name') or user_data.get('last_name')):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')} (ID: {user_data['user_id']})".strip()
        elif user_data:
            return f"ID: {user_data['user_id']}"
        return "Unknown User"

    async def save_user_from_message(self, message: Message):
        user = message.from_user
        await self.db.save_user(user_id=user.id, username=user.username, first_name=user.first_name, last_name=user.last_name)
        log_user_action("SAVED_FROM_MESSAGE", user.id, {'username': user.username, 'first_name': user.first_name})

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
                return False, ""
            return True, f"until {ban_until.strftime('%d.%m.%Y %H:%M')}"
        return True, "permanently"

    async def check_rate_limit(self, user_id: int) -> tuple[bool, int]:
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('last_message_time'):
            return True, 0
        last_time = user_data['last_message_time']
        if hasattr(last_time, 'tzinfo') and last_time.tzinfo:
            last_time = last_time.replace(tzinfo=None)
        time_diff = (datetime.now() - last_time).total_seconds() / 60
        if time_diff < RATE_LIMIT_MINUTES:
            return False, RATE_LIMIT_MINUTES - int(time_diff)
        return True, 0

    async def forward_message_to_admins(self, message: Message, user_data: Dict, message_id: int):
        content_preview = ""
        if message.text:
            content_preview = f"\nText: {message.text[:100]}{'...' if len(message.text) > 100 else ''}"
        elif message.caption:
            content_preview = f"\nCaption: {message.caption[:100]}{'...' if len(message.caption) > 100 else ''}"
        elif message.photo:
            content_preview = "\nPhoto"
        elif message.video:
            content_preview = "\nVideo"
        elif message.voice:
            content_preview = "\nVoice"
        elif message.sticker:
            content_preview = "\nSticker"

        text = (
            f"New message #{message_id}\n"
            f"From: {self.get_user_info_with_id(user_data)}\n"
            f"Time: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"{content_preview}\n\n"
            f"Reply with: #ID text"
        )
        admins = await self.db.get_admins()
        success_count = 0
        for admin_id in admins:
            try:
                await self.bot.send_message(admin_id, text)
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
        logger.info(f"Message #{message_id} forwarded to {success_count}/{len(admins)} admins")
        return success_count

    def register_handlers(self):
        
        @self.router.callback_query(lambda c: c.data == 'accept_tos')
        async def callback_accept_tos(callback_query: CallbackQuery):
            user_id = callback_query.from_user.id
            await self.db.accept_tos(user_id)
            await callback_query.answer("Thank you! Terms accepted. You can now use the bot.", show_alert=True)
            await callback_query.message.delete()
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=APP_URL))
            ]])
            user = callback_query.from_user
            await callback_query.message.answer(
                f"Hello, {user.first_name or 'user'}.\n\n"
                f"This bot allows you to send messages to the administrator.\n\n"
                f"Use the app below to send messages.\n"
                f"Limit: {RATE_LIMIT_MINUTES} minutes between messages",
                reply_markup=keyboard
            )
            log_user_action("ACCEPTED_TOS", user.id, {'username': user.username, 'first_name': user.first_name})

        # ========== КОМАНДА ДЛЯ ОТПРАВКИ КОПИИ ДАННЫХ ==========
        @self.router.message(Command("send_copy"))
        async def cmd_send_copy(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("Usage: /send_copy userID")
            
            try:
                target_id = int(args[1])
            except ValueError:
                return await message.answer("Invalid user ID")
            
            user_data = await self.db.get_user_full_data(target_id)
            if not user_data:
                return await message.answer(f"User with ID {target_id} not found")
            
            text = f"User data for ID: {target_id}\n\n"
            text += f"Username: @{user_data.get('username', 'none')}\n"
            text += f"Name: {user_data.get('first_name', 'none')} {user_data.get('last_name', '')}\n"
            text += f"Registration date: {user_data['created_at'].strftime('%d.%m.%Y %H:%M') if user_data['created_at'] else 'N/A'}\n"
            text += f"Messages sent: {user_data.get('messages_sent', 0)}\n"
            text += f"Unanswered: {user_data['unanswered_count']}\n"
            text += f"Banned: {'Yes' if user_data.get('is_banned') else 'No'}\n"
            if user_data.get('is_banned'):
                text += f"   Reason: {user_data.get('ban_reason', 'not specified')}\n"
                if user_data.get('ban_until'):
                    text += f"   Until: {user_data['ban_until'].strftime('%d.%m.%Y %H:%M')}\n"
            
            await message.answer(text)
            
            if user_data['messages']:
                msgs_text = "Message history:\n\n"
                for i, msg in enumerate(user_data['messages'][:10], 1):
                    status = "answered" if msg['is_answered'] else "waiting"
                    date = msg['forwarded_at'].strftime('%d.%m %H:%M') if msg['forwarded_at'] else 'N/A'
                    msgs_text += f"{i}. #{msg['message_id']} {status} {date}\n"
                    msgs_text += f"   {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n\n"
                
                if len(user_data['messages']) > 10:
                    msgs_text += f"... and {len(user_data['messages']) - 10} more messages"
                
                await message.answer(msgs_text)
            else:
                await message.answer("User has no messages")
            
            logger.info(f"Admin {user.id} requested data copy of user {target_id}")

        @self.router.message(Command("remove_data"))
        async def cmd_remove_data(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("Usage: /remove_data userID")
            
            try:
                target_id = int(args[1])
            except ValueError:
                return await message.answer("Invalid user ID")
            
            user_data = await self.db.get_user(target_id)
            if not user_data:
                return await message.answer(f"User with ID {target_id} not found")
            
            if await self.db.is_admin(target_id) and target_id != OWNER_ID:
                return await message.answer("Cannot delete administrator")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.remove_data_confirmations[f"remove_{user.id}_{target_id}"] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5),
                'target_id': target_id
            }
            
            await message.answer(
                f"CONFIRMATION REQUIRED\n\n"
                f"You are about to completely delete user ID: {target_id}\n"
                f"This will delete:\n"
                f"• All user messages\n"
                f"• User profile\n"
                f"• Conversation history\n\n"
                f"This action is irreversible.\n\n"
                f"To confirm, send this code within 5 minutes:\n"
                f"{confirm_code}\n\n"
                f"Command: /confirm_remove {target_id} {confirm_code}"
            )

        @self.router.message(Command("confirm_remove"))
        async def cmd_confirm_remove(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            args = message.text.split()
            if len(args) < 3:
                return await message.answer("Usage: /confirm_remove ID CODE")
            
            try:
                target_id = int(args[1])
                code = args[2].strip()
            except ValueError:
                return await message.answer("Invalid ID")
            
            confirm_key = f"remove_{user.id}_{target_id}"
            confirm_data = self.db.remove_data_confirmations.get(confirm_key)
            
            if not confirm_data:
                return await message.answer("No active deletion request for this user")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.remove_data_confirmations[confirm_key]
                return await message.answer("Confirmation time expired")
            
            if code != confirm_data['code']:
                return await message.answer("Invalid confirmation code")
            
            deleted = await self.db.delete_all_user_data(target_id)
            
            if deleted:
                del self.db.remove_data_confirmations[confirm_key]
                await message.answer(f"User {target_id} and all their data completely deleted")
                logger.info(f"Admin {user.id} removed all data of user {target_id}")
                
                admin_name = self.get_user_info(await self.db.get_user(user.id))
                await self.notify_admins(
                    f"Admin {admin_name} completely deleted user {target_id} and all their data",
                    exclude_user_id=user.id
                )
            else:
                await message.answer(f"Failed to delete user {target_id}")

        # ========== ОСНОВНЫЕ КОМАНДЫ ==========
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            user = message.from_user
            logger.info(f"/start command from user {user.id} (@{user.username})")
            await self.save_user_from_message(message)
            
            has_accepted = await self.db.has_accepted_tos(user.id)
            
            if not has_accepted:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="I Accept Terms", callback_data="accept_tos")
                ]])
                
                await message.answer(
                    f"Hello, {user.first_name or 'user'}.\n\n"
                    f"To use this bot, you must accept the terms:\n\n"
                    f"Privacy Policy: https://telegra.ph/Privacy-Policy-for-AV-Messages-Bot-02-26\n"
                    f"Terms of Service: https://telegra.ph/Terms-of-Service-for-message-to-av-Bot-02-26\n\n"
                    f"By clicking 'I Accept Terms', you confirm that you have read and agree to these documents.",
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer(
                f"Hello, {message.from_user.first_name or 'user'}.\n\n"
                f"This bot allows you to send messages to the administrator.\n\n"
                f"Use the app below to send messages.\n"
                f"Limit: {RATE_LIMIT_MINUTES} minutes between messages",
                reply_markup=keyboard
            )
            user_data = await self.db.get_user(message.from_user.id)
            log_user_action("START_COMMAND", message.from_user.id, {'username': user.username, 'first_name': user.first_name})
            await self.notify_admins(f"New user: {self.get_user_info_with_id(user_data)}", exclude_user_id=message.from_user.id)

        @self.router.message(Command("app"))
        async def cmd_app(message: Message):
            user = message.from_user
            logger.info(f"/app command from user {user.id}")
            
            if not await self.db.has_accepted_tos(user.id):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="I Accept Terms", callback_data="accept_tos")
                ]])
                await message.answer(
                    f"You need to accept the terms first. Please use /start.",
                    reply_markup=keyboard
                )
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer("Click the button below to open the app", reply_markup=keyboard)
            log_user_action("APP_COMMAND", user.id, {'username': user.username, 'first_name': user.first_name})

        @self.router.message(Command("help"))
        async def cmd_help(message: Message):
            user = message.from_user
            logger.info(f"/help command from user {user.id}")
            if await self.db.is_admin(user.id):
                await message.answer(
                    "Admin commands:\n\n"
                    "/app - open Mini App\n"
                    "/stats - statistics\n"
                    "/users - user list (with IDs)\n"
                    "/requests - unanswered messages\n"
                    "/get #ID - message info\n"
                    "/del #ID - delete message (with confirmation)\n"
                    "#ID text - reply to message\n"
                    "/ban ID reason [hours] - ban user\n"
                    "/unban ID - unban user\n"
                    "/admin - admin management (with IDs)\n"
                    "/send_copy ID - get user data copy\n"
                    "/remove_data ID - delete all user data (with confirmation)\n"
                    "/clear_db_1708 - clear database (delete all messages)"
                )
            else:
                await message.answer(
                    "Commands:\n\n"
                    "/start - start the bot\n"
                    "/app - open app\n"
                    "/help - this help\n\n"
                    "Use the Mini App to send messages"
                )

        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            user = message.from_user
            logger.info(f"/stats command from user {user.id}")
            if not await self.db.is_admin(user.id):
                return
            stats = await self.db.get_stats()
            user_stats = await self.db.get_users_count()
            admins = await self.db.get_admins()
            text = (
                f"Statistics\n\n"
                f"Users:\n"
                f"Total: {user_stats['total']}\n"
                f"Active (24h): {user_stats['active_today']}\n"
                f"Banned: {user_stats['banned']}\n"
                f"Admins: {len(admins)}\n\n"
                f"Messages:\n"
                f"Total: {stats['total_messages']}\n"
                f"Answers: {stats['answers_sent']}\n"
                f"Bans issued: {stats['bans_issued']}"
            )
            await message.answer(text)

        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            user = message.from_user
            logger.info(f"/users command from user {user.id}")
            if not await self.db.is_admin(user.id):
                return
            users = await self.db.get_all_users()
            if not users:
                return await message.answer("No users")
            
            text = "Users (ID + username):\n\n"
            for i, u in enumerate(users[:20], 1):
                status = 'BANNED' if u.get('is_banned') else 'ACTIVE'
                is_admin = await self.db.is_admin(u['user_id'])
                admin_star = 'ADMIN ' if is_admin else ''
                username = f"@{u['username']}" if u.get('username') else 'no username'
                tos_accepted = 'ACCEPTED' if u.get('accepted_tos') else 'NOT ACCEPTED'
                text += f"{i}. {status} {admin_star}{username} (ID: {u['user_id']}) [ToS: {tos_accepted}] | msgs: {u.get('messages_sent', 0)}\n"
            if len(users) > 20:
                text += f"\nShowing 20 of {len(users)}"
            await message.answer(text)

        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    return await message.answer("/ban ID reason [hours]")
                peer_id = int(args[0])
                if await self.db.is_admin(peer_id):
                    return await message.answer("Cannot ban admin")
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    return await message.answer(f"Hours must be 1-{MAX_BAN_HOURS}")
                ban_until = datetime.now() + timedelta(hours=hours) if hours else None
                await self.db.ban_user(peer_id, reason, ban_until)
                await self.db.update_stats(bans_issued=1)
                ban_duration = f"for {hours} h" if hours else "permanently"
                await message.answer(f"User {peer_id} banned {ban_duration}")
            except Exception as e:
                await message.answer(f"Error: {e}")

        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    return await message.answer("/unban ID")
                peer_id = int(args[0])
                await self.db.unban_user(peer_id)
                await message.answer(f"User {peer_id} unbanned")
            except Exception as e:
                await message.answer(f"Error: {e}")

        @self.router.message(Command("admin"))
        async def cmd_admin(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            text = message.text.split()
            if len(text) == 1:
                await message.answer("Admin management:\n\n/admin add ID - add\n/admin remove ID - remove\n/admin list - list (with IDs)")
            elif len(text) >= 3:
                action = text[1].lower()
                try:
                    target_id = int(text[2])
                    if action == "add":
                        if target_id == OWNER_ID:
                            return await message.answer("Owner is already admin")
                        if await self.db.add_admin(target_id, user.id):
                            await message.answer(f"Admin {target_id} added")
                        else:
                            await message.answer("Error")
                    elif action == "remove":
                        if target_id == OWNER_ID:
                            return await message.answer("Cannot remove owner")
                        if await self.db.remove_admin(target_id):
                            await message.answer(f"Admin {target_id} removed")
                        else:
                            await message.answer("Error")
                except ValueError:
                    await message.answer("Invalid ID")
            elif len(text) == 2 and text[1].lower() == "list":
                admins = await self.db.get_admins()
                admin_text = "Admins (ID + username):\n\n"
                for i, aid in enumerate(admins, 1):
                    ud = await self.db.get_user(aid) or {}
                    username = f"@{ud['username']}" if ud.get('username') else 'no username'
                    if aid == OWNER_ID:
                        admin_text += f"{i}. OWNER {username} (ID: {aid})\n"
                    else:
                        admin_text += f"{i}. {username} (ID: {aid})\n"
                await message.answer(admin_text)

        @self.router.message(Command("clear_db_1708"))
        async def cmd_clear_db(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.delete_confirmations[user.id] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5)
            }
            
            await message.answer(
                f"DANGEROUS ACTION\n\n"
                f"You are about to completely clear the database.\n"
                f"All messages will be permanently deleted.\n\n"
                f"To confirm, send this code within 5 minutes:\n"
                f"{confirm_code}\n\n"
                f"Command: /confirm_clear {confirm_code}"
            )

        @self.router.message(Command("confirm_clear"))
        async def cmd_confirm_clear(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("Usage: /confirm_clear CODE")
            
            code = args[1].strip()
            confirm_data = self.db.delete_confirmations.get(user.id)
            
            if not confirm_data:
                return await message.answer("No active clear request")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.delete_confirmations[user.id]
                return await message.answer("Confirmation time expired")
            
            if code != confirm_data['code']:
                return await message.answer("Invalid confirmation code")
            
            await self.db.clear_database()
            del self.db.delete_confirmations[user.id]
            
            await message.answer("Database completely cleared")
            
            admin_name = self.get_user_info(await self.db.get_user(user.id))
            await self.notify_admins(
                f"Admin {admin_name} completely cleared the database",
                exclude_user_id=user.id
            )

        @self.router.message(Command("get"))
        async def cmd_get_message(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                return await message.answer("Usage: /get #ID")
            arg = parts[1].strip()
            msg_id_str = arg.lstrip('#')
            if not msg_id_str.isdigit():
                return await message.answer("Invalid ID. Example: /get #123 or /get 123")
            msg_id = int(msg_id_str)
            msg_data = await self.db.get_message_with_details(msg_id)
            if not msg_data:
                return await message.answer(f"Message #{msg_id} not found")

            user_info = f"{msg_data.get('user_first_name', '')} {msg_data.get('user_last_name', '')}".strip() or "No name"
            if msg_data.get('username'):
                user_info += f" (@{msg_data['username']})"
            text = f"Message #{msg_id}\n"
            text += f"Sender: {user_info} (ID: {msg_data['user_id']})\n"
            text += f"Date: {msg_data['forwarded_at'].strftime('%d.%m.%Y %H:%M') if msg_data['forwarded_at'] else 'N/A'}\n"
            text += f"Text:\n{msg_data.get('text', '')}\n"
            if msg_data.get('is_answered'):
                answered_by = msg_data.get('answered_by_name') or f"ID {msg_data['answered_by']}"
                text += f"Answer ({msg_data['answered_at'].strftime('%d.%m.%Y %H:%M') if msg_data['answered_at'] else ''}):\n{msg_data.get('answer_text', '')}\n"
                text += f"Answered by: {answered_by}\n"
            else:
                text += "Status: waiting for answer"
            await message.answer(text)

        @self.router.message(Command("del"))
        async def cmd_delete_message(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                return await message.answer("Usage: /del #ID")
            
            arg = parts[1].strip()
            msg_id_str = arg.lstrip('#')
            
            if not msg_id_str.isdigit():
                return await message.answer("Invalid ID. Example: /del #123")
            
            msg_id = int(msg_id_str)
            
            msg_data = await self.db.get_message(msg_id)
            if not msg_data:
                return await message.answer(f"Message #{msg_id} not found")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.delete_confirmations[f"del_{user.id}_{msg_id}"] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5),
                'msg_id': msg_id
            }
            
            await message.answer(
                f"Delete confirmation\n\n"
                f"You are about to delete message #{msg_id}\n\n"
                f"To confirm, send this code:\n"
                f"{confirm_code}\n\n"
                f"Command: /confirm_del {msg_id} {confirm_code}"
            )

        @self.router.message(Command("confirm_del"))
        async def cmd_confirm_delete(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            
            args = message.text.split()
            if len(args) < 3:
                return await message.answer("Usage: /confirm_del ID CODE")
            
            try:
                msg_id = int(args[1])
                code = args[2].strip()
            except ValueError:
                return await message.answer("Invalid ID")
            
            confirm_key = f"del_{user.id}_{msg_id}"
            confirm_data = self.db.delete_confirmations.get(confirm_key)
            
            if not confirm_data:
                return await message.answer("No active deletion request for this message")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.delete_confirmations[confirm_key]
                return await message.answer("Confirmation time expired")
            
            if code != confirm_data['code']:
                return await message.answer("Invalid confirmation code")
            
            deleted = await self.db.delete_message(msg_id)
            
            if deleted:
                del self.db.delete_confirmations[confirm_key]
                await message.answer(f"Message #{msg_id} deleted")
                logger.info(f"Admin {user.id} deleted message #{msg_id}")
                
                admin_name = self.get_user_info(await self.db.get_user(user.id))
                await self.notify_admins(
                    f"Admin {admin_name} deleted message #{msg_id}",
                    exclude_user_id=user.id
                )
            else:
                await message.answer(f"Failed to delete message #{msg_id}")

        @self.router.message(Command("requests"))
        async def cmd_requests(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("No rights")
            unanswered = await self.db.get_unanswered_requests()
            if not unanswered:
                await message.answer("No unanswered requests")
                return
            
            text = "Unanswered requests:\n\n"
            for i, req in enumerate(unanswered[:20], 1):
                dt = req['forwarded_at'].strftime('%d.%m %H:%M') if req['forwarded_at'] else 'N/A'
                user_name = req.get('first_name') or req.get('username') or f"ID {req['user_id']}"
                user_id = req['user_id']
                msg_snippet = (req['text'][:50] + '…') if req['text'] and len(req['text']) > 50 else (req['text'] or '')
                text += f"{i}. #{req['message_id']} from {dt} — {user_name} (ID: {user_id})\n"
                text += f"   {msg_snippet}\n\n"
            if len(unanswered) > 20:
                text += f"... and {len(unanswered)-20} more requests"
            await message.answer(text)

        # ========== УНИВЕРСАЛЬНЫЙ ХЕНДЛЕР ДЛЯ ВСЕХ СООБЩЕНИЙ ==========
        @self.router.message()
        async def handle_message(message: Message):
            user = message.from_user
            user_id = user.id
            is_admin = await self.db.is_admin(user_id)
            
            # АДМИНЫ ПРОПУСКАЮТ ПРОВЕРКУ
            if is_admin:
                if message.text and message.text.startswith('#'):
                    await self.handle_answer_command(message)
                else:
                    await message.answer("To reply, use:\n#ID reply text\n\nExample: #100569 Thank you for your message!")
                return
            
            # ДЛЯ ВСЕХ НЕ-АДМИНОВ — ПРОВЕРЯЕМ ПРИНЯТИЕ TOS
            if not await self.db.has_accepted_tos(user_id):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="I Accept Terms", callback_data="accept_tos")
                ]])
                await message.answer(
                    f"Hello, {user.first_name or 'user'}.\n\n"
                    f"To use this bot, you must accept the terms:\n\n"
                    f"Privacy Policy: https://telegra.ph/Privacy-Policy-for-AV-Messages-Bot-02-26\n"
                    f"Terms of Service: https://telegra.ph/Terms-of-Service-for-message-to-av-Bot-02-26\n\n"
                    f"By clicking 'I Accept Terms', you confirm that you have read and agree to these documents.",
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                return
            
            # ЕСЛИ УЖЕ ПРИНЯЛ — ПОКАЗЫВАЕМ КНОПКУ ПРИЛОЖЕНИЯ
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer(
                f"Hello, {user.first_name or 'user'}.\n\n"
                f"This bot allows you to send messages to the administrator.\n\n"
                f"Use the app below to send messages.\n"
                f"Limit: {RATE_LIMIT_MINUTES} minutes between messages",
                reply_markup=keyboard
            )

    async def handle_answer_command(self, message: Message):
        user = message.from_user
        text = message.text.strip()
        match = re.match(r'^#(\d+)\s+(.+)$', text, re.DOTALL)
        if not match:
            await message.answer("Invalid format. Use: #ID reply text")
            return
        message_id = int(match.group(1))
        answer_text = match.group(2).strip()
        original = await self.db.get_message(message_id)
        if not original:
            await message.answer(f"Message #{message_id} not found")
            return
        user_id = original['user_id']
        is_banned, _ = await self.check_ban_status(user_id)
        if is_banned:
            await message.answer("User is banned")
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=APP_URL))
        ]])

        try:
            admin_name = self.get_user_info(await self.db.get_user(user.id))

            await self.bot.send_message(
                user_id,
                f"You have received a reply to message #{message_id}\n\n"
                f"You can view the reply in the app.",
                reply_markup=keyboard
            )

            await self.db.mark_message_answered(message_id, user.id, answer_text)
            await self.db.update_stats(answers_sent=1)

            await message.answer(f"Reply to #{message_id} sent to user")

            user_info = await self.db.get_user(user_id)
            await self.notify_admins(
                f"Admin {admin_name} replied to #{message_id} for user {self.get_user_info_with_id(user_info)}",
                exclude_user_id=user.id
            )
        except Exception as e:
            logger.error(f"Reply error: {e}\n{traceback.format_exc()}")
            await message.answer("Failed to send reply (server error)")

    async def process_web_app_message(self, user_id: int, text: str):
        user_data = await self.db.get_user(user_id)
        
        if user_data and user_data.get('is_banned'):
            ban_until = user_data.get('ban_until')
            if ban_until and datetime.now() > ban_until.replace(tzinfo=None):
                await self.db.unban_user(user_id)
            else:
                return False, "banned"
        
        if not await self.db.is_admin(user_id) and not await self.db.has_accepted_tos(user_id):
            return False, "tos_not_accepted"
        
        if not await self.db.is_admin(user_id):
            can_send, remaining = await self.check_rate_limit(user_id)
            if not can_send:
                return False, f"rate_limit:{remaining}"
        
        try:
            message_id = await self.db.get_next_message_id()
            await self.db.save_message(message_id, user_id, 'text', text=text)
            
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
                await self.db.update_stats(total_messages=1, successful_forwards=success_count)
                return True, message_id
            else:
                return False, "no_admins"
        except Exception as e:
            logger.error(f"Process web app error: {e}\n{traceback.format_exc()}")
            await self.db.update_stats(failed_forwards=1)
            return False, "error"

    async def shutdown(self, sig=None):
        logger.info(f"Shutting down... Signal: {sig}")
        self.is_running = False
        await self.bot.session.close()
        await self.dp.stop_polling()
        await self.db.close()
        logger.info("Shutdown complete")

    async def run_polling(self):
        try:
            await self.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
            logger.info("Bot started (polling mode)")
            logger.info(f"Owner: {OWNER_ID}")
            logger.info(f"Mini App URL: {APP_URL}")
            while self.is_running:
                try:
                    await self.dp.start_polling(self.bot)
                except Exception as e:
                    logger.error(f"Polling error: {e}\n{traceback.format_exc()}")
                    if self.is_running:
                        await asyncio.sleep(5)
        finally:
            await self.bot.session.close()
            await self.db.close()

# ==================== Web Server Handlers ====================
async def main():
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("Missing BOT_TOKEN or DATABASE_URL")
        return

    db = Database(DATABASE_URL)
    await db.create_pool()

    bot = MessageForwardingBot(BOT_TOKEN, db)

    app = web.Application()

    async def static_files_handler(request: web.Request) -> web.Response:
        filename = request.match_info['filename']
        file_path = os.path.join(os.path.dirname(__file__), 'mini_app', filename)
        if '..' in filename or not os.path.exists(file_path):
            return web.Response(status=404, text="File not found")
        content_types = {'.js': 'application/javascript', '.css': 'text/css', '.html': 'text/html', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml'}
        ext = os.path.splitext(filename)[1]
        content_type = content_types.get(ext, 'text/plain')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return web.Response(text=content, content_type=content_type)
        except Exception as e:
            return web.Response(status=500, text="Internal error")

    async def root_handler(request: web.Request) -> web.Response:
        html_path = os.path.join(os.path.dirname(__file__), 'mini_app', 'index.html')
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html = f.read()
            return web.Response(text=html, content_type='text/html')
        except FileNotFoundError:
            return web.Response(text="Mini App index.html not found", content_type='text/plain')

    async def webhook_handler(request: web.Request) -> web.Response:
        try:
            update_data = await request.json()
            update = Update(**update_data)
            await bot.dp.feed_update(bot.bot, update)
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(text="Error", status=500)

    async def api_auth_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            init_data = data.get('initData')
            logger.info(f"Auth request received, initData length: {len(init_data) if init_data else 0}")
            if not init_data:
                return web.json_response({'ok': False, 'error': 'No initData'})

            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'ok': False, 'error': 'User ID not found'})

            logger.info(f"Auth from user {user_id} (@{user_info.get('username', 'N/A')})")
            await db.save_user(user_id, username=user_info.get('username'), first_name=user_info.get('first_name'), last_name=user_info.get('last_name'))
            log_user_action("AUTH", user_id, {'username': user_info.get('username'), 'first_name': user_info.get('first_name')})

            user_data = await db.get_user(user_id)
            is_banned = user_data and user_data.get('is_banned')
            if is_banned:
                ban_until = user_data.get('ban_until')
                if ban_until and datetime.now() > ban_until.replace(tzinfo=None):
                    await db.unban_user(user_id)
                    is_banned = False
                else:
                    ban_info = {'reason': user_data.get('ban_reason'), 'until': ban_until.isoformat() if ban_until else None}
                    return web.json_response({'ok': False, 'error': 'banned', 'ban_info': ban_info}, status=403)

            is_admin = await db.is_admin(user_id)
            unanswered = await db.get_unanswered_count(user_id) if not is_admin else 0
            has_accepted = await db.has_accepted_tos(user_id)

            return web.json_response({
                'ok': True,
                'user': {
                    'id': user_id,
                    'is_admin': is_admin,
                    'is_banned': is_banned,
                    'first_name': user_info.get('first_name'),
                    'username': user_info.get('username'),
                    'unanswered': unanswered,
                    'accepted_tos': has_accepted
                }
            })
        except Exception as e:
            logger.error(f"Auth handler error: {e}\n{traceback.format_exc()}")
            return web.json_response({'ok': False, 'error': str(e)})

    async def web_app_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            init_data = data.get('initData')
            text = data.get('text', '').strip()
            if not init_data or not text:
                return web.json_response({'ok': False, 'error': 'Missing data'})

            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'ok': False, 'error': 'User ID not found'})

            success, result = await bot.process_web_app_message(user_id, text)
            if success:
                return web.json_response({'ok': True, 'message_id': result})
            else:
                if isinstance(result, str) and result.startswith('rate_limit:'):
                    minutes = result.split(':')[1]
                    return web.json_response({
                        'ok': False, 
                        'error': 'rate_limit',
                        'minutes': minutes,
                        'message': f'Limit: {RATE_LIMIT_MINUTES} minutes. Remaining: {minutes} min.'
                    })
                elif result == 'tos_not_accepted':
                    return web.json_response({
                        'ok': False,
                        'error': 'tos_not_accepted',
                        'message': 'You must accept the terms of use in the bot. Type /start'
                    })
                else:
                    return web.json_response({'ok': False, 'error': result})
        except Exception as e:
            logger.error(f"Web app handler error: {e}\n{traceback.format_exc()}")
            return web.json_response({'ok': False, 'error': str(e)})

    async def api_messages_inbox_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                return web.json_response({'error': 'Unauthorized'}, status=401)
            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'error': 'User ID not found'}, status=400)
            messages = await db.get_user_inbox(user_id)
            for m in messages:
                if m.get('answered_at') and hasattr(m['answered_at'], 'isoformat'):
                    m['answered_at'] = m['answered_at'].isoformat()
            return web.json_response({'messages': messages})
        except Exception as e:
            logger.error(f"Inbox handler error: {e}")
            return web.json_response({'messages': []})

    async def api_messages_sent_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                return web.json_response({'error': 'Unauthorized'}, status=401)
            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'error': 'User ID not found'}, status=400)
            messages = await db.get_user_sent(user_id)
            for m in messages:
                if m.get('forwarded_at') and hasattr(m['forwarded_at'], 'isoformat'):
                    m['forwarded_at'] = m['forwarded_at'].isoformat()
                if m.get('answered_at') and hasattr(m['answered_at'], 'isoformat'):
                    m['answered_at'] = m['answered_at'].isoformat()
            return web.json_response({'messages': messages})
        except Exception as e:
            logger.error(f"Sent handler error: {e}")
            return web.json_response({'messages': []})

    async def health_handler(request: web.Request) -> web.Response:
        return web.Response(text="OK")

    async def shutdown_handler(sig):
        logger.info(f"Received signal {sig}, shutting down...")
        await bot.shutdown(sig)
        await asyncio.sleep(1)

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

    logger.info("Routes registered")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"HTTP server started on port {PORT}")

    if sys.platform != 'win32':
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))

    try:
        await bot.run_polling()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrupted")
    except Exception as e:
        logger.critical(f"Fatal: {e}\n{traceback.format_exc()}")
