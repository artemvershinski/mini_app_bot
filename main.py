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
    log_msg = f"ДЕЙСТВИЕ [{action}] | ID: {user_id} | @{username} | {first_name}"
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
        logger.info("Подключение к PostgreSQL...")
        self.pool = await asyncpg.create_pool(self.dsn, min_size=10, max_size=20)
        await self.init_db()
        logger.info("Подключение к PostgreSQL установлено")

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
                logger.info("Добавление колонки accepted_tos в таблицу users...")
                await conn.execute('ALTER TABLE users ADD COLUMN accepted_tos BOOLEAN DEFAULT FALSE')
                logger.info("Колонка accepted_tos добавлена")
            
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
                logger.info("Добавление колонки answer_text в таблицу messages...")
                await conn.execute('ALTER TABLE messages ADD COLUMN answer_text TEXT')
                logger.info("Колонка answer_text добавлена")

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
                VALUES ($1, 'owner', 'Владелец', TRUE)
                ON CONFLICT (user_id) DO UPDATE SET username='owner', first_name='Владелец', accepted_tos = TRUE
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

    async def unset_tos(self, user_id: int) -> bool:
        """Сбрасывает согласие пользователя с условиями."""
        async with self.pool.acquire() as conn:
            result = await conn.execute('UPDATE users SET accepted_tos = FALSE, updated_at = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)
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
            logger.error(f"Ошибка добавления администратора {user_id}: {e}")
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
            logger.error(f"Ошибка удаления администратора {user_id}: {e}")
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
            logger.warning("База данных очищена администратором")

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
        logger.info("Экземпляр бота создан")

    async def notify_admins(self, message: str, exclude_user_id: int = None):
        admins = await self.db.get_admins()
        for admin_id in admins:
            if exclude_user_id and admin_id == exclude_user_id:
                continue
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление администратору {admin_id}: {e}")

    def get_user_info(self, user_data: Dict) -> str:
        if user_data and user_data.get('username'):
            return f"@{user_data['username']}"
        elif user_data and (user_data.get('first_name') or user_data.get('last_name')):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        elif user_data:
            return f"ID: {user_data['user_id']}"
        return "Неизвестный пользователь"

    def get_user_info_with_id(self, user_data: Dict) -> str:
        if user_data and user_data.get('username'):
            return f"@{user_data['username']} (ID: {user_data['user_id']})"
        elif user_data and (user_data.get('first_name') or user_data.get('last_name')):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')} (ID: {user_data['user_id']})".strip()
        elif user_data:
            return f"ID: {user_data['user_id']}"
        return "Неизвестный пользователь"

    async def save_user_from_message(self, message: Message):
        user = message.from_user
        await self.db.save_user(user_id=user.id, username=user.username, first_name=user.first_name, last_name=user.last_name)
        log_user_action("СОХРАНЕНИЕ_ИЗ_СООБЩЕНИЯ", user.id, {'username': user.username, 'first_name': user.first_name})

    async def check_ban_status(self, user_id: int) -> tuple[bool, str, Optional[datetime]]:
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('is_banned'):
            return False, "", None
        ban_until = user_data.get('ban_until')
        if ban_until:
            if hasattr(ban_until, 'tzinfo') and ban_until.tzinfo:
                ban_until = ban_until.replace(tzinfo=None)
            if datetime.now() > ban_until:
                await self.db.unban_user(user_id)
                return False, "", None
            return True, user_data.get('ban_reason', 'Причина не указана'), ban_until
        return True, user_data.get('ban_reason', 'Причина не указана'), None

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
            content_preview = f"\nТекст: {message.text[:100]}{'...' if len(message.text) > 100 else ''}"
        elif message.caption:
            content_preview = f"\nПодпись: {message.caption[:100]}{'...' if len(message.caption) > 100 else ''}"
        elif message.photo:
            content_preview = "\nФото"
        elif message.video:
            content_preview = "\nВидео"
        elif message.voice:
            content_preview = "\nГолосовое"
        elif message.sticker:
            content_preview = "\nСтикер"

        text = (
            f"📩 <b>Новое сообщение #{message_id}</b>\n"
            f"<b>Отправитель:</b> {self.get_user_info_with_id(user_data)}\n"
            f"<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"{content_preview}\n\n"
            f"<i>Для ответа используйте: #ID текст</i>"
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
                logger.error(f"Ошибка отправки администратору {admin_id}: {e}")
        logger.info(f"Сообщение #{message_id} переслано {success_count}/{len(admins)} администраторам")
        return success_count

    def register_handlers(self):
        
        @self.router.callback_query(lambda c: c.data == 'accept_tos')
        async def callback_accept_tos(callback_query: CallbackQuery):
            user_id = callback_query.from_user.id
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user_id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await callback_query.answer(
                    f"⛔ Вы заблокированы {ban_text}. Причина: {reason}",
                    show_alert=True
                )
                return
            
            await self.db.accept_tos(user_id)
            await callback_query.answer("✅ Спасибо! Условия приняты. Теперь вы можете пользоваться ботом.", show_alert=True)
            await callback_query.message.delete()
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=APP_URL))
            ]])
            user = callback_query.from_user
            await callback_query.message.answer(
                f"Уважаемый пользователь, {user.first_name or ''}.\n\n"
                f"Данный бот предназначен для направления обращений администратору.\n\n"
                f"Для отправки сообщения используйте кнопку ниже.\n"
                f"Лимит отправки: {RATE_LIMIT_MINUTES} минут между сообщениями.",
                reply_markup=keyboard
            )
            log_user_action("ПРИНЯТИЕ_TOS", user.id, {'username': user.username, 'first_name': user.first_name})

        # ========== КОМАНДА ДЛЯ СБРОСА СОГЛАСИЯ TOS ==========
        @self.router.message(Command("unset_tos"))
        async def cmd_unset_tos(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("❌ Использование: /unset_tos userID")
            
            try:
                target_id = int(args[1])
            except ValueError:
                return await message.answer("❌ Некорректный идентификатор пользователя.")
            
            # Проверяем существование пользователя
            user_data = await self.db.get_user(target_id)
            if not user_data:
                return await message.answer(f"❌ Пользователь с идентификатором {target_id} не найден.")
            
            # Проверяем, принял ли вообще ToS
            if not await self.db.has_accepted_tos(target_id):
                return await message.answer(f"ℹ️ Пользователь {target_id} еще не принимал условия использования.")
            
            # Сбрасываем согласие
            success = await self.db.unset_tos(target_id)
            if success:
                await message.answer(f"✅ Согласие с условиями использования для пользователя {target_id} сброшено.")
                logger.info(f"Администратор {user.id} сбросил ToS для пользователя {target_id}")
                
                # Оповещаем других админов
                admin_name = self.get_user_info(await self.db.get_user(user.id))
                target_name = self.get_user_info(user_data)
                await self.notify_admins(
                    f"🔄 Администратор {admin_name} сбросил согласие с условиями для пользователя {target_name}",
                    exclude_user_id=user.id
                )
            else:
                await message.answer(f"❌ Не удалось сбросить согласие для пользователя {target_id}.")

        # ========== КОМАНДА ДЛЯ ОТПРАВКИ КОПИИ ДАННЫХ ==========
        @self.router.message(Command("send_copy"))
        async def cmd_send_copy(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("❌ Использование: /send_copy userID")
            
            try:
                target_id = int(args[1])
            except ValueError:
                return await message.answer("❌ Некорректный идентификатор пользователя.")
            
            user_data = await self.db.get_user_full_data(target_id)
            if not user_data:
                return await message.answer(f"❌ Пользователь с идентификатором {target_id} не найден.")
            
            text = f"📋 <b>Данные пользователя ID: {target_id}</b>\n\n"
            text += f"👤 <b>Username:</b> @{user_data.get('username', 'отсутствует')}\n"
            text += f"📝 <b>Имя:</b> {user_data.get('first_name', 'отсутствует')} {user_data.get('last_name', '')}\n"
            text += f"📅 <b>Дата регистрации:</b> {user_data['created_at'].strftime('%d.%m.%Y %H:%M') if user_data['created_at'] else 'N/A'}\n"
            text += f"💬 <b>Сообщений отправлено:</b> {user_data.get('messages_sent', 0)}\n"
            text += f"📨 <b>Неотвеченных:</b> {user_data['unanswered_count']}\n"
            text += f"✅ <b>Согласие с условиями:</b> {'Да' if user_data.get('accepted_tos') else 'Нет'}\n"
            text += f"🚫 <b>Заблокирован:</b> {'Да' if user_data.get('is_banned') else 'Нет'}\n"
            if user_data.get('is_banned'):
                text += f"   <b>Причина:</b> {user_data.get('ban_reason', 'не указана')}\n"
                if user_data.get('ban_until'):
                    text += f"   <b>До:</b> {user_data['ban_until'].strftime('%d.%m.%Y %H:%M')}\n"
            
            await message.answer(text)
            
            if user_data['messages']:
                msgs_text = "📬 <b>История сообщений:</b>\n\n"
                for i, msg in enumerate(user_data['messages'][:10], 1):
                    status = "✅" if msg['is_answered'] else "⏳"
                    date = msg['forwarded_at'].strftime('%d.%m %H:%M') if msg['forwarded_at'] else 'N/A'
                    msgs_text += f"{i}. #{msg['message_id']} {status} {date}\n"
                    msgs_text += f"   {msg['text'][:100]}{'...' if len(msg['text']) > 100 else ''}\n\n"
                
                if len(user_data['messages']) > 10:
                    msgs_text += f"<i>... и ещё {len(user_data['messages']) - 10} сообщений</i>"
                
                await message.answer(msgs_text)
            else:
                await message.answer("📭 У пользователя нет сообщений.")
            
            logger.info(f"Администратор {user.id} запросил копию данных пользователя {target_id}")

        @self.router.message(Command("remove_data"))
        async def cmd_remove_data(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("❌ Использование: /remove_data userID")
            
            try:
                target_id = int(args[1])
            except ValueError:
                return await message.answer("❌ Некорректный идентификатор пользователя.")
            
            user_data = await self.db.get_user(target_id)
            if not user_data:
                return await message.answer(f"❌ Пользователь с идентификатором {target_id} не найден.")
            
            if await self.db.is_admin(target_id) and target_id != OWNER_ID:
                return await message.answer("⛔ Невозможно удалить данные администратора.")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.remove_data_confirmations[f"remove_{user.id}_{target_id}"] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5),
                'target_id': target_id
            }
            
            await message.answer(
                f"⚠️ <b>ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ</b>\n\n"
                f"Вы собираетесь полностью удалить пользователя ID: {target_id}\n"
                f"Будут безвозвратно удалены:\n"
                f"• Все сообщения пользователя\n"
                f"• Профиль пользователя\n"
                f"• История переписки\n\n"
                f"Данное действие <b>необратимо</b>.\n\n"
                f"Для подтверждения отправьте следующий код в течение 5 минут:\n"
                f"<code>{confirm_code}</code>\n\n"
                f"<i>Команда: /confirm_remove {target_id} {confirm_code}</i>"
            )

        @self.router.message(Command("confirm_remove"))
        async def cmd_confirm_remove(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 3:
                return await message.answer("❌ Использование: /confirm_remove ID КОД")
            
            try:
                target_id = int(args[1])
                code = args[2].strip()
            except ValueError:
                return await message.answer("❌ Некорректный идентификатор.")
            
            confirm_key = f"remove_{user.id}_{target_id}"
            confirm_data = self.db.remove_data_confirmations.get(confirm_key)
            
            if not confirm_data:
                return await message.answer("❌ Не найден активный запрос на удаление данного пользователя.")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.remove_data_confirmations[confirm_key]
                return await message.answer("❌ Время подтверждения истекло. Запросите удаление повторно.")
            
            if code != confirm_data['code']:
                return await message.answer("❌ Неверный код подтверждения.")
            
            deleted = await self.db.delete_all_user_data(target_id)
            
            if deleted:
                del self.db.remove_data_confirmations[confirm_key]
                await message.answer(f"✅ Пользователь {target_id} и все связанные с ним данные полностью удалены.")
                logger.info(f"Администратор {user.id} полностью удалил данные пользователя {target_id}")
                
                admin_name = self.get_user_info(await self.db.get_user(user.id))
                await self.notify_admins(
                    f"🗑 Администратор {admin_name} полностью удалил пользователя {target_id} и все его данные.",
                    exclude_user_id=user.id
                )
            else:
                await message.answer(f"❌ Не удалось удалить пользователя {target_id}.")

        # ========== КОМАНДЫ PRIVACY И TERMS ==========
        @self.router.message(Command("privacy"))
        async def cmd_privacy(message: Message):
            user = message.from_user
            logger.info(f"/privacy от пользователя {user.id}")
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user.id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await message.answer(
                    f"⛔ <b>Доступ заблокирован</b>\n\n"
                    f"Ваш аккаунт заблокирован {ban_text}.\n"
                    f"Причина: {reason}\n\n"
                    f"Для вопросов: @vrsnsky_bot"
                )
                return
            
            await message.answer(
                "📄 <b>Политика конфиденциальности</b>\n\n"
                "Полный текст документа доступен по ссылке:\n"
                "🔗 https://telegra.ph/Privacy-Policy-for-AV-Messages-Bot-02-26",
                disable_web_page_preview=True
            )

        @self.router.message(Command("terms"))
        async def cmd_terms(message: Message):
            user = message.from_user
            logger.info(f"/terms от пользователя {user.id}")
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user.id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await message.answer(
                    f"⛔ <b>Доступ заблокирован</b>\n\n"
                    f"Ваш аккаунт заблокирован {ban_text}.\n"
                    f"Причина: {reason}\n\n"
                    f"Для вопросов: @vrsnsky_bot"
                )
                return
            
            await message.answer(
                "📄 <b>Условия использования</b>\n\n"
                "Полный текст документа доступен по ссылке:\n"
                "🔗 https://telegra.ph/Terms-of-Service-for-message-to-av-Bot-02-26",
                disable_web_page_preview=True
            )

        # ========== ОСНОВНЫЕ КОМАНДЫ ==========
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            user = message.from_user
            logger.info(f"/start от пользователя {user.id} (@{user.username})")
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user.id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_remaining = (ban_until - datetime.now()).total_seconds() // 3600
                    if ban_remaining < 24:
                        ban_text = f"через {ban_remaining:.0f} часов" if ban_remaining > 1 else "через 1 час"
                    else:
                        days = ban_remaining // 24
                        ban_text = f"через {days:.0f} дней"
                await message.answer(
                    f"⛔ <b>ВЫ ЗАБЛОКИРОВАНЫ</b>\n\n"
                    f"<b>Причина:</b> {reason}\n"
                    f"<b>Истекает:</b> {ban_text}\n\n"
                    f"Если вы считаете, что это ошибка, обратитесь к администратору."
                )
                return
            
            await self.save_user_from_message(message)
            
            has_accepted = await self.db.has_accepted_tos(user.id)
            
            if not has_accepted:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принимаю условия", callback_data="accept_tos")
                ]])
                
                await message.answer(
                    f"Уважаемый пользователь, {user.first_name or ''}.\n\n"
                    f"Для использования функционала бота необходимо принять условия:\n\n"
                    f"📄 <a href='https://telegra.ph/Privacy-Policy-for-AV-Messages-Bot-02-26'>Политика конфиденциальности</a>\n"
                    f"📄 <a href='https://telegra.ph/Terms-of-Service-for-message-to-av-Bot-02-26'>Условия использования</a>\n\n"
                    f"Нажимая кнопку «Принимаю условия», вы подтверждаете ознакомление и согласие с указанными документами.",
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer(
                f"Уважаемый пользователь, {message.from_user.first_name or ''}.\n\n"
                f"Данный бот предназначен для направления обращений администратору.\n\n"
                f"Для отправки сообщения используйте кнопку ниже.\n"
                f"Лимит отправки: {RATE_LIMIT_MINUTES} минут между сообщениями.",
                reply_markup=keyboard
            )
            user_data = await self.db.get_user(message.from_user.id)
            log_user_action("START_COMMAND", message.from_user.id, {'username': user.username, 'first_name': user.first_name})
            await self.notify_admins(f"Новый пользователь: {self.get_user_info_with_id(user_data)}", exclude_user_id=message.from_user.id)

        @self.router.message(Command("app"))
        async def cmd_app(message: Message):
            user = message.from_user
            logger.info(f"/app от пользователя {user.id}")
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user.id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await message.answer(
                    f"⛔ <b>Доступ запрещён</b>\n\n"
                    f"Ваш аккаунт заблокирован {ban_text}.\n"
                    f"Причина: {reason}"
                )
                return
            
            if not await self.db.has_accepted_tos(user.id):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принимаю условия", callback_data="accept_tos")
                ]])
                await message.answer(
                    f"Для доступа к приложению необходимо принять условия использования. Пожалуйста, выполните команду /start.",
                    reply_markup=keyboard
                )
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer("Для перехода в приложение нажмите кнопку ниже.", reply_markup=keyboard)
            log_user_action("APP_COMMAND", user.id, {'username': user.username, 'first_name': user.first_name})

        @self.router.message(Command("help"))
        async def cmd_help(message: Message):
            user = message.from_user
            logger.info(f"/help от пользователя {user.id}")
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user.id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await message.answer(
                    f"⛔ <b>Доступ запрещён</b>\n\n"
                    f"Ваш аккаунт заблокирован {ban_text}.\n"
                    f"Причина: {reason}\n\n"
                    f"Команда /help недоступна заблокированным пользователям."
                )
                return
            
            # Проверяем ToS для не-админов
            is_admin = await self.db.is_admin(user.id)
            if not is_admin and not await self.db.has_accepted_tos(user.id):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принимаю условия", callback_data="accept_tos")
                ]])
                await message.answer(
                    f"Для доступа к справке необходимо принять условия использования. Пожалуйста, выполните команду /start.",
                    reply_markup=keyboard
                )
                return
            
            if is_admin:
                await message.answer(
                    "<b>Доступные команды администратора:</b>\n\n"
                    "📱 <b>Основные:</b>\n"
                    "• /app - открыть приложение\n"
                    "• /stats - статистика системы\n"
                    "• /users - список пользователей\n"
                    "• /requests - неотвеченные обращения\n\n"
                    "📝 <b>Работа с сообщениями:</b>\n"
                    "• #ID текст - ответить на сообщение\n"
                    "• /get #ID - информация о сообщении\n"
                    "• /del #ID - удалить сообщение\n\n"
                    "🔨 <b>Модерация:</b>\n"
                    "• /ban ID причина [часы] - заблокировать\n"
                    "• /unban ID - разблокировать\n"
                    "• /unset_tos ID - сбросить согласие с условиями\n\n"
                    "👑 <b>Управление:</b>\n"
                    "• /admin - управление администраторами\n"
                    "• /send_copy ID - получить копию данных\n"
                    "• /remove_data ID - удалить все данные\n"
                    "• /clear_db_1708 - полная очистка базы данных\n\n"
                    "📄 <b>Документы:</b>\n"
                    "• /privacy - политика конфиденциальности\n"
                    "• /terms - условия использования"
                )
            else:
                await message.answer(
                    "<b>Доступные команды:</b>\n\n"
                    "📱 /app - открыть приложение для отправки сообщений\n"
                    "📄 /privacy - политика конфиденциальности\n"
                    "📄 /terms - условия использования\n"
                    "❓ /help - эта справка\n\n"
                    "<i>Для отправки сообщений используйте кнопку «Открыть приложение» или команду /app</i>"
                )

        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            user = message.from_user
            logger.info(f"/stats от пользователя {user.id}")
            if not await self.db.is_admin(user.id):
                return
            stats = await self.db.get_stats()
            user_stats = await self.db.get_users_count()
            admins = await self.db.get_admins()
            text = (
                f"📊 <b>Статистика системы</b>\n\n"
                f"<b>Пользователи:</b>\n"
                f"• Всего: {user_stats['total']}\n"
                f"• Активных (24ч): {user_stats['active_today']}\n"
                f"• Заблокировано: {user_stats['banned']}\n"
                f"• Администраторов: {len(admins)}\n\n"
                f"<b>Сообщения:</b>\n"
                f"• Всего: {stats['total_messages']}\n"
                f"• Ответов: {stats['answers_sent']}\n"
                f"• Выдано банов: {stats['bans_issued']}"
            )
            await message.answer(text)

        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            user = message.from_user
            logger.info(f"/users от пользователя {user.id}")
            if not await self.db.is_admin(user.id):
                return
            users = await self.db.get_all_users()
            if not users:
                return await message.answer("📭 В системе нет зарегистрированных пользователей.")
            
            text = "👥 <b>Список пользователей:</b>\n\n"
            for i, u in enumerate(users[:20], 1):
                status = '🚫 ЗАБЛОКИРОВАН' if u.get('is_banned') else '✅ АКТИВЕН'
                is_admin = await self.db.is_admin(u['user_id'])
                admin_star = '👑 АДМИН ' if is_admin else ''
                username = f"@{u['username']}" if u.get('username') else 'нет username'
                tos_accepted = 'ДА' if u.get('accepted_tos') else 'НЕТ'
                text += f"{i}. {status} {admin_star}{username} (ID: {u['user_id']}) | ToS: {tos_accepted} | сообщений: {u.get('messages_sent', 0)}\n"
            if len(users) > 20:
                text += f"\n<i>Отображено 20 из {len(users)} пользователей</i>"
            await message.answer(text)

        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    return await message.answer("❌ Использование: /ban ID причина [часы]")
                peer_id = int(args[0])
                if await self.db.is_admin(peer_id):
                    return await message.answer("⛔ Невозможно заблокировать администратора.")
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    return await message.answer(f"❌ Количество часов должно быть от 1 до {MAX_BAN_HOURS}.")
                ban_until = datetime.now() + timedelta(hours=hours) if hours else None
                
                # Отправляем уведомление пользователю о блокировке
                try:
                    ban_text = f"на {hours} ч." if hours else "навсегда"
                    if hours:
                        ban_until_str = ban_until.strftime('%d.%m.%Y %H:%M')
                        await self.bot.send_message(
                            peer_id,
                            f"⛔ <b>ВЫ ЗАБЛОКИРОВАНЫ</b>\n\n"
                            f"<b>Причина:</b> {reason}\n"
                            f"<b>Блокировка истечет:</b> {ban_until_str}\n\n"
                            f"До истечения срока вы не можете пользоваться ботом."
                        )
                    else:
                        await self.bot.send_message(
                            peer_id,
                            f"⛔ <b>ВЫ ЗАБЛОКИРОВАНЫ НАВСЕГДА</b>\n\n"
                            f"<b>Причина:</b> {reason}\n\n"
                            f"Для вопросов обратитесь к администратору."
                        )
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление о бане пользователю {peer_id}: {e}")
                
                await self.db.ban_user(peer_id, reason, ban_until)
                await self.db.update_stats(bans_issued=1)
                ban_duration = f"на {hours} ч." if hours else "навсегда"
                await message.answer(f"✅ Пользователь {peer_id} заблокирован {ban_duration}.")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    return await message.answer("❌ Использование: /unban ID")
                peer_id = int(args[0])
                
                # Отправляем уведомление пользователю о разблокировке
                try:
                    await self.bot.send_message(
                        peer_id,
                        f"✅ <b>ВЫ РАЗБЛОКИРОВАНЫ</b>\n\n"
                        f"Блокировка снята. Теперь вы снова можете пользоваться ботом.\n\n"
                        f"Для начала работы выполните команду /start"
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление о разблокировке пользователю {peer_id}: {e}")
                
                await self.db.unban_user(peer_id)
                await message.answer(f"✅ Пользователь {peer_id} разблокирован.")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @self.router.message(Command("admin"))
        async def cmd_admin(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            text = message.text.split()
            if len(text) == 1:
                await message.answer("👑 <b>Управление администраторами</b>\n\n• /admin add ID - добавить\n• /admin remove ID - удалить\n• /admin list - список")
            elif len(text) >= 3:
                action = text[1].lower()
                try:
                    target_id = int(text[2])
                    if action == "add":
                        if target_id == OWNER_ID:
                            return await message.answer("👑 Владелец уже является администратором.")
                        if await self.db.add_admin(target_id, user.id):
                            await message.answer(f"✅ Пользователь {target_id} назначен администратором.")
                        else:
                            await message.answer("❌ Произошла ошибка при добавлении администратора.")
                    elif action == "remove":
                        if target_id == OWNER_ID:
                            return await message.answer("⛔ Невозможно удалить владельца из списка администраторов.")
                        if await self.db.remove_admin(target_id):
                            await message.answer(f"✅ Пользователь {target_id} исключен из списка администраторов.")
                        else:
                            await message.answer("❌ Произошла ошибка при удалении администратора.")
                except ValueError:
                    await message.answer("❌ Некорректный идентификатор пользователя.")
            elif len(text) == 2 and text[1].lower() == "list":
                admins = await self.db.get_admins()
                admin_text = "👑 <b>Список администраторов:</b>\n\n"
                for i, aid in enumerate(admins, 1):
                    ud = await self.db.get_user(aid) or {}
                    username = f"@{ud['username']}" if ud.get('username') else 'нет username'
                    if aid == OWNER_ID:
                        admin_text += f"{i}. 👑 {username} (ID: {aid}) - владелец\n"
                    else:
                        admin_text += f"{i}. {username} (ID: {aid})\n"
                await message.answer(admin_text)

        @self.router.message(Command("clear_db_1708"))
        async def cmd_clear_db(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.delete_confirmations[user.id] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5)
            }
            
            await message.answer(
                f"⚠️ <b>ОПАСНОЕ ДЕЙСТВИЕ</b>\n\n"
                f"Вы собираетесь полностью очистить базу данных.\n"
                f"> Все сообщения будут безвозвратно удалены.\n\n"
                f"Для подтверждения отправьте следующий код в течение 5 минут:\n"
                f"<code>{confirm_code}</code>\n\n"
                f"<i>Команда: /confirm_clear {confirm_code}</i>"
            )

        @self.router.message(Command("confirm_clear"))
        async def cmd_confirm_clear(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 2:
                return await message.answer("❌ Использование: /confirm_clear КОД")
            
            code = args[1].strip()
            confirm_data = self.db.delete_confirmations.get(user.id)
            
            if not confirm_data:
                return await message.answer("❌ Не найден активный запрос на очистку базы данных.")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.delete_confirmations[user.id]
                return await message.answer("❌ Время подтверждения истекло. Запросите очистку повторно.")
            
            if code != confirm_data['code']:
                return await message.answer("❌ Неверный код подтверждения.")
            
            await self.db.clear_database()
            del self.db.delete_confirmations[user.id]
            
            await message.answer("✅ База данных полностью очищена.")
            
            admin_name = self.get_user_info(await self.db.get_user(user.id))
            await self.notify_admins(
                f"⚠️ Администратор {admin_name} полностью очистил базу данных.",
                exclude_user_id=user.id
            )

        @self.router.message(Command("get"))
        async def cmd_get_message(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                return await message.answer("❌ Использование: /get #ID")
            arg = parts[1].strip()
            msg_id_str = arg.lstrip('#')
            if not msg_id_str.isdigit():
                return await message.answer("❌ Некорректный идентификатор. Пример: /get #123 или /get 123")
            msg_id = int(msg_id_str)
            msg_data = await self.db.get_message_with_details(msg_id)
            if not msg_data:
                return await message.answer(f"❌ Сообщение #{msg_id} не найдено.")

            user_info = f"{msg_data.get('user_first_name', '')} {msg_data.get('user_last_name', '')}".strip() or "Не указано"
            if msg_data.get('username'):
                user_info += f" (@{msg_data['username']})"
            text = f"📄 <b>Сообщение #{msg_id}</b>\n"
            text += f"👤 <b>Отправитель:</b> {user_info} (ID: {msg_data['user_id']})\n"
            text += f"📅 <b>Дата:</b> {msg_data['forwarded_at'].strftime('%d.%m.%Y %H:%M') if msg_data['forwarded_at'] else 'N/A'}\n"
            text += f"📝 <b>Текст:</b>\n{msg_data.get('text', '')}\n"
            if msg_data.get('is_answered'):
                answered_by = msg_data.get('answered_by_name') or f"ID {msg_data['answered_by']}"
                text += f"✅ <b>Ответ:</b> ({msg_data['answered_at'].strftime('%d.%m.%Y %H:%M') if msg_data['answered_at'] else ''}):\n{msg_data.get('answer_text', '')}\n"
                text += f"👤 <b>Ответил:</b> {answered_by}\n"
            else:
                text += "⏳ <b>Статус:</b> ожидает ответа"
            await message.answer(text)

        @self.router.message(Command("del"))
        async def cmd_delete_message(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                return await message.answer("❌ Использование: /del #ID")
            
            arg = parts[1].strip()
            msg_id_str = arg.lstrip('#')
            
            if not msg_id_str.isdigit():
                return await message.answer("❌ Некорректный идентификатор. Пример: /del #123")
            
            msg_id = int(msg_id_str)
            
            msg_data = await self.db.get_message(msg_id)
            if not msg_data:
                return await message.answer(f"❌ Сообщение #{msg_id} не найдено.")
            
            confirm_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.db.delete_confirmations[f"del_{user.id}_{msg_id}"] = {
                'code': confirm_code,
                'expires': datetime.now() + timedelta(minutes=5),
                'msg_id': msg_id
            }
            
            await message.answer(
                f"⚠️ <b>Подтверждение удаления</b>\n\n"
                f"Вы собираетесь удалить сообщение #{msg_id}.\n\n"
                f"Для подтверждения отправьте следующий код:\n"
                f"<code>{confirm_code}</code>\n\n"
                f"<i>Команда: /confirm_del {msg_id} {confirm_code}</i>"
            )

        @self.router.message(Command("confirm_del"))
        async def cmd_confirm_delete(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            
            args = message.text.split()
            if len(args) < 3:
                return await message.answer("❌ Использование: /confirm_del ID КОД")
            
            try:
                msg_id = int(args[1])
                code = args[2].strip()
            except ValueError:
                return await message.answer("❌ Некорректный идентификатор.")
            
            confirm_key = f"del_{user.id}_{msg_id}"
            confirm_data = self.db.delete_confirmations.get(confirm_key)
            
            if not confirm_data:
                return await message.answer("❌ Не найден активный запрос на удаление данного сообщения.")
            
            if datetime.now() > confirm_data['expires']:
                del self.db.delete_confirmations[confirm_key]
                return await message.answer("❌ Время подтверждения истекло. Запросите удаление повторно.")
            
            if code != confirm_data['code']:
                return await message.answer("❌ Неверный код подтверждения.")
            
            deleted = await self.db.delete_message(msg_id)
            
            if deleted:
                del self.db.delete_confirmations[confirm_key]
                await message.answer(f"✅ Сообщение #{msg_id} удалено.")
                logger.info(f"Администратор {user.id} удалил сообщение #{msg_id}")
                
                admin_name = self.get_user_info(await self.db.get_user(user.id))
                await self.notify_admins(
                    f"🗑 Администратор {admin_name} удалил сообщение #{msg_id}.",
                    exclude_user_id=user.id
                )
            else:
                await message.answer(f"❌ Не удалось удалить сообщение #{msg_id}.")

        @self.router.message(Command("requests"))
        async def cmd_requests(message: Message):
            user = message.from_user
            if not await self.db.is_admin(user.id):
                return await message.answer("⛔ У вас недостаточно прав для выполнения данной команды.")
            unanswered = await self.db.get_unanswered_requests()
            if not unanswered:
                await message.answer("✅ В настоящий момент неотвеченных обращений нет.")
                return
            
            text = "📋 <b>Неотвеченные обращения:</b>\n\n"
            for i, req in enumerate(unanswered[:20], 1):
                dt = req['forwarded_at'].strftime('%d.%m %H:%M') if req['forwarded_at'] else 'N/A'
                user_name = req.get('first_name') or req.get('username') or f"ID {req['user_id']}"
                user_id = req['user_id']
                msg_snippet = (req['text'][:50] + '…') if req['text'] and len(req['text']) > 50 else (req['text'] or '')
                text += f"{i}. #{req['message_id']} от {dt} — {user_name} (ID: {user_id})\n"
                text += f"   {msg_snippet}\n\n"
            if len(unanswered) > 20:
                text += f"<i>... и ещё {len(unanswered)-20} обращений</i>"
            await message.answer(text)

        @self.router.message()
        async def handle_message(message: Message):
            user = message.from_user
            user_id = user.id
            is_admin = await self.db.is_admin(user_id)
            
            # Проверяем бан
            is_banned, reason, ban_until = await self.check_ban_status(user_id)
            if is_banned:
                ban_text = "навсегда"
                if ban_until:
                    ban_text = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
                await message.answer(
                    f"⛔ <b>Доступ запрещён</b>\n\n"
                    f"Ваш аккаунт заблокирован {ban_text}.\n"
                    f"Причина: {reason}"
                )
                return
            
            if is_admin:
                if message.text and message.text.startswith('#'):
                    await self.handle_answer_command(message)
                else:
                    await message.answer(
                        "👑 <b>Для ответа используйте формат:</b>\n"
                        "<code>#ID текст ответа</code>\n\n"
                        "Например: #100569 Благодарим за обращение! Ответ будет предоставлен в ближайшее время."
                    )
                return
            
            if not await self.db.has_accepted_tos(user_id):
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="✅ Принимаю условия", callback_data="accept_tos")
                ]])
                await message.answer(
                    f"Уважаемый пользователь, {user.first_name or ''}.\n\n"
                    f"Для использования функционала бота необходимо принять условия:\n\n"
                    f"📄 <a href='https://telegra.ph/Privacy-Policy-for-AV-Messages-Bot-02-26'>Политика конфиденциальности</a>\n"
                    f"📄 <a href='https://telegra.ph/Terms-of-Service-for-message-to-av-Bot-02-26'>Условия использования</a>\n\n"
                    f"Нажимая кнопку «Принимаю условия», вы подтверждаете ознакомление и согласие с указанными документами.",
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                return
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=APP_URL))]])
            await message.answer(
                f"Уважаемый пользователь, {user.first_name or ''}.\n\n"
                f"Данный бот предназначен для направления обращений администратору.\n\n"
                f"Для отправки сообщения используйте кнопку ниже.\n"
                f"Лимит отправки: {RATE_LIMIT_MINUTES} минут между сообщениями.",
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
        original = await self.db.get_message(message_id)
        if not original:
            await message.answer(f"❌ Сообщение #{message_id} не найдено.")
            return
        user_id = original['user_id']
        is_banned, reason, ban_until = await self.check_ban_status(user_id)
        if is_banned:
            await message.answer("⛔ Невозможно отправить ответ заблокированному пользователю.")
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=APP_URL))
        ]])

        try:
            admin_name = self.get_user_info(await self.db.get_user(user.id))

            await self.bot.send_message(
                user_id,
                f"📬 <b>Получен ответ на ваше обращение #{message_id}</b>\n\n"
                f"Для просмотра ответа откройте приложение.",
                reply_markup=keyboard
            )

            await self.db.mark_message_answered(message_id, user.id, answer_text)
            await self.db.update_stats(answers_sent=1)

            await message.answer(f"✅ Ответ на обращение #{message_id} успешно отправлен пользователю.")

            user_info = await self.db.get_user(user_id)
            await self.notify_admins(
                f"💬 Администратор {admin_name} ответил на обращение #{message_id} пользователя {self.get_user_info_with_id(user_info)}.",
                exclude_user_id=user.id
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке ответа: {e}\n{traceback.format_exc()}")
            await message.answer("❌ Не удалось отправить ответ. Произошла техническая ошибка.")

    async def process_web_app_message(self, user_id: int, text: str):
        user_data = await self.db.get_user(user_id)
        
        # Проверка бана
        is_banned, reason, ban_until = await self.check_ban_status(user_id)
        if is_banned:
            ban_info = {
                'reason': reason,
                'until': ban_until.isoformat() if ban_until else None,
                'until_str': ban_until.strftime('%d.%m.%Y %H:%M') if ban_until else 'навсегда'
            }
            return False, f"banned:{json.dumps(ban_info)}"
        
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
            logger.error(f"Ошибка обработки сообщения из Web App: {e}\n{traceback.format_exc()}")
            await self.db.update_stats(failed_forwards=1)
            return False, "error"

    async def shutdown(self, sig=None):
        logger.info(f"Завершение работы... Сигнал: {sig}")
        self.is_running = False
        await self.bot.session.close()
        await self.dp.stop_polling()
        await self.db.close()
        logger.info("Завершение работы выполнено успешно.")

    async def run_polling(self):
        try:
            await self.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
            logger.info("Бот запущен в режиме polling")
            logger.info(f"Владелец: {OWNER_ID}")
            logger.info(f"URL приложения: {APP_URL}")
            while self.is_running:
                try:
                    await self.dp.start_polling(self.bot)
                except Exception as e:
                    logger.error(f"Ошибка в polling: {e}\n{traceback.format_exc()}")
                    if self.is_running:
                        await asyncio.sleep(5)
        finally:
            await self.bot.session.close()
            await self.db.close()

# ==================== Web Server Handlers ====================
async def main():
    if not BOT_TOKEN or not DATABASE_URL:
        logger.error("Отсутствует BOT_TOKEN или DATABASE_URL")
        return

    db = Database(DATABASE_URL)
    await db.create_pool()

    bot = MessageForwardingBot(BOT_TOKEN, db)

    app = web.Application()

    async def static_files_handler(request: web.Request) -> web.Response:
        filename = request.match_info['filename']
        file_path = os.path.join(os.path.dirname(__file__), 'mini_app', filename)
        if '..' in filename or not os.path.exists(file_path):
            return web.Response(status=404, text="Файл не найден")
        content_types = {'.js': 'application/javascript', '.css': 'text/css', '.html': 'text/html', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml'}
        ext = os.path.splitext(filename)[1]
        content_type = content_types.get(ext, 'text/plain')
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            return web.Response(text=content, content_type=content_type)
        except Exception as e:
            return web.Response(status=500, text="Внутренняя ошибка сервера")

    async def root_handler(request: web.Request) -> web.Response:
        html_path = os.path.join(os.path.dirname(__file__), 'mini_app', 'index.html')
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html = f.read()
            return web.Response(text=html, content_type='text/html')
        except FileNotFoundError:
            return web.Response(text="Файл Mini App index.html не найден", content_type='text/plain')

    async def webhook_handler(request: web.Request) -> web.Response:
        try:
            update_data = await request.json()
            update = Update(**update_data)
            await bot.dp.feed_update(bot.bot, update)
            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Ошибка webhook: {e}")
            return web.Response(text="Ошибка", status=500)

    async def api_auth_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            init_data = data.get('initData')
            logger.info(f"Запрос авторизации получен, длина initData: {len(init_data) if init_data else 0}")
            if not init_data:
                return web.json_response({'ok': False, 'error': 'Нет initData'})

            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'ok': False, 'error': 'ID пользователя не найден'})

            logger.info(f"Авторизация пользователя {user_id} (@{user_info.get('username', 'N/A')})")
            await db.save_user(user_id, username=user_info.get('username'), first_name=user_info.get('first_name'), last_name=user_info.get('last_name'))
            log_user_action("AUTH", user_id, {'username': user_info.get('username'), 'first_name': user_info.get('first_name')})

            user_data = await db.get_user(user_id)
            
            # Проверка бана
            is_banned = user_data and user_data.get('is_banned')
            ban_info = None
            if is_banned:
                ban_until = user_data.get('ban_until')
                if ban_until and datetime.now() > ban_until.replace(tzinfo=None):
                    await db.unban_user(user_id)
                    is_banned = False
                else:
                    ban_info = {
                        'reason': user_data.get('ban_reason'),
                        'until': ban_until.isoformat() if ban_until else None,
                        'until_str': ban_until.strftime('%d.%m.%Y %H:%M') if ban_until else 'навсегда'
                    }
                    return web.json_response({
                        'ok': False, 
                        'error': 'banned', 
                        'ban_info': ban_info
                    }, status=403)

            is_admin = await db.is_admin(user_id)
            unanswered = await db.get_unanswered_count(user_id) if not is_admin else 0
            has_accepted = await db.has_accepted_tos(user_id)

            return web.json_response({
                'ok': True,
                'user': {
                    'id': user_id,
                    'is_admin': is_admin,
                    'is_banned': is_banned,
                    'ban_info': ban_info,
                    'first_name': user_info.get('first_name'),
                    'username': user_info.get('username'),
                    'unanswered': unanswered,
                    'accepted_tos': has_accepted
                }
            })
        except Exception as e:
            logger.error(f"Ошибка обработчика авторизации: {e}\n{traceback.format_exc()}")
            return web.json_response({'ok': False, 'error': str(e)})

    async def web_app_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            init_data = data.get('initData')
            text = data.get('text', '').strip()
            if not init_data or not text:
                return web.json_response({'ok': False, 'error': 'Отсутствуют данные'})

            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'ok': False, 'error': 'ID пользователя не найден'})

            success, result = await bot.process_web_app_message(user_id, text)
            if success:
                return web.json_response({'ok': True, 'message_id': result})
            else:
                if isinstance(result, str) and result.startswith('banned:'):
                    ban_info_str = result[7:]
                    try:
                        ban_info = json.loads(ban_info_str)
                        return web.json_response({
                            'ok': False, 
                            'error': 'banned',
                            'ban_info': ban_info,
                            'message': f'Ваш аккаунт заблокирован. Причина: {ban_info["reason"]}. Истекает: {ban_info["until_str"]}'
                        })
                    except:
                        return web.json_response({'ok': False, 'error': 'banned', 'message': 'Ваш аккаунт заблокирован'})
                elif isinstance(result, str) and result.startswith('rate_limit:'):
                    minutes = result.split(':')[1]
                    return web.json_response({
                        'ok': False, 
                        'error': 'rate_limit',
                        'minutes': minutes,
                        'message': f'Лимит отправки сообщений: {RATE_LIMIT_MINUTES} минут. Осталось: {minutes} мин.'
                    })
                elif result == 'tos_not_accepted':
                    return web.json_response({
                        'ok': False,
                        'error': 'tos_not_accepted',
                        'message': 'Необходимо принять условия использования в боте. Выполните команду /start'
                    })
                else:
                    return web.json_response({'ok': False, 'error': result})
        except Exception as e:
            logger.error(f"Ошибка обработчика Web App: {e}\n{traceback.format_exc()}")
            return web.json_response({'ok': False, 'error': str(e)})

    async def api_messages_inbox_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                return web.json_response({'error': 'Не авторизован'}, status=401)
            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'error': 'ID пользователя не найден'}, status=400)
            
            # Проверка бана
            is_banned, reason, ban_until = await bot.check_ban_status(user_id)
            if is_banned:
                return web.json_response({'error': 'banned', 'ban_info': {
                    'reason': reason,
                    'until': ban_until.isoformat() if ban_until else None,
                    'until_str': ban_until.strftime('%d.%m.%Y %H:%M') if ban_until else 'навсегда'
                }}, status=403)
            
            messages = await db.get_user_inbox(user_id)
            for m in messages:
                if m.get('answered_at') and hasattr(m['answered_at'], 'isoformat'):
                    m['answered_at'] = m['answered_at'].isoformat()
            return web.json_response({'messages': messages})
        except Exception as e:
            logger.error(f"Ошибка обработчика входящих: {e}")
            return web.json_response({'messages': []})

    async def api_messages_sent_handler(request: web.Request) -> web.Response:
        try:
            init_data = request.headers.get('X-Telegram-Init-Data')
            if not init_data:
                return web.json_response({'error': 'Не авторизован'}, status=401)
            parsed = urllib.parse.parse_qs(init_data)
            user_str = parsed.get('user', ['{}'])[0]
            user_info = json.loads(urllib.parse.unquote(user_str))
            user_id = user_info.get('id')
            if not user_id:
                return web.json_response({'error': 'ID пользователя не найден'}, status=400)
            
            # Проверка бана
            is_banned, reason, ban_until = await bot.check_ban_status(user_id)
            if is_banned:
                return web.json_response({'error': 'banned', 'ban_info': {
                    'reason': reason,
                    'until': ban_until.isoformat() if ban_until else None,
                    'until_str': ban_until.strftime('%d.%m.%Y %H:%M') if ban_until else 'навсегда'
                }}, status=403)
            
            messages = await db.get_user_sent(user_id)
            for m in messages:
                if m.get('forwarded_at') and hasattr(m['forwarded_at'], 'isoformat'):
                    m['forwarded_at'] = m['forwarded_at'].isoformat()
                if m.get('answered_at') and hasattr(m['answered_at'], 'isoformat'):
                    m['answered_at'] = m['answered_at'].isoformat()
            return web.json_response({'messages': messages})
        except Exception as e:
            logger.error(f"Ошибка обработчика отправленных: {e}")
            return web.json_response({'messages': []})

    async def health_handler(request: web.Request) -> web.Response:
        return web.Response(text="OK")

    async def shutdown_handler(sig):
        logger.info(f"Получен сигнал {sig}, завершение работы...")
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

    logger.info("Маршруты зарегистрированы")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"HTTP сервер запущен на порту {PORT}")

    if sys.platform != 'win32':
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))

    try:
        await bot.run_polling()
    except KeyboardInterrupt:
        logger.info("Прерывание с клавиатуры")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}\n{traceback.format_exc()}")
