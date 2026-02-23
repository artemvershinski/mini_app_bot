import asyncio, logging, os, sys, signal, asyncpg
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from keep_alive import create_keep_alive_server
from aiohttp import web
import re
import json

# Конфигурация
OWNER_ID = 989062605
RATE_LIMIT_MINUTES = 10
MAX_BAN_HOURS = 720
KEEP_ALIVE_PORT = int(os.getenv("PORT", 8080))
DATABASE_URL = os.getenv("DATABASE_URL")
APP_URL = os.getenv("APP_URL", "https://message-forwarding-bot.onrender.com")
MESSAGE_ID_START = 100569

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
    
    async def create_pool(self):
        """Создание пула соединений с БД"""
        self.pool = await asyncpg.create_pool(self.dsn)
        await self.init_db()
        logger.info("✅ Подключение к PostgreSQL установлено")
    
    async def init_db(self):
        """Инициализация таблиц с проверкой существующих колонок"""
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
            
            # Таблица сообщений
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
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
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
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Добавляем колонку answers_sent если её нет
            try:
                await conn.execute('SELECT answers_sent FROM stats LIMIT 1')
            except asyncpg.UndefinedColumnError:
                logger.info("Добавляем колонку answers_sent в таблицу stats")
                await conn.execute('ALTER TABLE stats ADD COLUMN answers_sent INTEGER DEFAULT 0')
            
            # Таблица для хранения последнего ID сообщения
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_counter (
                    id INTEGER PRIMARY KEY,
                    last_message_id INTEGER NOT NULL
                )
            ''')
            
            # Инициализируем счетчик сообщений
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
    
    async def get_next_message_id(self) -> int:
        """Получение следующего ID сообщения"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow('''
                UPDATE message_counter 
                SET last_message_id = last_message_id + 1 
                WHERE id = 1 
                RETURNING last_message_id
            ''')
            return result['last_message_id']
    
    async def save_message(self, message_id: int, user_id: int, content_type: str, 
                          file_id: str = None, caption: str = None, text: str = None):
        """Сохранение сообщения"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO messages (message_id, user_id, content_type, file_id, caption, text)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', message_id, user_id, content_type, file_id, caption, text)
    
    async def get_message(self, message_id: int) -> Optional[Dict]:
        """Получение сообщения по ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM messages WHERE message_id = $1', message_id)
            return dict(row) if row else None
    
    async def mark_message_answered(self, message_id: int, answered_by: int):
        """Отметить сообщение как отвеченное"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE messages 
                SET is_answered = TRUE, answered_by = $2, answered_at = CURRENT_TIMESTAMP
                WHERE message_id = $1
            ''', message_id, answered_by)
    
    async def get_user_messages(self, user_id: int, limit: int = 50) -> List[Dict]:
        """Получение сообщений пользователя"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM messages 
                WHERE user_id = $1 
                ORDER BY forwarded_at DESC 
                LIMIT $2
            ''', user_id, limit)
            return [dict(row) for row in rows]
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя по ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            return dict(row) if row else None
    
    async def save_user(self, user_id: int, **kwargs):
        """Сохранение или обновление пользователя"""
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)', user_id)
            
            if exists:
                set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys())])
                set_clause += ", updated_at = CURRENT_TIMESTAMP"
                query = f'UPDATE users SET {set_clause} WHERE user_id = $1'
                await conn.execute(query, user_id, *kwargs.values())
            else:
                fields = ['user_id'] + list(kwargs.keys())
                values = [user_id] + list(kwargs.values())
                placeholders = ', '.join([f'${i+1}' for i in range(len(values))])
                query = f'INSERT INTO users ({", ".join(fields)}) VALUES ({placeholders})'
                await conn.execute(query, *values)
    
    async def update_user_stats(self, user_id: int, increment_messages: bool = True):
        """Обновление статистики пользователя"""
        async with self.pool.acquire() as conn:
            if increment_messages:
                await conn.execute('''
                    UPDATE users SET messages_sent = messages_sent + 1, 
                                     updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
            else:
                await conn.execute('''
                    UPDATE users SET updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
    
    async def update_user_last_message(self, user_id: int, message_time: datetime):
        """Обновление времени последнего сообщения"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET last_message_time = $1, updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $2
            ''', message_time, user_id)
    
    async def ban_user(self, user_id: int, reason: str, ban_until: Optional[datetime] = None):
        """Блокировка пользователя"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = TRUE, 
                                 ban_reason = $1, 
                                 ban_until = $2,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $3
            ''', reason, ban_until, user_id)
    
    async def unban_user(self, user_id: int):
        """Разблокировка пользователя"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = FALSE, 
                                 ban_reason = NULL, 
                                 ban_until = NULL,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $1
            ''', user_id)
    
    async def get_all_users(self) -> List[Dict]:
        """Получение всех пользователей"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY created_at DESC')
            return [dict(row) for row in rows]
    
    async def add_admin(self, user_id: int, added_by: int) -> bool:
        """Добавление администратора"""
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
                return True
        except Exception as e:
            logger.error(f"Ошибка добавления админа {user_id}: {e}")
            return False
    
    async def remove_admin(self, user_id: int) -> bool:
        """Удаление администратора"""
        if user_id == OWNER_ID:
            return False
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
                return True
        except Exception as e:
            logger.error(f"Ошибка удаления админа {user_id}: {e}")
            return False
    
    async def get_admins(self) -> List[int]:
        """Получение списка ID администраторов"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM admins WHERE is_active = TRUE')
            return [row['user_id'] for row in rows]
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверка является ли пользователь администратором"""
        if user_id == OWNER_ID:
            return True
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM admins WHERE user_id = $1 AND is_active = TRUE)',
                user_id
            )
            return exists
    
    async def update_stats(self, **kwargs):
        """Обновление глобальной статистики"""
        async with self.pool.acquire() as conn:
            # Проверяем какие колонки существуют
            columns = await conn.fetchrow('SELECT * FROM stats WHERE id = 1')
            existing_columns = columns.keys() if columns else []
            
            # Фильтруем только существующие колонки
            valid_kwargs = {k: v for k, v in kwargs.items() if k in existing_columns}
            
            if valid_kwargs:
                set_clause = ', '.join([f"{k} = {k} + ${i+1}" for i, k in enumerate(valid_kwargs.keys())])
                set_clause += ", updated_at = CURRENT_TIMESTAMP"
                query = f'UPDATE stats SET {set_clause} WHERE id = 1'
                await conn.execute(query, *valid_kwargs.values())
    
    async def get_stats(self) -> Dict:
        """Получение глобальной статистики"""
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
        """Получение статистики по пользователям"""
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
    
    async def get_most_active_user(self) -> Optional[Dict]:
        """Получение самого активного пользователя"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM users 
                WHERE messages_sent > 0 
                ORDER BY messages_sent DESC 
                LIMIT 1
            ''')
            return dict(row) if row else None
    
    async def get_messages_stats(self) -> Dict:
        """Получение статистики по сообщениям"""
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM messages')
            answered = await conn.fetchval('SELECT COUNT(*) FROM messages WHERE is_answered = TRUE')
            return {
                'total': total,
                'answered': answered
            }
    
    async def close(self):
        """Закрытие соединения с БД"""
        if self.pool:
            await self.pool.close()

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
    
    async def notify_admins(self, message: str, exclude_user_id: int = None):
        """Отправка уведомления всем администраторам"""
        admins = await self.db.get_admins()
        for admin_id in admins:
            if exclude_user_id and admin_id == exclude_user_id:
                continue
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    
    def get_user_info(self, user_data: Dict) -> str:
        """Форматирование информации о пользователе"""
        if user_data.get('username'):
            return f"@{user_data['username']}"
        elif user_data.get('first_name') or user_data.get('last_name'):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        return f"ID: {user_data['user_id']}"
    
    async def save_user_from_message(self, message: Message):
        """Сохранение пользователя из сообщения"""
        user = message.from_user
        user_id = user.id
        await self.db.save_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
    
    async def send_reply_notification(self, user_id: int, message_id: int, answer_text: str, admin_name: str):
        """Отправка уведомления пользователю о ответе"""
        try:
            await self.bot.send_message(
                user_id,
                f"🔔 <b>Вам поступил ответ на сообщение #{message_id}</b>\n\n"
                f"{answer_text}\n\n"
                f"<i>Ответил: {admin_name}</i>"
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")
            return False
    
    async def forward_message_to_admins(self, message: Message, user_data: Dict, message_id: int):
        """Пересылка сообщения админам с ID"""
        # Определяем тип контента и готовим сообщение
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
            f"<i>Ответить можно в Mini App: нажми /app</i>"
        )
        
        admins = await self.db.get_admins()
        success_count = 0
        
        for admin_id in admins:
            try:
                # Отправляем текстовое уведомление
                await self.bot.send_message(admin_id, text)
                
                # Если есть медиа, отправляем отдельно
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
                logger.error(f"Ошибка отправки админу {admin_id}: {e}")
        
        return success_count
    
    def register_handlers(self):
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            await self.save_user_from_message(message)
            
            # Кнопка для открытия Mini App
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="📱 Открыть приложение", 
                        web_app=WebAppInfo(url=APP_URL)
                    )
                ]]
            )
            
            await message.answer(
                f"👋 <b>Привет, {message.from_user.first_name or 'пользователь'}!</b>\n\n"
                f"Это бот для обратной связи.\n\n"
                f"📱 <b>Нажми кнопку ниже, чтобы открыть приложение</b>\n"
                f"Там ты сможешь:\n"
                f"• Отправлять сообщения\n"
                f"• Смотреть историю переписки\n"
                f"• Получать уведомления об ответах\n\n"
                f"⏱ Лимит: {RATE_LIMIT_MINUTES} минут между сообщениями",
                reply_markup=keyboard
            )
            
            user_data = await self.db.get_user(message.from_user.id)
            await self.notify_admins(
                f"👤 <b>Новый пользователь:</b>\n"
                f"• {self.get_user_info(user_data)}\n"
                f"• ID: {message.from_user.id}",
                exclude_user_id=message.from_user.id
            )
        
        @self.router.message(Command("app"))
        async def cmd_app(message: Message):
            """Открыть Mini App"""
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
        
        @self.router.message(Command("help"))
        async def cmd_help(message: Message):
            if await self.db.is_admin(message.from_user.id):
                await message.answer(
                    "<b>Команды администратора:</b>\n\n"
                    "• /app - открыть Mini App\n"
                    "• /stats - статистика\n"
                    "• /users - список пользователей\n"
                    "• /ban ID причина - заблокировать\n"
                    "• /unban ID - разблокировать\n"
                    "• /admin - управление админами\n\n"
                    f"<i>Все операции лучше делать в Mini App</i>"
                )
            else:
                await message.answer(
                    "🤖 <b>Команды:</b>\n\n"
                    "• /start - начать работу\n"
                    "• /app - открыть приложение\n"
                    "• /help - помощь\n\n"
                    f"📱 <b>Используй Mini App для отправки сообщений</b>"
                )
        
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return
            
            stats = await self.db.get_stats()
            user_stats = await self.db.get_users_count()
            messages_stats = await self.db.get_messages_stats()
            most_active = await self.db.get_most_active_user()
            admins = await self.db.get_admins()
            
            text = (
                f"📊 <b>Статистика бота</b>\n\n"
                f"<b>Пользователи:</b>\n"
                f"• Всего: {user_stats['total']}\n"
                f"• Активных (24ч): {user_stats['active_today']}\n"
                f"• Заблокированных: {user_stats['banned']}\n"
                f"• Администраторов: {len(admins)}\n\n"
                f"<b>Сообщения:</b>\n"
                f"• Всего отправлено: {stats['total_messages']}\n"
                f"• Всего сообщений в БД: {messages_stats['total']}\n"
                f"• Отвеченных: {messages_stats['answered']}\n"
                f"• Успешно переслано: {stats['successful_forwards']}\n"
                f"• Ошибок при пересылке: {stats['failed_forwards']}\n"
                f"• Блокировок по лимиту: {stats['rate_limit_blocks']}\n"
                f"• Выдано банов: {stats['bans_issued']}\n"
            )
            
            if 'answers_sent' in stats:
                text += f"• Отправлено ответов: {stats['answers_sent']}\n"
            
            text += "\n"
            
            if most_active and most_active.get('messages_sent', 0) > 0:
                text += (
                    f"<b>Самый активный:</b>\n"
                    f"• {self.get_user_info(most_active)}\n"
                    f"• Сообщений: {most_active['messages_sent']}\n"
                    f"• Первое сообщение: {most_active['created_at'].strftime('%d.%m.%Y')}\n\n"
                )
            
            text += f"<i>Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</i>"
            await message.answer(text)
        
        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return
            
            users = await self.db.get_all_users()
            if not users:
                return await message.answer("📭 <b>Пользователей пока нет</b>")
            
            text = "👥 <b>Список пользователей:</b>\n\n"
            for i, user in enumerate(users[:50], 1):
                status = '🚫' if user.get('is_banned') else '✅'
                is_admin = await self.db.is_admin(user['user_id'])
                admin_star = '👑 ' if is_admin else ''
                
                # Получаем последнее сообщение пользователя
                last_msgs = await self.db.get_user_messages(user['user_id'], 1)
                last_msg_info = ""
                if last_msgs:
                    last_msg_info = f" | Посл. #{last_msgs[0]['message_id']}"
                
                text += f"{i}. {status} {admin_star}{self.get_user_info(user)} | ID: <code>{user['user_id']}</code> | Сообщений: {user.get('messages_sent', 0)}{last_msg_info}\n"
            
            if len(users) > 50:
                text += f"\n<i>Показано 50 из {len(users)} пользователей</i>"
            
            await message.answer(text)
        
        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    return await message.answer(
                        "❌ <b>Неверный формат команды</b>\n\n"
                        "Используйте: <code>/ban PEER_ID Причина [Время в часах]</code>"
                    )
                
                peer_id = int(args[0])
                
                if await self.db.is_admin(peer_id):
                    return await message.answer("❌ Нельзя заблокировать администратора.")
                
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    return await message.answer(
                        f"❌ Время бана должно быть от 1 до {MAX_BAN_HOURS} часов"
                    )
                
                try:
                    user = await self.bot.get_chat(peer_id)
                    await self.db.save_user(
                        user_id=peer_id,
                        username=user.username,
                        first_name=user.first_name,
                        last_name=user.last_name
                    )
                except:
                    await self.db.save_user(user_id=peer_id)
                
                ban_until = datetime.now() + timedelta(hours=hours) if hours else None
                await self.db.ban_user(peer_id, reason, ban_until)
                await self.db.update_stats(bans_issued=1)
                
                ban_duration = f"на {hours} часов" if hours else "навсегда"
                user_data = await self.db.get_user(peer_id)
                
                await message.answer(
                    f"✅ <b>Пользователь заблокирован</b>\n\n"
                    f"<b>Информация:</b> {self.get_user_info(user_data)}\n"
                    f"<b>ID:</b> <code>{peer_id}</code>\n"
                    f"<b>Причина:</b> {reason}\n"
                    f"<b>Длительность:</b> {ban_duration}"
                )
                
            except ValueError:
                await message.answer("❌ Неверный формат Peer ID")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {str(e)}")
        
        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    return await message.answer("❌ Используйте: <code>/unban PEER_ID</code>")
                
                peer_id = int(args[0])
                user_data = await self.db.get_user(peer_id)
                
                if user_data and user_data.get('is_banned'):
                    await self.db.unban_user(peer_id)
                    
                    await message.answer(
                        f"✅ <b>Пользователь разблокирован</b>\n\n"
                        f"<b>Информация:</b> {self.get_user_info(user_data)}\n"
                        f"<b>ID:</b> <code>{peer_id}</code>"
                    )
                else:
                    await message.answer(f"ℹ️ Пользователь {peer_id} не заблокирован")
            
            except ValueError:
                await message.answer("❌ Неверный формат Peer ID")
        
        @self.router.message(Command("admin"))
        async def cmd_admin(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            text = message.text.split()
            if len(text) == 1:
                await message.answer(
                    "👑 <b>Управление администраторами</b>\n\n"
                    "• <code>/admin add ID</code> - добавить\n"
                    "• <code>/admin remove ID</code> - удалить\n"
                    "• <code>/admin list</code> - список"
                )
            elif len(text) >= 3:
                action = text[1].lower()
                try:
                    target_id = int(text[2])
                    
                    if action == "add":
                        if target_id == OWNER_ID:
                            return await message.answer("👑 Владелец уже является администратором.")
                        
                        if await self.db.add_admin(target_id, message.from_user.id):
                            await message.answer(f"✅ Администратор {target_id} добавлен")
                        else:
                            await message.answer("❌ Не удалось добавить администратора.")
                    
                    elif action == "remove":
                        if target_id == OWNER_ID:
                            return await message.answer("❌ Нельзя удалить владельца бота.")
                        
                        if await self.db.remove_admin(target_id):
                            await message.answer(f"✅ Администратор {target_id} удален")
                        else:
                            await message.answer("❌ Не удалось удалить администратора.")
                
                except ValueError:
                    await message.answer("❌ Неверный формат ID.")
            
            elif len(text) == 2 and text[1].lower() == "list":
                admins = await self.db.get_admins()
                admin_list_text = "👑 <b>Список администраторов:</b>\n\n"
                
                for i, admin_id in enumerate(admins, 1):
                    user_data = await self.db.get_user(admin_id) or {}
                    if admin_id == OWNER_ID:
                        admin_list_text += f"{i}. 👑 {self.get_user_info(user_data)} | <code>{admin_id}</code> (владелец)\n"
                    else:
                        admin_list_text += f"{i}. {self.get_user_info(user_data)} | <code>{admin_id}</code>\n"
                
                await message.answer(admin_list_text)
        
        @self.router.message(F.web_app_data)
        async def handle_web_app_data(message: Message):
            """Обработка данных из Mini App"""
            data = message.web_app_data.data
            try:
                payload = json.loads(data)
                action = payload.get('action')
                user_id = message.from_user.id
                
                if action == 'send_message':
                    # Пользователь отправил сообщение через Mini App
                    text = payload.get('text', '')
                    
                    # Создаем временный объект сообщения для обработки
                    message.text = text
                    await self.process_user_message(message, from_webapp=True)
                    
                elif action == 'reply_message':
                    # Админ отвечает на сообщение через Mini App
                    if not await self.db.is_admin(user_id):
                        return
                    
                    message_id = payload.get('message_id')
                    answer_text = payload.get('answer')
                    
                    await self.process_admin_reply(message_id, answer_text, user_id)
                    
            except Exception as e:
                logger.error(f"Ошибка обработки WebApp данных: {e}")
        
        @self.router.message()
        async def handle_message(message: Message):
            """Обработка обычных сообщений"""
            await self.process_user_message(message, from_webapp=False)
    
    async def process_user_message(self, message: Message, from_webapp: bool = False):
        """Обработка сообщения от пользователя"""
        user_id = message.from_user.id
        
        # Сохраняем пользователя
        await self.save_user_from_message(message)
        
        # Проверяем бан
        user_data = await self.db.get_user(user_id)
        if user_data and user_data.get('is_banned'):
            ban_until = user_data.get('ban_until')
            if ban_until and datetime.now() > ban_until:
                await self.db.unban_user(user_id)
            else:
                ban_info = f"до {ban_until.strftime('%d.%m.%Y %H:%M')}" if ban_until else "навсегда"
                if not from_webapp:
                    await message.answer(
                        f"🚫 <b>Вы заблокированы {ban_info}</b>\n"
                        f"Причина: {user_data.get('ban_reason', 'Не указана')}"
                    )
                return
        
        # Проверяем лимит для обычных пользователей
        if not await self.db.is_admin(user_id):
            if user_data and user_data.get('last_message_time'):
                last_time = user_data['last_message_time']
                if hasattr(last_time, 'tzinfo'):
                    last_time = last_time.replace(tzinfo=None)
                
                time_diff = (datetime.now() - last_time).total_seconds() / 60
                if time_diff < RATE_LIMIT_MINUTES:
                    remaining = RATE_LIMIT_MINUTES - int(time_diff)
                    await self.db.update_stats(rate_limit_blocks=1)
                    if not from_webapp:
                        await message.answer(
                            f"⏳ <b>Подождите {remaining} минут</b>\n\n"
                            f"Вы можете отправить только одно сообщение за {RATE_LIMIT_MINUTES} минут."
                        )
                    return
        
        try:
            # Получаем следующий ID
            message_id = await self.db.get_next_message_id()
            
            # Сохраняем сообщение
            content_type = str(message.content_type)
            file_id = None
            text = message.text if message.text else None
            caption = message.caption if message.caption else None
            
            if message.photo:
                file_id = message.photo[-1].file_id
            elif message.video:
                file_id = message.video.file_id
            elif message.voice:
                file_id = message.voice.file_id
            elif message.document:
                file_id = message.document.file_id
            elif message.sticker:
                file_id = message.sticker.file_id
            
            await self.db.save_message(
                message_id=message_id,
                user_id=user_id,
                content_type=content_type,
                file_id=file_id,
                caption=caption,
                text=text
            )
            
            # Пересылаем админам
            user_data = await self.db.get_user(user_id)
            success_count = await self.forward_message_to_admins(message, user_data, message_id)
            
            if success_count > 0:
                # Обновляем статистику
                await self.db.update_user_last_message(user_id, datetime.now())
                await self.db.update_user_stats(user_id, increment_messages=True)
                await self.db.update_stats(
                    total_messages=1,
                    successful_forwards=success_count
                )
                
                # Подтверждение пользователю
                if not from_webapp:
                    await message.answer(
                        f"✅ <b>Сообщение #{message_id} отправлено!</b>\n\n"
                        f"🔔 Ответ придет в этот чат с уведомлением"
                    )
                
                logger.info(f"Сообщение #{message_id} от {user_id} переслано {success_count} админам")
            else:
                raise Exception("Нет доступных администраторов")
                
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await self.db.update_stats(failed_forwards=1)
            if not from_webapp:
                await message.answer("❌ Не удалось отправить сообщение. Попробуйте позже.")
    
    async def process_admin_reply(self, message_id: int, answer_text: str, admin_id: int):
        """Обработка ответа админа на сообщение"""
        try:
            # Получаем исходное сообщение
            original = await self.db.get_message(message_id)
            if not original:
                return
            
            user_id = original['user_id']
            admin_data = await self.db.get_user(admin_id)
            admin_name = self.get_user_info(admin_data) if admin_data else f"ID: {admin_id}"
            
            # Отправляем уведомление пользователю
            success = await self.send_reply_notification(user_id, message_id, answer_text, admin_name)
            
            if success:
                # Отмечаем как отвеченное
                await self.db.mark_message_answered(message_id, admin_id)
                await self.db.update_stats(answers_sent=1)
                
                # Уведомляем других админов
                user_info = await self.db.get_user(user_id)
                await self.notify_admins(
                    f"💬 <b>Админ ответил на #{message_id}</b>\n"
                    f"Пользователь: {self.get_user_info(user_info)}\n"
                    f"Админ: {admin_name}",
                    exclude_user_id=admin_id
                )
        except Exception as e:
            logger.error(f"Ошибка ответа: {e}")
    
    async def start_keep_alive_server(self):
        """Запуск keep-alive сервера"""
        app = create_keep_alive_server(KEEP_ALIVE_PORT)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', KEEP_ALIVE_PORT).start()
        logger.info(f"✅ Keep-alive сервер на порту {KEEP_ALIVE_PORT}")
        return runner
    
    async def shutdown(self, sig=None):
        """Грациозное завершение работы"""
        logger.info(f"Сигнал {sig}, завершение...")
        self.is_running = False
        await self.dp.stop_polling()
        await self.bot.session.close()
        await self.db.close()
    
    async def run(self):
        """Запуск бота"""
        runner = None
        try:
            if sys.platform != 'win32':
                loop = asyncio.get_running_loop()
                for sig in [signal.SIGTERM, signal.SIGINT]:
                    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.shutdown(s)))
            
            runner = await self.start_keep_alive_server()
            await self.bot.delete_webhook(drop_pending_updates=True)
            
            logger.info("🤖 Бот запущен")
            logger.info(f"👑 Владелец: {OWNER_ID}")
            logger.info(f"📱 Mini App URL: {APP_URL}")
            
            while self.is_running:
                try:
                    await self.dp.start_polling(self.bot)
                except Exception as e:
                    logger.error(f"Polling error: {e}")
                    if self.is_running:
                        await asyncio.sleep(5)
        finally:
            await self.bot.session.close()
            await self.db.close()
            if runner:
                await runner.cleanup()

def main():
    """Точка входа"""
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        logger.error("❌ Нет BOT_TOKEN")
        return
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logger.error("❌ Нет DATABASE_URL")
        return
    
    async def run_bot():
        db = Database(DATABASE_URL)
        await db.create_pool()
        bot = MessageForwardingBot(BOT_TOKEN, db)
        await bot.run()
    
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")

if __name__ == "__main__":
    main()