import telebot
import sqlite3
import csv
import io
import re
from datetime import datetime, timedelta
from telebot.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
import threading
import time
import secrets
import string
from typing import Optional, List
# Added robust HTTP session/retry imports
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import telebot.apihelper as apihelper

# === SOZLAMALAR ===
BOT_TOKEN = '8231571160:AAHgw1Dqrb4oAYN2euwq8OO20iC0tpmoBcI'
ADMIN_SECRET = 'ddosmaster2025'
MOD_SECRET = 'mod2025'
ANNOUNCE_CHANNEL = None

SUB_REQUIRE_MODE = 'all'
SUB_REQUIRE_COUNT = 1
DAILY_GLOBAL_ISSUE_LIMIT = 1000

CHANNEL_USERNAMES = [
    '@muhammadaliaiblog',
]

# Simple referral milestone rewards
REFERRAL_REWARD_THRESHOLDS = [3, 5, 10]
BONUS_REWARD_TEXTS = {
    3: "Premium mini‑PROM: 10 приемов для повышения продуктивности за 15 минут.",
    5: "Expert mini‑PROM: Чек‑лист улучшения текста (ясность, структура, тон).",
    10: "Master mini‑PROM: Фреймворк решений: цель → альтернативы → риски → критерии.",
}

# Configure a resilient HTTP session for Telegram API before creating the bot
try:
    session = requests.Session()
    try:
        retries = Retry(
            total=5,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]),
        )
    except TypeError:
        # For older urllib3 versions that use method_whitelist
        retries = Retry(
            total=5,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    apihelper.SESSION = session
    apihelper.CONNECT_TIMEOUT = 10
    apihelper.READ_TIMEOUT = 55
except Exception:
    # Fail silently; polling wrapper below will still handle restarts
    pass

def channel_link(username: str) -> str:
    return f'https://t.me/{username.lstrip("@")}'

bot = telebot.TeleBot(BOT_TOKEN)

# Cached bot username
_BOT_USERNAME_CACHE = None

def get_bot_username() -> str:
    global _BOT_USERNAME_CACHE
    if _BOT_USERNAME_CACHE:
        return _BOT_USERNAME_CACHE
    try:
        me = bot.get_me()
        _BOT_USERNAME_CACHE = getattr(me, 'username', None)
    except Exception:
        _BOT_USERNAME_CACHE = None
    return _BOT_USERNAME_CACHE or ''

# === MA'LUMOTLAR BAZASI ===
conn_lock = threading.Lock()
conn = sqlite3.connect('data.db', check_same_thread=False)
c = conn.cursor()

with conn_lock:
    c.execute('''CREATE TABLE IF NOT EXISTS proms (
        id TEXT PRIMARY KEY,
        content TEXT,
        used INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS admins (
        user_id INTEGER PRIMARY KEY
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        received_id TEXT,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()

# === MIGRATSIYALAR: yangi ustun va jadvallar ===
with conn_lock:
    try:
        c.execute("ALTER TABLE users ADD COLUMN joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        c.execute("UPDATE users SET joined_at = CURRENT_TIMESTAMP WHERE joined_at IS NULL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE users ADD COLUMN lang TEXT DEFAULT 'uz'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE proms ADD COLUMN category_id INTEGER")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE proms ADD COLUMN deleted INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE proms ADD COLUMN expires_at TIMESTAMP")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE users ADD COLUMN referrer_id INTEGER")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE users ADD COLUMN subscribed_at TIMESTAMP")
    except Exception:
        pass
    c.execute('''CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_prom_history (
        user_id INTEGER,
        prom_id TEXT,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS moderators (
        user_id INTEGER PRIMARY KEY
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS referrals (
        referrer_id INTEGER,
        referred_id INTEGER UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    conn.commit()

# === I18N (RU/UZ) ===
I18N = {
    'menu_title': {'uz': "📍 Asosiy menyu:", 'ru': "📍 Главное меню:"},
    'admin_menu_title': {'uz': "🔧 Admin menyu:", 'ru': "🔧 Админ-меню:"},
    'btn_lang': {'uz': "🌐 Til / Язык", 'ru': "🌐 Язык / Til"},
    'btn_get_by_id': {'uz': "🎁 PROM olish", 'ru': "🎁 Получить PROM"},
    'btn_help': {'uz': "ℹ️ Yordam", 'ru': "ℹ️ Помощь"},
    'btn_my_prom': {'uz': "📜 Mening PROMim", 'ru': "📜 Мой PROM"},
    'btn_stats': {'uz': "📊 Statistika", 'ru': "📊 Статистика"},
    'btn_refresh': {'uz': "🔄 PROM yangilash", 'ru': "🔄 Обновить PROM"},
    'btn_by_category': {'uz': "🎯 Kategoriya bo'yicha PROM", 'ru': "🎯 PROM по категории"},
    'btn_history': {'uz': "🕘 Tarix", 'ru': "🕘 История"},
    'start_hello': {'uz': "👋 Salom! GPT PROM-botga xush kelibsiz.", 'ru': "👋 Привет! Добро пожаловать в GPT PROM-бот."},
    'help_text': {
        'uz': "💬 PROM ID ni kiriting va foydalaning.\nAdminlar PROMlarni menyu orqali boshqaradi.",
        'ru': "💬 Введите ID PROM-а, чтобы получить доступ. Админы управляют PROM-ами через меню."
    },
    'prompt_enter_prom_id': {'uz': "🔐 Olingan PROM ID ni kiriting:", 'ru': "🔐 Введите ID PROM-а:"},
    'invalid_prom_id': {'uz': "❌ Noto'g'ri yoki ishlatilgan PROM ID.", 'ru': "❌ Неверный или уже использованный ID PROM-а."},
    'your_prom': {'uz': "✅ Sizning PROMingiz:", 'ru': "✅ Ваш PROM:"},
    'not_received_yet': {'uz': "❗ Siz hali PROM olmadingiz.", 'ru': "❗ Вы еще не получали PROM."},
    'not_found_or_deleted': {'uz': "❗ PROM topilmadi yoki o'chirilgan.", 'ru': "❗ PROM не найден или удалён."},
    'stats_title': {'uz': "📊 Statistika:", 'ru': "📊 Статистика:"},
    'stats_users': {'uz': "👥 Foydalanuvchilar:", 'ru': "👥 Пользователей:"},
    'stats_proms': {'uz': "🧾 PROMlar:", 'ru': "🧾 PROM-ов:"},
    'stats_issued': {'uz': "✅ Berilgan:", 'ru': "✅ Выдано:"},
    'stats_admins': {'uz': "👑 Adminlar:", 'ru': "👑 Админов:"},
    'sub_required_title': {
        'uz': "📢 Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling, so'ng 'Tekshirish'ni bosing:",
        'ru': "📢 Чтобы пользоваться ботом, подпишитесь на каналы ниже, затем нажмите 'Проверить':"
    },
    'btn_subscribe_check': {'uz': "🔄 Tekshirish", 'ru': "🔄 Проверить"},
    'btn_subscribe_to': {'uz': "✅ {name} ga obuna bo'lish", 'ru': "✅ Подписаться на {name}"},
    'sub_checked_ok': {'uz': "✅ Obuna tasdiqlandi!", 'ru': "✅ Подписка подтверждена!"},
    'sub_checked_fail': {'uz': "❌ Hali obuna bo'lmadingiz.", 'ru': "❌ Вы ещё не подписались."},
    'by_category_choose': {'uz': "🎯 Kategoriya tanlang:", 'ru': "🎯 Выберите категорию:"},
    'no_categories': {'uz': "📭 Kategoriyalar yo'q.", 'ru': "📭 Нет категорий."},
    'no_free_in_cat': {'uz': "📭 Bu kategoriyada mavjud PROM yo'q.", 'ru': "📭 В этой категории нет доступных PROM-ов."},
    'history_title': {'uz': "🕘 Tarix:", 'ru': "🕘 История:"},
    'empty_history': {'uz': "📭 Tarix bo'sh.", 'ru': "📭 История пуста."},
    'btn_prev': {'uz': "⬅️ Orqaga", 'ru': "⬅️ Назад"},
    'btn_next': {'uz': "➡️ Keyingi", 'ru': "➡️ Вперед"},
    'btn_hide': {'uz': "👁️ Yashirish", 'ru': "👁️ Скрыть"},
    'btn_show': {'uz': "👁️ Ko'rsatish", 'ru': "👁️ Показать"},
    'btn_copy': {'uz': "📋 Nusxalash", 'ru': "📋 Копировать"},
    'btn_referral': {'uz': "🔗 Referal havola", 'ru': "🔗 Реферальная ссылка"},
    'referral_text': {
        'uz': "🔗 Sizning referal havolangiz:\n{link}\n\n👥 Referallar: {count} | ✅ Konversiyalar: {conv}",
        'ru': "🔗 Ваша реферальная ссылка:\n{link}\n\n👥 Рефералов: {count} | ✅ Конверсий: {conv}"
    },
    'btn_open_link': {'uz': "🔗 Havolani ochish", 'ru': "🔗 Открыть ссылку"},
    'btn_share_text': {'uz': "📣 Ulashish matni", 'ru': "📣 Текст для рассылки"},
    'share_text': {
        'uz': "Men foydali PROM-lar olaman bu bot orqali! Qoshiling va PROM oling: {link}",
        'ru': "Я беру полезные PROM в этом боте! Заходи и получай свой PROM: {link}"
    },
    'btn_top_today': {'uz': "🏆 Bugun TOP", 'ru': "🏆 Топ сегодня"},
    'top_today_title': {'uz': "🏆 Bugungi TOP referlar:", 'ru': "🏆 Топ рефералов за сегодня:"},
    'no_top': {'uz': "📭 Hali ma'lumot yo'q.", 'ru': "📭 Пока нет данных."},
    'reward_unlocked': {
        'uz': "🎉 Tabriklaymiz! Siz {n} konversiyaga yetdingiz va bonus PROM oldingiz:\n\n{content}",
        'ru': "🎉 Поздравляем! Вы достигли {n} конверсий и получили бонусный PROM:\n\n{content}"
    },
    'already_have_prom': {'uz': "❗ Sizda allaqachon PROM bor. Har bir foydalanuvchiga faqat bitta.", 'ru': "❗ У вас уже есть PROM. Один на пользователя."},
    'daily_user_limit': {'uz': "❗ Kunlik limitga yetdingiz.", 'ru': "❗ Достигнут дневной лимит."},
    'daily_global_limit': {'uz': "❗ Bugungi kunda berish limiti tugadi.", 'ru': "❗ Исчерпан дневной лимит выдач."},
    'csv_prompt': {'uz': "📥 CSV import/eksport: tugmani tanlang.", 'ru': "📥 CSV импорт/экспорт: выберите действие."},
    'btn_csv_import': {'uz': "⬆️ Import CSV", 'ru': "⬆️ Импорт CSV"},
    'btn_csv_export': {'uz': "⬇️ Export CSV", 'ru': "⬇️ Экспорт CSV"},
    'btn_csv_template': {'uz': "📄 Shablon", 'ru': "📄 Шаблон"},
    'send_csv_file': {'uz': "📎 CSV fayl jo'nating (ustunlar: id,content,expires_at?,category_id?) yoki matn sifatida yuboring.", 'ru': "📎 Пришлите CSV-файл (столбцы: id,content,expires_at?,category_id?) или отправьте как текст."},
    'csv_import_ok': {'uz': "✅ CSV import yakunlandi: {n} ta yozuv.", 'ru': "✅ Импорт CSV завершен: {n} записей."},
    'csv_export_title': {'uz': "📦 PROM CSV eksport", 'ru': "📦 Экспорт PROM CSV"},
    'not_owner_restore': {'uz': "♻️ PROM qaytarildi: endi foydalanilmagan.", 'ru': "♻️ PROM возвращён: теперь неиспользованный."},
    'soft_deleted': {'uz': "🗑 Yumshoq o'chirildi.", 'ru': "🗑 Мягко удалён."},
    'soft_undeleted': {'uz': "♻️ Qayta tiklandi.", 'ru': "♻️ Восстановлен."},
    'top_refs': {'uz': "🏆 Top referallar:", 'ru': "🏆 Топ рефералов:"},
}

def generate_prom_id(prefix: str = 'SEC', length: int = 8) -> str:
    """Generate a random PROM ID like 'SEC-AB12CD34'."""
    alphabet = string.ascii_uppercase + string.digits
    random_part = ''.join(secrets.choice(alphabet) for _ in range(length))
    return f"{prefix}-{random_part}"

def get_or_create_category(name: str) -> int:
    with conn_lock:
        c.execute("SELECT id FROM categories WHERE name = ?", (name,))
        row = c.fetchone()
        if row:
            return row[0]
        c.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        return c.lastrowid

# Curated, high-quality prompts (safe, no jailbreaks)
SECRET_PROMPTS = [
    "Ты экспертный помощник по продуктивности. Дай структурированные, краткие, практичные ответы. Если данных мало — задай 3 уточняющих вопроса.",
    "Ты строгий редактор текста. Улучши ясность, структуру и стиль. Сохрани смысл. Дай итоговую версию и 3 ключевых правки.",
    "Ты аналитик. Сформулируй проблему, гипотезы, критерии успеха, риски и план на 3 шага. Верни ответ в виде маркированного списка.",
    "Ты преподаватель. Объясни тему простыми словами, приведи 2 аналогии и 1 мини-задачу для самопроверки (с ответом).",
    "Ты инженер по требованиям. Извлеки из текста: акторов, цели, функциональные и нефункциональные требования, ограничения. Верни JSON-структуру.",
    "Ты помощник по исследованиям. Сформируй 5 уточняющих вопросов, затем краткий обзор (150–200 слов) и список из 5 источников (если известны).",
    "Ты наставник по коду. Опиши потенциальные ошибки, сложность (Big-O), юзкейсы и тест-кейсы. Предложи улучшенную версию функции.",
    "Ты консультант по решению задач. Разбей задачу на подцели, предложи 2 альтернативных подхода и критерии выбора.",
    "Ты менеджер проекта. Составь чек-лист, таймлайн на 2 недели и риски с митигациями. Верни в табличном виде (markdown).",
    "Ты ассистент по коммуникациям. Сконструируй вежливое письмо: цель, контекст, просьба, дедлайн, следующий шаг. 2 тона: формальный и нейтральный."
]

def get_lang(user_id: int) -> str:
    with conn_lock:
        c.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row and row[0] in ('uz', 'ru'):
            return row[0]
    return 'uz'

def set_lang(user_id: int, lang: str):
    if lang not in ('uz', 'ru'):
        return
    with conn_lock:
        c.execute("UPDATE users SET lang = ? WHERE user_id = ?", (lang, user_id))
        conn.commit()

def tr(key: str, lang: str) -> str:
    return I18N.get(key, {}).get(lang, I18N.get(key, {}).get('uz', key))

# === SETTINGS HELPERS ===
def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    with conn_lock:
        c.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = c.fetchone()
        return (row[0] if row else default)

def set_setting(key: str, value: Optional[str]):
    with conn_lock:
        if value is None:
            c.execute("DELETE FROM settings WHERE key = ?", (key,))
        else:
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()

def get_announce_channel() -> Optional[str]:
    # Prefer runtime override; fallback to DB setting
    return ANNOUNCE_CHANNEL or get_setting('announce_channel')

def build_bot_deeplink(for_user_id: Optional[int] = None) -> str:
    username = get_bot_username()
    if not username:
        return "https://t.me/"  # fallback
    if for_user_id:
        return f"https://t.me/{username}?start=ref_{for_user_id}"
    return f"https://t.me/{username}"

def post_secret_announce(ids: List[str], inv_user_id: Optional[int] = None) -> bool:
    """Post nicely formatted IDs to the announce channel, if configured. Returns True on success."""
    channel = get_announce_channel()
    if not channel:
        return False
    # Build text
    header = "🔒 10 ta SECRET PROM yaratildi (har biri 1 marta ishlatiladi):"
    ids_text = "\n".join(ids)
    footer = "\n\n👉 PROM olish uchun botga o'ting va ID ni kiriting."
    text = f"{header}\n{ids_text}{footer}"
    # Button to go to bot with ref link
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton(text="🤖 Botga o'tish", url=build_bot_deeplink(inv_user_id)))
    try:
        bot.send_message(channel, text, reply_markup=kb, disable_web_page_preview=True)
        return True
    except Exception:
        return False

# === OBUNANI TEKSHIRISH ===
def is_subscribed_to(username: str, user_id: int) -> bool:
    try:
        member = bot.get_chat_member(username, user_id)
        status = getattr(member, 'status', None)
        return status in ('member', 'administrator', 'creator')
    except Exception:
        return False


def is_subscribed(user_id: int) -> bool:
    if not CHANNEL_USERNAMES:
        return True
    statuses = [is_subscribed_to(u, user_id) for u in CHANNEL_USERNAMES]
    if SUB_REQUIRE_MODE == 'all':
        return all(statuses)
    if SUB_REQUIRE_MODE == 'any':
        return any(statuses)
    if SUB_REQUIRE_MODE == 'at_least':
        return sum(1 for s in statuses if s) >= max(1, SUB_REQUIRE_COUNT)
    return all(statuses)


def send_subscribe_prompt(chat_id: int, lang: str = 'uz'):
    text_lines = [tr('sub_required_title', lang)]
    kb = InlineKeyboardMarkup()
    for username in CHANNEL_USERNAMES:
        text_lines.append(f"• {username}")
        kb.row(InlineKeyboardButton(text=tr('btn_subscribe_to', lang).format(name=username), url=channel_link(username)))
    kb.row(InlineKeyboardButton(text=tr('btn_subscribe_check', lang), callback_data="check_sub"))
    bot.send_message(chat_id, "\n".join(text_lines), reply_markup=kb, disable_web_page_preview=True)


def ensure_subscription(message: Message) -> bool:
    user_id = message.from_user.id
    if is_admin(user_id) or is_moderator(user_id):
        return True
    if is_subscribed(user_id):
        return True
    send_subscribe_prompt(message.chat.id, get_lang(user_id))
    return False

# === MENYU ===
def send_main_menu(chat_id: int, user_id: int = None):
    lang = get_lang(user_id) if user_id else 'uz'
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(KeyboardButton(tr('btn_lang', lang)))
    markup.row(KeyboardButton(tr('btn_get_by_id', lang)), KeyboardButton(tr('btn_help', lang)))
    markup.row(KeyboardButton(tr('btn_my_prom', lang)), KeyboardButton(tr('btn_stats', lang)))
    markup.row(KeyboardButton(tr('btn_refresh', lang)))
    markup.row(KeyboardButton(tr('btn_by_category', lang)), KeyboardButton(tr('btn_history', lang)))
    markup.row(KeyboardButton(tr('btn_referral', lang)))
    # If user has elevated rights, provide a shortcut back to admin menu
    if user_id and (is_admin(user_id) or is_moderator(user_id)):
        markup.row(KeyboardButton("🔧 Admin menyu"))
    bot.send_message(chat_id, tr('menu_title', lang), reply_markup=markup)

def send_admin_menu(chat_id):
    markup = ReplyKeyboardMarkup(resize_keyboard=True)
    # Admin actions
    markup.row(KeyboardButton("➕ PROM qo'shish"), KeyboardButton("📋 PROMlar ro'yxati"))
    markup.row(KeyboardButton("👤 Foydalanuvchilar ro'yxati"), KeyboardButton("📤 Xabar yuborish"))
    markup.row(KeyboardButton("🗑 PROM o'chirish"), KeyboardButton("🔔 Barchaga bildirishnoma"))
    markup.row(KeyboardButton("🔍 PROM qidirish"), KeyboardButton("📝 PROM tahrirlash"))
    markup.row(KeyboardButton("📥 CSV import/eksport"))
    # User functions inside admin menu
    markup.row(KeyboardButton("🌐 Til / Язык"), KeyboardButton("🎁 PROM olish"))
    markup.row(KeyboardButton("📜 Mening PROMim"), KeyboardButton("📊 Statistika"))
    markup.row(KeyboardButton("🔄 PROM yangilash"))
    # Controls
    markup.row(KeyboardButton("↩️ Foydalanuvchi menyusi"), KeyboardButton("🚪 Adminlikdan chiqish"))
    bot.send_message(chat_id, "🔧 Admin menyu:", reply_markup=markup)

@bot.message_handler(func=lambda msg: msg.text == "🌐 Til / Язык")
def handle_language_switch(message: Message):
    user_id = message.from_user.id
    current = get_lang(user_id)
    new_lang = 'ru' if current == 'uz' else 'uz'
    set_lang(user_id, new_lang)
    send_main_menu(message.chat.id, user_id)

def is_admin(user_id):
    with conn_lock:
        c.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
        return c.fetchone() is not None

def is_moderator(user_id):
    with conn_lock:
        c.execute("SELECT 1 FROM moderators WHERE user_id = ?", (user_id,))
        return c.fetchone() is not None

def has_moderator_rights(user_id):
    return is_admin(user_id) or is_moderator(user_id)

@bot.message_handler(commands=['start'])
def handle_start(message: Message):
    user_id = message.from_user.id
    with conn_lock:
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        c.execute("SELECT * FROM admins WHERE user_id = ?", (user_id,))
        if c.fetchone():
            send_admin_menu(message.chat.id)
            return
        # Deep-link referral: /start ref_123 or /start ref-123
        try:
            parts = (message.text or '').strip().split(' ', 1)
            if len(parts) == 2 and parts[1].startswith(('ref_', 'ref-')):
                raw = parts[1].split('_', 1)[-1].split('-', 1)[-1]
                ref_id = int(re.sub(r'\D', '', raw))
                if ref_id and ref_id != user_id:
                    c.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
                    row = c.fetchone()
                    if not row or not row[0]:
                        c.execute("UPDATE users SET referrer_id = ? WHERE user_id = ?", (ref_id, user_id))
                    c.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (ref_id, user_id))
                    conn.commit()
        except Exception:
            pass
    if not ensure_subscription(message):
        return
    lang = get_lang(user_id)
    bot.send_message(message.chat.id, I18N['start_hello'][lang])
    send_main_menu(message.chat.id, user_id)

@bot.message_handler(func=lambda msg: msg.text in ["🎁 PROM olish", "🎁 Получить PROM"])
def request_prom_id(message: Message):
    if not ensure_subscription(message):
        return
    lang = get_lang(message.from_user.id)
    bot.send_message(message.chat.id, I18N['prompt_enter_prom_id'][lang], reply_markup=ReplyKeyboardRemove())
    bot.register_next_step_handler(message, process_prom_id)

@bot.message_handler(func=lambda msg: msg.text == "🔄 PROM yangilash")
def refresh_prom(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    lang = get_lang(user_id)
    with conn_lock:
        c.execute("SELECT received_id FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if not row or not row[0]:
            bot.send_message(message.chat.id, I18N['not_received_yet'][lang])
            return
        prom_id = row[0]
        c.execute("SELECT content FROM proms WHERE id = ?", (prom_id,))
        prom = c.fetchone()
        if prom:
            bot.send_message(message.chat.id, f"♻️ {I18N['your_prom'][lang]}\n\n{prom[0]}")
        else:
            bot.send_message(message.chat.id, I18N['not_found_or_deleted'][lang])

@bot.message_handler(func=lambda msg: msg.text in ["📜 Mening PROMim", "📜 Мой PROM"])
def user_prom(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    with conn_lock:
        c.execute("SELECT received_id FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row and row[0]:
            prom_id = row[0]
            c.execute("SELECT content FROM proms WHERE id = ?", (prom_id,))
            prom = c.fetchone()
            if prom:
                bot.send_message(message.chat.id, f"📜 Sizning PROMingiz: {prom[0]}")
            else:
                bot.send_message(message.chat.id, "❗ PROM topilmadi yoki o'chirilgan.")
        else:
            bot.send_message(message.chat.id, "❗ Siz hali PROM olmadingiz.")

@bot.message_handler(func=lambda msg: msg.text in ["📊 Statistika", "📊 Статистика"])
def show_stats(message: Message):
    if not ensure_subscription(message):
        return
    with conn_lock:
        c.execute("SELECT COUNT(*) FROM users")
        users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM proms")
        total_proms = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM proms WHERE used = 1")
        used_proms = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM admins")
        admins = c.fetchone()[0]
        # referrals and conversions
        c.execute("SELECT COUNT(*) FROM referrals")
        total_referrals = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE subscribed_at IS NOT NULL")
        conversions = c.fetchone()[0]
        # issued today
        c.execute("SELECT COUNT(*) FROM user_prom_history WHERE DATE(received_at) = DATE('now')")
        issued_today = c.fetchone()[0]
        # daily joins last 7 days
        c.execute("SELECT DATE(COALESCE(joined_at, '1970-01-01')), COUNT(*) FROM users WHERE DATE(COALESCE(joined_at, '1970-01-01')) >= DATE('now','-6 day') GROUP BY DATE(COALESCE(joined_at, '1970-01-01')) ORDER BY DATE(COALESCE(joined_at, '1970-01-01'))")
        daily_rows = c.fetchall()
        daily_summary = ", ".join(f"{d}: {n}" for d, n in daily_rows)
        # top referrers
        c.execute("SELECT referrer_id, COUNT(*) as cnt FROM referrals GROUP BY referrer_id ORDER BY cnt DESC LIMIT 5")
        top_refs = c.fetchall()
    lang = get_lang(message.from_user.id)
    extra = "\n" + tr('top_refs', lang) + "\n" + "\n".join([f"👤 {rid}: {cnt}" for rid, cnt in top_refs]) if top_refs else ""
    bot.send_message(
        message.chat.id,
        f"{tr('stats_title', lang)}\n"
        f"{tr('stats_users', lang)} {users}\n"
        f"{tr('stats_proms', lang)} {total_proms}\n"
        f"{tr('stats_issued', lang)} {used_proms} (bugun: {issued_today})\n"
        f"{tr('stats_admins', lang)} {admins}\n"
        f"Referrals: {total_referrals}, Conversions: {conversions}\n"
        f"7 kunlik qo'shilish: {daily_summary}" + extra
    )

@bot.message_handler(func=lambda msg: msg.text in ["ℹ️ Yordam", "ℹ️ Помощь"])
def help_menu(message: Message):
    if not ensure_subscription(message):
        return
    lang = get_lang(message.from_user.id)
    bot.send_message(message.chat.id, I18N['help_text'][lang])

@bot.message_handler(func=lambda msg: msg.text in ["🎯 Kategoriya bo'yicha PROM", "🎯 PROM по категории"])
def handle_by_category(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    lang = get_lang(user_id)
    # Build categories list with available free proms count
    with conn_lock:
        c.execute(
            """
            SELECT c.id, c.name, COUNT(p.id) AS free_count
            FROM categories c
            LEFT JOIN proms p
              ON p.category_id = c.id
             AND IFNULL(p.deleted,0)=0
             AND p.used = 0
             AND (p.expires_at IS NULL OR p.expires_at > CURRENT_TIMESTAMP)
            GROUP BY c.id, c.name
            HAVING free_count > 0
            ORDER BY c.name COLLATE NOCASE
            """
        )
        rows = c.fetchall()
    if not rows:
        bot.send_message(message.chat.id, tr('no_categories', lang))
        return
    kb = InlineKeyboardMarkup()
    for cat_id, name, free_count in rows:
        kb.row(InlineKeyboardButton(text=f"{name} ({free_count})", callback_data=f"cat:{cat_id}"))
    bot.send_message(message.chat.id, tr('by_category_choose', lang), reply_markup=kb)

@bot.callback_query_handler(func=lambda call: call.data.startswith('cat:'))
def cb_choose_category(call: CallbackQuery):
    user_id = call.from_user.id
    lang = get_lang(user_id)
    try:
        cat_id = int(call.data.split(':', 1)[1])
    except Exception:
        bot.answer_callback_query(call.id)
        return
    # If user already has a PROM, do not issue a new one
    with conn_lock:
        c.execute("SELECT received_id FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row and row[0]:
            # Show existing content
            prom_id = row[0]
            c.execute("SELECT content FROM proms WHERE id = ?", (prom_id,))
            prow = c.fetchone()
            text = tr('already_have_prom', lang)
            if prow:
                text += f"\n\n{prow[0]}"
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, text)
            return
        # Check global daily limit
        c.execute("SELECT COUNT(*) FROM user_prom_history WHERE DATE(received_at) = DATE('now')")
        issued_today = c.fetchone()[0]
        if issued_today >= DAILY_GLOBAL_ISSUE_LIMIT:
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, tr('daily_global_limit', lang))
            return
        # Find one free PROM in this category
        c.execute(
            """
            SELECT id, content
            FROM proms
            WHERE category_id = ?
              AND IFNULL(deleted,0)=0
              AND used = 0
              AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (cat_id,)
        )
        prom = c.fetchone()
        if not prom:
            bot.answer_callback_query(call.id)
            bot.send_message(call.message.chat.id, tr('no_free_in_cat', lang))
            return
        prom_id, content = prom
        # Assign to user
        c.execute("INSERT OR REPLACE INTO users (user_id, received_id) VALUES (?, ?)", (user_id, prom_id))
        c.execute("UPDATE proms SET used = 1 WHERE id = ?", (prom_id,))
        c.execute("INSERT INTO user_prom_history (user_id, prom_id) VALUES (?, ?)", (user_id, prom_id))
        conn.commit()
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton(text=I18N['btn_copy'][lang], callback_data=f"copy:{prom_id}"))
    kb.row(InlineKeyboardButton(text=I18N['btn_hide'][lang], callback_data=f"hide:{prom_id}"))
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, f"{I18N['your_prom'][lang]}\n\n{content}", reply_markup=kb)

@bot.message_handler(func=lambda msg: msg.text in ["🔗 Referal havola", "🔗 Реферальная ссылка"])
def handle_referral_link(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    lang = get_lang(user_id)
    link = build_bot_deeplink(for_user_id=user_id)
    with conn_lock:
        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,))
        refs = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ? AND subscribed_at IS NOT NULL", (user_id,))
        conv = c.fetchone()[0]
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton(text=tr('btn_open_link', lang), url=link))
    kb.row(InlineKeyboardButton(text=tr('btn_share_text', lang), callback_data=f"refshare:{user_id}"))
    bot.send_message(
        message.chat.id,
        tr('referral_text', lang).format(link=link, count=refs, conv=conv),
        reply_markup=kb,
        disable_web_page_preview=True,
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('refshare:'))
def cb_ref_share(call: CallbackQuery):
    user_id = call.from_user.id
    lang = get_lang(user_id)
    link = build_bot_deeplink(for_user_id=user_id)
    text = tr('share_text', lang).format(link=link)
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    bot.send_message(call.message.chat.id, text, disable_web_page_preview=True)

def process_prom_id(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    lang = get_lang(user_id)
    prom_id = message.text.strip()
    with conn_lock:
        c.execute("SELECT received_id FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if row and row[0]:
            bot.send_message(message.chat.id, tr('already_have_prom', lang))
            send_main_menu(message.chat.id, user_id)
            return
        c.execute("SELECT id, content FROM proms WHERE id = ? AND used = 0 AND IFNULL(deleted,0)=0 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)", (prom_id,))
        prom = c.fetchone()
        if not prom:
            bot.send_message(message.chat.id, I18N['invalid_prom_id'][lang])
            send_main_menu(message.chat.id, user_id)
            return
        # Check global daily limit
        c.execute("SELECT COUNT(*) FROM user_prom_history WHERE DATE(received_at) = DATE('now')")
        issued_today = c.fetchone()[0]
        if issued_today >= DAILY_GLOBAL_ISSUE_LIMIT:
            bot.send_message(message.chat.id, tr('daily_global_limit', lang))
            send_main_menu(message.chat.id, user_id)
            return
        c.execute("INSERT OR REPLACE INTO users (user_id, received_id) VALUES (?, ?)", (user_id, prom_id))
        c.execute("UPDATE proms SET used = 1 WHERE id = ?", (prom_id,))
        c.execute("INSERT INTO user_prom_history (user_id, prom_id) VALUES (?, ?)", (user_id, prom_id))
        conn.commit()
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton(text=I18N['btn_copy'][lang], callback_data=f"copy:{prom_id}"))
    kb.row(InlineKeyboardButton(text=I18N['btn_hide'][lang], callback_data=f"hide:{prom_id}"))
    bot.send_message(message.chat.id, f"{I18N['your_prom'][lang]}\n\n{prom[1]}", reply_markup=kb)
    send_main_menu(message.chat.id, user_id)

@bot.message_handler(commands=['admin'])
def handle_admin_auth(message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2 or parts[1] != ADMIN_SECRET:
        bot.send_message(message.chat.id, "⛔ Noto'g'ri kod.")
        return
    with conn_lock:
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (message.from_user.id,))
        conn.commit()
    bot.send_message(message.chat.id, "✅ Siz endi adminsiz.")
    send_admin_menu(message.chat.id)

@bot.message_handler(commands=['mod'])
def handle_mod_auth(message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2 or parts[1] != MOD_SECRET:
        bot.send_message(message.chat.id, "⛔ Noto'g'ri kod.")
        return
    with conn_lock:
        c.execute("INSERT OR IGNORE INTO moderators (user_id) VALUES (?)", (message.from_user.id,))
        conn.commit()
    bot.send_message(message.chat.id, "✅ Siz endi moderatorisiz.")


@bot.message_handler(func=lambda msg: msg.text == "🚪 Adminlikdan chiqish")
def handle_admin_logout(message: Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    with conn_lock:
        c.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()
    bot.send_message(message.chat.id, "👋 Siz adminlikdan chiqdingiz.")
    send_main_menu(message.chat.id, user_id)


@bot.message_handler(func=lambda msg: msg.text == "↩️ Foydalanuvchi menyusi")
def handle_back_to_user_menu(message: Message):
    send_main_menu(message.chat.id, message.from_user.id)


@bot.message_handler(func=lambda msg: msg.text == "🔧 Admin menyu")
def handle_open_admin_menu(message: Message):
    user_id = message.from_user.id
    if has_moderator_rights(user_id):
        send_admin_menu(message.chat.id)
    else:
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")

@bot.message_handler(func=lambda msg: msg.text == "➕ PROM qo'shish")
def handle_add_button(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "✏️ PROMni quyidagi formatda kiriting: ID | matn")
    bot.register_next_step_handler(message, process_add_prom)

def process_add_prom(message: Message):
    text = (message.text or '').strip()

    # If user pressed another admin menu button, route to that handler and exit this flow
    if text in {
        "📋 PROMlar ro'yxati",
        "📤 Xabar yuborish",
        "🔍 PROM qidirish",
        "📝 PROM tahrirlash",
        "🗑 PROM o'chirish",
        "👤 Foydalanuvchilar ro'yxati",
        "➕ PROM qo'shish",
        "🔔 Barchaga bildirishnoma",
        "🌐 Til / Язык",
        "📊 Statistika",
        "ℹ️ Yordam",
        "📜 Mening PROMim",
        "🎁 PROM olish",
        "🎁 Получить PROM",
        "📊 Статистика",
        "ℹ️ Помощь",
        "📜 Мой PROM",
        "🔄 PROM yangilash",
        "↩️ Foydalanuvchi menyusi",
        "🚪 Adminlikdan chiqish",
        "🔧 Admin menyu",
    }:
        if text == "📋 PROMlar ro'yxati":
            return handle_list_button(message)
        if text == "📤 Xabar yuborish":
            return handle_broadcast(message)
        if text == "🔍 PROM qidirish":
            return handle_search_prom(message)
        if text == "📝 PROM tahrirlash":
            return handle_edit_prom(message)
        if text == "🗑 PROM o'chirish":
            return handle_delete_prom(message)
        if text == "👤 Foydalanuvchilar ro'yxati":
            return handle_users_list(message)
        if text in ("🎁 PROM olish", "🎁 Получить PROM"):
            return request_prom_id(message)
        if text == "🔄 PROM yangilash":
            return refresh_prom(message)
        if text in ("📊 Statistika", "📊 Статистика"):
            return show_stats(message)
        if text in ("ℹ️ Yordam", "ℹ️ Помощь"):
            return help_menu(message)
        if text in ("📜 Mening PROMim", "📜 Мой PROM"):
            return user_prom(message)
        if text == "🌐 Til / Язык":
            return handle_language_switch(message)
        if text == "➕ PROM qo'shish":
            # Re-prompt add flow explicitly
            bot.send_message(message.chat.id, "✏️ PROMni quyidagi formatda kiriting: ID | matn")
            bot.register_next_step_handler(message, process_add_prom)
            return
        if text == "↩️ Foydalanuvchi menyusi":
            send_main_menu(message.chat.id, message.from_user.id)
            return
        if text == "🚪 Adminlikdan chiqish":
            return handle_admin_logout(message)
        if text == "🔧 Admin menyu":
            if has_moderator_rights(message.from_user.id):
                send_admin_menu(message.chat.id)
            else:
                bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
            return
        if text == "📥 CSV import/eksport":
            return handle_csv_menu(message)

    # Validate format before splitting
    if '|' not in text:
        bot.send_message(
            message.chat.id,
            "⚠️ Noto'g'ri format. Foydalaning: ID | matn\n👉 Masalan: 123 | Bu PROM matni",
        )
        bot.register_next_step_handler(message, process_add_prom)
        return

    prom_id, content = map(str.strip, text.split('|', 1))
    if not prom_id or not content:
        bot.send_message(message.chat.id, "⚠️ Noto'g'ri format. ID va matn bo'sh bo'lmasin.")
        bot.register_next_step_handler(message, process_add_prom)
        return

    with conn_lock:
        c.execute("INSERT OR REPLACE INTO proms (id, content, used) VALUES (?, ?, 0)", (prom_id, content))
        conn.commit()
    bot.send_message(message.chat.id, f"✅ PROM `{prom_id}` qo'shildi.", parse_mode='Markdown')

@bot.message_handler(func=lambda msg: msg.text == "📋 PROMlar ro'yxati")
def handle_list_button(message: Message):
    if not ensure_subscription(message):
        return
    with conn_lock:
        c.execute("SELECT id, used, created_at FROM proms WHERE IFNULL(deleted,0)=0 ORDER BY created_at DESC")
        rows = c.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Hozircha PROMlar yo'q.")
        return
    total_count = len(rows)
    used_count = sum(1 for _id, used, _ in rows if used == 1)
    free_count = total_count - used_count
    lines = [
        "📄 PROMlar ro'yxati:",
        f"Jami: {total_count} | Berilgan: {used_count} | Mavjud: {free_count}",
        "",
    ]
    with conn_lock:
        for prom_id, used, created_at in rows:
            status = "✅" if used == 0 else "❌"
            user_info = ""
            if used == 1:
                c.execute("SELECT user_id FROM users WHERE received_id = ? LIMIT 1", (prom_id,))
                urow = c.fetchone()
                if urow:
                    user_info = f" | 👤 {urow[0]}"
            lines.append(f"{status} {prom_id} | {created_at[:16]}{user_info}")
    bot.send_message(message.chat.id, "\n".join(lines))
    # Inline manage last 10 entries
    manage_rows = rows[:10]
    for prom_id, used, created_at in manage_rows:
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"prom_edit:{prom_id}"),
            InlineKeyboardButton(text="🗑 Delete", callback_data=f"prom_softdel:{prom_id}")
        )
        if used == 1:
            kb.row(InlineKeyboardButton(text="♻️ Restore unused", callback_data=f"prom_restore:{prom_id}"))
        bot.send_message(message.chat.id, f"{prom_id} · {('used' if used else 'free')} · {created_at[:16]}", reply_markup=kb)

@bot.message_handler(func=lambda msg: msg.text == "🔍 PROM qidirish")
def handle_search_prom(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "🔎 Qidiriladigan PROM ID yoki matnni kiriting:")
    bot.register_next_step_handler(message, process_search_prom)

def process_search_prom(message: Message):
    query = message.text.strip()
    with conn_lock:
        c.execute("SELECT id, content, used FROM proms WHERE id LIKE ? OR content LIKE ? LIMIT 10", (f"%{query}%", f"%{query}%"))
        rows = c.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "🔍 Hech narsa topilmadi.")
        return
    for row in rows:
        status = "✅" if row[2] == 0 else "❌"
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"prom_edit:{row[0]}"),
            InlineKeyboardButton(text=("🗑 Delete" if row[2] == 0 else "♻️ Restore unused"), callback_data=(f"prom_softdel:{row[0]}" if row[2] == 0 else f"prom_restore:{row[0]}"))
        )
        bot.send_message(message.chat.id, f"{status} {row[0]}: {row[1][:100]}", reply_markup=kb)

@bot.message_handler(func=lambda msg: msg.text == "📝 PROM tahrirlash")
def handle_edit_prom(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "✏️ Tahrirlanadigan PROM ID ni kiriting:")
    bot.register_next_step_handler(message, process_edit_prom_id)

def process_edit_prom_id(message: Message):
    prom_id = message.text.strip()
    with conn_lock:
        c.execute("SELECT content FROM proms WHERE id = ?", (prom_id,))
        prom = c.fetchone()
    if not prom:
        bot.send_message(message.chat.id, "❌ PROM topilmadi.")
        return
    bot.send_message(message.chat.id, f"📝 Yangi matnni kiriting (hozirgi: {prom[0][:40]}...):")
    bot.register_next_step_handler(message, lambda m: process_edit_prom_content(m, prom_id))

def process_edit_prom_content(message: Message, prom_id):
    new_content = message.text.strip()
    with conn_lock:
        c.execute("UPDATE proms SET content = ? WHERE id = ?", (new_content, prom_id))
        conn.commit()
    bot.send_message(message.chat.id, f"✅ PROM `{prom_id}` yangilandi.", parse_mode='Markdown')

@bot.message_handler(func=lambda msg: msg.text == "👤 Foydalanuvchilar ro'yxati")
def handle_users_list(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    with conn_lock:
        c.execute("SELECT user_id, received_id, joined_at FROM users ORDER BY joined_at DESC")
        rows = c.fetchall()
    if not rows:
        bot.send_message(message.chat.id, "📭 Hozircha foydalanuvchilar yo'q.")
        return
    text = "👥 Foydalanuvchilar ro'yxati:\n"
    for uid, pid, joined in rows:
        text += f"👤 {uid} — PROM: {pid or '—'} | {joined[:16]}\n"
    bot.send_message(message.chat.id, text)

@bot.message_handler(func=lambda msg: msg.text == "📤 Xabar yuborish")
def handle_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "📨 Yuboriladigan xabar matnini kiriting:")
    bot.register_next_step_handler(message, process_broadcast)

def process_broadcast(message: Message):
    text = message.text
    with conn_lock:
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
    count = 0
    for (uid,) in users:
        try:
            bot.send_message(uid, f"📢 Admindan xabar:\n\n{text}")
            count += 1
            time.sleep(0.05)
        except Exception as e:
            pass
    bot.send_message(message.chat.id, f"✅ Xabar {count} foydalanuvchiga yuborildi.")

@bot.message_handler(commands=['set_announce_channel'])
def cmd_set_announce_channel(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    parts = (message.text or '').strip().split(maxsplit=1)
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Foydalanish: /set_announce_channel @kanal_yoki_id")
        return
    chan = parts[1]
    set_setting('announce_channel', chan)
    bot.send_message(message.chat.id, f"✅ E'lon kanali sozlandi: {chan}")

@bot.message_handler(commands=['get_announce_channel'])
def cmd_get_announce_channel(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    chan = get_announce_channel()
    default_text = "yo'q"
    bot.send_message(message.chat.id, f"E'lon kanali: {chan or default_text}")

@bot.message_handler(func=lambda msg: msg.text == "🗑 PROM o'chirish")
def handle_delete_prom(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "🧹 O'chiriladigan PROM ID ni kiriting:")
    bot.register_next_step_handler(message, process_delete_prom)

def process_delete_prom(message: Message):
    prom_id = message.text.strip()
    with conn_lock:
        c.execute("UPDATE proms SET deleted = 1 WHERE id = ?", (prom_id,))
        conn.commit()
    bot.send_message(message.chat.id, f"🗑 PROM `{prom_id}` yumshoq oʻchirildi.", parse_mode='Markdown')

@bot.message_handler(func=lambda msg: msg.text == "🔔 Barchaga bildirishnoma")
def notify_all_users(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    bot.send_message(message.chat.id, "🔈 Bildirishnoma matnini kiriting:")
    bot.register_next_step_handler(message, process_notify_all)

def process_notify_all(message: Message):
    msg = message.text
    with conn_lock:
        c.execute("SELECT user_id FROM users")
        users = c.fetchall()
    for (uid,) in users:
        try:
            bot.send_message(uid, f"🔔 Bildirishnoma:\n\n{msg}")
            time.sleep(0.05)
        except Exception as e:
            pass
    bot.send_message(message.chat.id, "✅ Bildirishnoma yuborildi.")

# === CSV IMPORT/EXPORT MENU ===
@bot.message_handler(func=lambda msg: msg.text == "📥 CSV import/eksport")
def handle_csv_menu(message: Message):
    if not has_moderator_rights(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    lang = get_lang(message.from_user.id)
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton(text=tr('btn_csv_import', lang), callback_data='csv:import'),
        InlineKeyboardButton(text=tr('btn_csv_export', lang), callback_data='csv:export'),
    )
    kb.row(InlineKeyboardButton(text=tr('btn_csv_template', lang), callback_data='csv:template'))
    bot.send_message(message.chat.id, tr('csv_prompt', lang), reply_markup=kb)

@bot.callback_query_handler(func=lambda call: call.data.startswith('csv:'))
def cb_csv(call: CallbackQuery):
    user_id = call.from_user.id
    if not has_moderator_rights(user_id):
        bot.answer_callback_query(call.id)
        return
    action = call.data.split(':', 1)[1]
    lang = get_lang(user_id)
    if action == 'import':
        WAITING_FOR[user_id] = {"action": "csv_import"}
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, tr('send_csv_file', lang))
    elif action == 'export':
        with conn_lock:
            c.execute("SELECT id, content, IFNULL(expires_at,''), IFNULL(category_id,'') FROM proms WHERE IFNULL(deleted,0)=0")
            rows = c.fetchall()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "content", "expires_at", "category_id"])
        for row in rows:
            writer.writerow(row)
        data_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        data_bytes.name = 'proms_export.csv'
        bot.answer_callback_query(call.id)
        bot.send_document(call.message.chat.id, data_bytes, caption=tr('csv_export_title', lang))
    elif action == 'template':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "content", "expires_at", "category_id"])
        writer.writerow(["PROM123", "Some text", "2025-12-31", "1"])
        data_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        data_bytes.name = 'proms_template.csv'
        bot.answer_callback_query(call.id)
        bot.send_document(call.message.chat.id, data_bytes, caption=tr('csv_export_title', lang))

@bot.message_handler(content_types=['document'])
def handle_document(message: Message):
    data = WAITING_FOR.get(message.from_user.id)
    if not data or data.get('action') != 'csv_import':
        return
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        text = downloaded_file.decode('utf-8', errors='ignore')
        imported = import_proms_from_csv_text(text)
        WAITING_FOR.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, tr('csv_import_ok', get_lang(message.from_user.id)).format(n=imported))
    except Exception:
        WAITING_FOR.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "❌ CSV importda xatolik.")

@bot.message_handler(func=lambda msg: WAITING_FOR.get(msg.from_user.id, {}).get('action') == 'csv_import')
def handle_csv_text(message: Message):
    text = message.text or ''
    try:
        imported = import_proms_from_csv_text(text)
        WAITING_FOR.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, tr('csv_import_ok', get_lang(message.from_user.id)).format(n=imported))
    except Exception:
        WAITING_FOR.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "❌ CSV importda xatolik.")

def import_proms_from_csv_text(text: str) -> int:
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    # Skip header if present
    if rows and rows[0] and rows[0][0].lower() == 'id':
        rows = rows[1:]
    count = 0
    with conn_lock:
        for r in rows:
            if not r:
                continue
            pid = (r[0] or '').strip()
            if not pid:
                continue
            content = (r[1] if len(r) > 1 else '').strip()
            expires_at = (r[2] if len(r) > 2 else '').strip() or None
            category_id = None
            try:
                if len(r) > 3 and r[3].strip() != '':
                    category_id = int(r[3])
            except Exception:
                category_id = None
            if expires_at:
                try:
                    datetime.strptime(expires_at, '%Y-%m-%d')
                except Exception:
                    expires_at = None
            if category_id is not None and expires_at:
                c.execute("INSERT OR REPLACE INTO proms (id, content, used, expires_at, category_id) VALUES (?, ?, 0, ?, ?)", (pid, content, expires_at, category_id))
            elif expires_at:
                c.execute("INSERT OR REPLACE INTO proms (id, content, used, expires_at) VALUES (?, ?, 0, ?)", (pid, content, expires_at))
            else:
                c.execute("INSERT OR REPLACE INTO proms (id, content, used) VALUES (?, ?, 0)", (pid, content))
            count += 1
        conn.commit()
    return count

@bot.message_handler(func=lambda msg: msg.text in ["🕘 Tarix", "🕘 История"])
def handle_history(message: Message):
    if not ensure_subscription(message):
        return
    user_id = message.from_user.id
    show_history_page(message.chat.id, user_id, page=0)

def show_history_page(chat_id: int, user_id: int, page: int):
    page_size = 5
    offset = page * page_size
    with conn_lock:
        c.execute(
            """
            SELECT h.prom_id, h.received_at, p.content
            FROM user_prom_history h
            LEFT JOIN proms p ON p.id = h.prom_id
            WHERE h.user_id = ?
            ORDER BY h.received_at DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, page_size, offset)
        )
        rows = c.fetchall()
    lang = get_lang(user_id)
    if not rows:
        bot.send_message(chat_id, tr('empty_history', lang))
        return
    text_lines = [tr('history_title', lang)]
    for pid, ts, content in rows:
        ts_str = (ts or '')[:16]
        summary = (content or '')
        if len(summary) > 120:
            summary = summary[:120] + '…'
        text_lines.append(f"{ts_str} — {pid}: {summary}")
    kb = InlineKeyboardMarkup()
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text=tr('btn_prev', lang), callback_data=f"hist:{page-1}"))
    # Offer next page button if there may be more
    if len(rows) == page_size:
        nav.append(InlineKeyboardButton(text=tr('btn_next', lang), callback_data=f"hist:{page+1}"))
    if nav:
        kb.row(*nav)
    bot.send_message(chat_id, "\n".join(text_lines), reply_markup=kb)

@bot.callback_query_handler(func=lambda call: call.data.startswith('hist:'))
def cb_history_nav(call: CallbackQuery):
    user_id = call.from_user.id
    try:
        page = int(call.data.split(':', 1)[1])
    except Exception:
        bot.answer_callback_query(call.id)
        return
    bot.answer_callback_query(call.id)
    show_history_page(call.message.chat.id, user_id, page)
# === ADMIN: CREATE 10 SECRET PROMs ===
@bot.message_handler(commands=['secret10'])
def seed_secret_proms(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    category_id = get_or_create_category('secret')
    created_ids = []
    with conn_lock:
        for content in SECRET_PROMPTS:
            new_id = generate_prom_id()
            c.execute(
                "INSERT OR REPLACE INTO proms (id, content, used, category_id, deleted) VALUES (?, ?, 0, ?, 0)",
                (new_id, content, category_id),
            )
            created_ids.append(new_id)
        conn.commit()
    text_lines = [
        "🔒 10 ta SECRET PROM yaratildi (har biri 1 marta ishlatiladi):",
        *created_ids
    ]
    bot.send_message(message.chat.id, "\n".join(text_lines))
    # Auto-post to channel if configured
    try:
        if post_secret_announce(created_ids, inv_user_id=message.from_user.id):
            bot.send_message(message.chat.id, "📣 Kanalga e'lon yuborildi.")
        else:
            bot.send_message(message.chat.id, "ℹ️ E'lon kanali sozlanmagan yoki yuborilmadi.")
    except Exception:
        bot.send_message(message.chat.id, "⚠️ Kanalga yuborishda xatolik yuz berdi.")

# === CALLBACK: OBUNA TEKSHIRISH ===
@bot.callback_query_handler(func=lambda call: call.data == 'check_sub')
def check_subscription_callback(call: CallbackQuery):
    user_id = call.from_user.id
    lang = get_lang(user_id)
    if is_subscribed(user_id) or is_admin(user_id):
        bot.answer_callback_query(call.id, I18N['sub_checked_ok'][lang])
        with conn_lock:
            c.execute("UPDATE users SET subscribed_at = COALESCE(subscribed_at, CURRENT_TIMESTAMP) WHERE user_id = ?", (user_id,))
            conn.commit()
            # check referral milestone rewards for referrer
            try:
                c.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
                referrer_row = c.fetchone()
                referrer_id = referrer_row[0] if referrer_row else None
                if referrer_id:
                    c.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ? AND subscribed_at IS NOT NULL", (referrer_id,))
                    conv_count = c.fetchone()[0]
                    for threshold in REFERRAL_REWARD_THRESHOLDS:
                        if conv_count == threshold:
                            bonus_text = BONUS_REWARD_TEXTS.get(threshold, "Bonus PROM")
                            referrer_lang = get_lang(referrer_id)
                            bot.send_message(referrer_id, tr('reward_unlocked', referrer_lang).format(n=threshold, content=bonus_text))
                            break
            except Exception:
                pass
        send_main_menu(call.message.chat.id, user_id)
    else:
        bot.answer_callback_query(call.id, I18N['sub_checked_fail'][lang], show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith('copy:'))
def handle_copy(call: CallbackQuery):
    prom_id = call.data.split(':', 1)[1]
    with conn_lock:
        c.execute("SELECT content FROM proms WHERE id = ?", (prom_id,))
        row = c.fetchone()
    if not row:
        bot.answer_callback_query(call.id)
        return
    try:
        bot.send_message(call.from_user.id, row[0])
        bot.answer_callback_query(call.id, "📋 Copied")
    except Exception:
        bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith('hide:'))
def handle_hide(call: CallbackQuery):
    prom_id = call.data.split(':', 1)[1]
    user_id = call.from_user.id
    lang = get_lang(user_id)
    try:
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=f"{I18N['your_prom'][lang]}\n\n•••",
        )
        bot.answer_callback_query(call.id)
    except Exception:
        bot.answer_callback_query(call.id)

# === INLINE MANAGEMENT CALLBACKS ===
WAITING_FOR = {}

@bot.callback_query_handler(func=lambda call: call.data.startswith('prom_edit:'))
def cb_prom_edit(call: CallbackQuery):
    user_id = call.from_user.id
    if not has_moderator_rights(user_id):
        bot.answer_callback_query(call.id)
        return
    prom_id = call.data.split(':', 1)[1]
    WAITING_FOR[user_id] = {"action": "edit_prom", "prom_id": prom_id}
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, f"✏️ Yangi matnni kiriting `{prom_id}` uchun:", parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data.startswith('prom_softdel:'))
def cb_prom_softdel(call: CallbackQuery):
    user_id = call.from_user.id
    if not has_moderator_rights(user_id):
        bot.answer_callback_query(call.id)
        return
    prom_id = call.data.split(':', 1)[1]
    with conn_lock:
        c.execute("UPDATE proms SET deleted = 1 WHERE id = ?", (prom_id,))
        conn.commit()
    bot.answer_callback_query(call.id, tr('soft_deleted', get_lang(user_id)))

@bot.callback_query_handler(func=lambda call: call.data.startswith('prom_restore:'))
def cb_prom_restore(call: CallbackQuery):
    user_id = call.from_user.id
    if not has_moderator_rights(user_id):
        bot.answer_callback_query(call.id)
        return
    prom_id = call.data.split(':', 1)[1]
    with conn_lock:
        c.execute("UPDATE users SET received_id = NULL WHERE received_id = ?", (prom_id,))
        c.execute("UPDATE proms SET used = 0, deleted = 0 WHERE id = ?", (prom_id,))
        conn.commit()
    bot.answer_callback_query(call.id, tr('not_owner_restore', get_lang(user_id)))

@bot.message_handler(func=lambda m: WAITING_FOR.get(m.from_user.id, {}).get('action') == 'edit_prom')
def catch_waiting_edit_prom(message: Message):
    data = WAITING_FOR.get(message.from_user.id)
    if not data:
        return
    prom_id = data.get('prom_id')
    new_content = (message.text or '').strip()
    with conn_lock:
        c.execute("UPDATE proms SET content = ? WHERE id = ?", (new_content, prom_id))
        conn.commit()
    WAITING_FOR.pop(message.from_user.id, None)
    bot.send_message(message.chat.id, f"✅ PROM `{prom_id}` yangilandi.", parse_mode='Markdown')

@bot.message_handler(commands=['postsecret'])
def cmd_postsecret(message: Message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Siz admin emassiz.")
        return
    # Try parse IDs from command message
    text = (message.text or '')
    ids = re.findall(r"\bSEC-[A-Z0-9]{6,}\b", text)
    if ids:
        ok = post_secret_announce(ids, inv_user_id=message.from_user.id)
        bot.send_message(message.chat.id, "📣 E'lon yuborildi." if ok else "⚠️ E'lon yuborilmadi. /set_announce_channel bilan sozlang yoki botni kanalga admin qiling.")
        return
    # Ask to paste list
    WAITING_FOR[message.from_user.id] = {"action": "postsecret_wait"}
    bot.send_message(message.chat.id, "ID ro'yxatini yuboring (har qatorda SEC-...)")

@bot.message_handler(func=lambda m: WAITING_FOR.get(m.from_user.id, {}).get('action') == 'postsecret_wait')
def catch_postsecret_wait(message: Message):
    data = WAITING_FOR.get(message.from_user.id)
    if not data:
        return
    ids = re.findall(r"\bSEC-[A-Z0-9]{6,}\b", (message.text or ''))
    WAITING_FOR.pop(message.from_user.id, None)
    if not ids:
        bot.send_message(message.chat.id, "❌ ID topilmadi. Qayta urinib ko'ring: /postsecret")
        return
    ok = post_secret_announce(ids, inv_user_id=message.from_user.id)
    bot.send_message(message.chat.id, "📣 E'lon yuborildi." if ok else "⚠️ E'lon yuborilmadi. /set_announce_channel bilan sozlang yoki botni kanalga admin qiling.")

# === ISHGA TUSHURISH ===
if __name__ == "__main__":
    try:
        infinity = getattr(bot, "infinity_polling", None)
        if callable(infinity):
            # Keep read timeout higher than long_polling_timeout
            infinity(timeout=55, long_polling_timeout=30, skip_pending=True)
        else:
            while True:
                try:
                    bot.polling(none_stop=True, interval=0, timeout=55, long_polling_timeout=30)
                except Exception as polling_error:
                    print(f"[WARN] Polling error: {polling_error}. Restarting in 5s...")
                    time.sleep(5)
    except KeyboardInterrupt:
        pass