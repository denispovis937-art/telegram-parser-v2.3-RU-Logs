# -*- coding: utf-8 -*-
"""
telegram-parser-v2.2
Парсер участников + инвайтер (Telethon, sync)

Апгрейды:
- Жёсткий фильтр качества (без привязки к языку)
- Дедуп списков usernames/userids
- Invite ledger (SQLite) — пропускает уже обработанных, умеет продолжать
- FloodWait handling + "человеческие" RU-логи в app.log
"""

import os
import time
import random
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Optional, Tuple, Union, Dict, Any

from telethon.sync import TelegramClient
from telethon import utils as tl_utils
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import (
    UserStatusOnline,
    UserStatusRecently,
    UserStatusLastWeek,
    UserStatusLastMonth,
    UserStatusOffline,
)

from telethon.errors import (
    FloodWaitError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    ChatAdminRequiredError,
    PeerFloodError,
    UsernameInvalidError,
    UserIdInvalidError,
    RPCError,
)

LOG_FILE = "app.log"
LEDGER_DB = "invite_ledger.db"

# -------------------- ЛОГИ --------------------

def _setup_logging() -> None:
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

_setup_logging()

def log_info(msg: str) -> None:
    logging.info(f"ИНФО | {msg}")

def log_ok(msg: str) -> None:
    logging.info(f"УСПЕХ | {msg}")

def log_warn(msg: str) -> None:
    logging.info(f"ВНИМАНИЕ | {msg}")

def log_pause(msg: str) -> None:
    logging.info(f"ПАУЗА | {msg}")

def log_stop(msg: str) -> None:
    logging.info(f"СТОП | {msg}")

# -------------------- OPTIONS --------------------

DEFAULT_OPTIONS = [
    "NONEID\n",
    "NONEHASH\n",
    "True\n",   # parse user-id
    "True\n",   # parse user-name
]

def ensure_options() -> None:
    if not os.path.exists("options.txt"):
        with open("options.txt", "w", encoding="utf-8") as f:
            f.writelines(DEFAULT_OPTIONS)
        return

    # если файл пустой — тоже восстановим
    with open("options.txt", "r+", encoding="utf-8") as f:
        lines = f.readlines()
        if not lines:
            f.seek(0)
            f.writelines(DEFAULT_OPTIONS)

def getoptions() -> List[str]:
    ensure_options()
    with open("options.txt", "r", encoding="utf-8") as f:
        return f.readlines()

# -------------------- QUALITY FILTER (HARD) --------------------

def _is_active(status) -> bool:
    """Жёстко считаем активным: online/recently/last week или offline был в последние 7 дней."""
    if status is None:
        return False
    if isinstance(status, (UserStatusOnline, UserStatusRecently, UserStatusLastWeek)):
        return True
    if isinstance(status, UserStatusOffline):
        try:
            # was_online обычно tz-aware (UTC)
            was = status.was_online
            if was is None:
                return False
            now = datetime.now(timezone.utc)
            return (now - was) <= timedelta(days=7)
        except Exception:
            return False
    # LastMonth считаем уже слабым для жёсткого фильтра
    return False

def quality_hard(user) -> Tuple[bool, str]:
    """
    Жёсткий фильтр качества (язык НЕ учитываем):
    - не бот
    - не deleted
    - не scam/fake (если поле есть)
    - есть username
    - есть фото
    - активен (online/recently/last week или был онлайн <=7 дней)
    """
    if getattr(user, "bot", False):
        return False, "бот"
    if getattr(user, "deleted", False):
        return False, "удалён"
    if getattr(user, "scam", False):
        return False, "scam"
    if getattr(user, "fake", False):
        return False, "fake"
    if not getattr(user, "username", None):
        return False, "нет username"
    if not getattr(user, "photo", None):
        return False, "нет фото"
    if not _is_active(getattr(user, "status", None)):
        return False, "не активен"
    return True, "ok"

# -------------------- DEDUP HELPERS --------------------

def _read_set(path: str, strip_at: bool = False) -> set:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        items = set()
        for line in f:
            s = line.strip()
            if not s:
                continue
            if strip_at and s.startswith("@"):
                s = s[1:]
            items.add(s)
        return items

def _append_unique(path: str, values: Iterable[str], prefix_at: bool = False) -> int:
    existing = _read_set(path, strip_at=prefix_at)
    new_vals = []
    for v in values:
        if not v:
            continue
        vv = v.strip()
        if not vv:
            continue
        # нормализация
        if prefix_at and vv.startswith("@"):
            vv = vv[1:]
        if vv in existing:
            continue
        existing.add(vv)
        new_vals.append(("@" + vv) if prefix_at else vv)

    if not new_vals:
        return 0

    with open(path, "a", encoding="utf-8") as f:
        for v in new_vals:
            f.write(v + "\n")
    return len(new_vals)

# -------------------- LEDGER (SQLite) --------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(LEDGER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target TEXT NOT NULL,
            user_key TEXT NOT NULL,
            user_id INTEGER,
            username TEXT,
            status TEXT NOT NULL,
            reason TEXT,
            ts TEXT NOT NULL
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_unique ON invites(target, user_key)")
    conn.commit()
    return conn

def ledger_get(conn: sqlite3.Connection, target: str, user_key: str) -> Optional[Tuple[str, str]]:
    cur = conn.execute(
        "SELECT status, reason FROM invites WHERE target=? AND user_key=? LIMIT 1",
        (target, user_key),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None

def ledger_put(conn: sqlite3.Connection, target: str, user_key: str, user_id: Optional[int],
               username: Optional[str], status: str, reason: str = "") -> None:
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO invites(target, user_key, user_id, username, status, reason, ts) VALUES (?,?,?,?,?,?,?)",
        (target, user_key, user_id, username, status, reason, ts),
    )
    conn.commit()

# -------------------- CORE OPS --------------------

def parsing(client: TelegramClient, chat_entity: Union[str, int, Any], parse_id: bool, parse_name: bool) -> None:
    """
    Парсинг участников с ЖЁСТКИМ фильтром качества.
    Результат пишет в:
      - usernames.txt (с @)
      - userids.txt
    """
    log_info(f"🔍 Начат парсинг: {chat_entity}")
    good_usernames: List[str] = []
    good_ids: List[str] = []

    total = 0
    kept = 0
    skipped = {}

    for user in client.iter_participants(chat_entity):
        total += 1
        ok, reason = quality_hard(user)
        if not ok:
            skipped[reason] = skipped.get(reason, 0) + 1
            continue

        kept += 1
        if parse_name and user.username:
            good_usernames.append(user.username)
        if parse_id:
            good_ids.append(str(user.id))

    added_u = added_i = 0
    if parse_name:
        added_u = _append_unique("usernames.txt", good_usernames, prefix_at=True)
    if parse_id:
        added_i = _append_unique("userids.txt", good_ids, prefix_at=False)

    log_ok(f"✅ Парсинг завершён. Всего: {total}, прошло фильтр: {kept}, добавлено usernames: {added_u}, ids: {added_i}")
    if skipped:
        parts = ", ".join([f"{k}={v}" for k, v in sorted(skipped.items(), key=lambda x: -x[1])])
        log_info(f"📉 Отфильтровано: {parts}")


def parsing_from_messages(
    client: TelegramClient,
    chat_entity: Union[str, int, Any],
    parse_id: bool,
    parse_name: bool,
    limit_messages: int = 5000,
    max_age_days: int = 7,
) -> None:
    """
    Парсинг пользователей ИЗ СООБЩЕНИЙ (когда список участников скрыт).
    Собирает авторов сообщений за последние `max_age_days` дней (или последние `limit_messages` сообщений).
    Применяет ЖЁСТКИЙ фильтр качества и пишет в:
      - usernames.txt (с @)
      - userids.txt
    """
    log_info(f"🔍 Начат парсинг из сообщений: {chat_entity} | лимит сообщений={limit_messages} | возраст≤{max_age_days}д")
    print("Старт: парсинг из сообщений… Это может занять время. Прогресс будет обновляться.", flush=True)
    good_usernames: List[str] = []
    good_ids: List[str] = []

    scanned = 0
    unique_found = 0
    kept = 0
    skipped: Dict[str, int] = {}

    seen_user_ids: set = set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    for msg in client.iter_messages(chat_entity, limit=limit_messages):
        # Периодический прогресс в консоль (чтобы не казалось, что всё зависло)
        scanned += 1
        if scanned % 200 == 0:
            print(f"Просмотрено: {scanned} | уникальных авторов: {unique_found} | прошло фильтр: {kept}", end='\r', flush=True)
        try:
            # Отсечём слишком старые сообщения (если есть дата)
            if getattr(msg, "date", None) is not None:
                try:
                    # msg.date обычно tz-aware (UTC)
                    if msg.date < cutoff:
                        break
                except Exception:
                    pass

            sid = getattr(msg, "sender_id", None)
            if not sid:
                continue
            if sid in seen_user_ids:
                continue
            seen_user_ids.add(sid)
            unique_found += 1

            # Получаем объект пользователя
            user = getattr(msg, "sender", None)
            if user is None:
                try:
                    user = msg.get_sender()
                except Exception:
                    user = None
            if user is None:
                try:
                    user = client.get_entity(sid)
                except Exception:
                    user = None
            if user is None:
                skipped["не удалось получить пользователя"] = skipped.get("не удалось получить пользователя", 0) + 1
                continue

            ok, reason = quality_hard(user)
            if not ok:
                skipped[reason] = skipped.get(reason, 0) + 1
                continue

            kept += 1
            if parse_name and getattr(user, "username", None):
                good_usernames.append(user.username)
            if parse_id and getattr(user, "id", None):
                good_ids.append(str(user.id))

        except Exception as e:
            skipped["ошибка сообщения"] = skipped.get("ошибка сообщения", 0) + 1
            log_warn(f"⚠️ Ошибка при обработке сообщения: {e}")

    added_u = added_i = 0
    if parse_name:
        added_u = _append_unique("usernames.txt", good_usernames, prefix_at=True)
    if parse_id:
        added_i = _append_unique("userids.txt", good_ids, prefix_at=False)

    log_ok(
        f"✅ Парсинг из сообщений завершён. Сообщений просмотрено: {scanned}, уникальных авторов: {unique_found}, прошло фильтр: {kept}, "
        f"добавлено usernames: {added_u}, ids: {added_i}"
    )
    print()  # перевод строки после прогресс-строки
    print("Готово: парсинг из сообщений завершён. Итоги — в app.log и файлах usernames.txt/userids.txt", flush=True)
    if skipped:
        parts = ", ".join([f"{k}={v}" for k, v in sorted(skipped.items(), key=lambda x: -x[1])])
        log_info(f"📉 Отфильтровано/пропущено: {parts}")

def _target_key(target: Any) -> str:
    """Стабильный ключ для target в ledger."""
    try:
        uname = getattr(target, "username", None)
        if uname:
            return "@" + str(uname)
        tid = getattr(target, "id", None)
        if tid is not None:
            return f"id:{tid}"
    except Exception:
        pass
    return str(target)


def target_ref(target: Any) -> Union[str, int, Any]:
    """Удобная "ссылка" на target, которую можно резолвить в других сессиях.

    Приоритет:
    1) @username (самое стабильное)
    2) peer-id через telethon.utils.get_peer_id (для каналов/супергрупп даёт -100...)
    3) обычный .id
    4) как есть
    """
    try:
        uname = getattr(target, "username", None)
        if uname:
            return "@" + str(uname)
        # get_peer_id работает и для каналов/чатов/юзеров
        try:
            pid = tl_utils.get_peer_id(target)
            if isinstance(pid, int):
                return pid
        except Exception:
            pass
        tid = getattr(target, "id", None)
        if tid is not None:
            return int(tid)
    except Exception:
        pass
    return target


def _make_client(session_file: str, api_id: int, api_hash: str) -> TelegramClient:
    """Создаёт sync TelethonClient по .session файлу."""
    session_name = session_file[:-8] if session_file.endswith(".session") else session_file
    client = TelegramClient(session_name, api_id, api_hash)
    client.connect()
    if not client.is_user_authorized():
        raise RuntimeError(f"Сессия не авторизована: {session_file}")
    return client


def inviting_rotate_sessions(
    api_id: int,
    api_hash: str,
    session_files: List[str],
    target: Union[str, int, Any],
    users: List[Union[str, int]],
    base_delay: float = 2.0,
    switch_on_floodwait_seconds: int = 60,
    rotate_every: int = 0,
    max_attempts_per_session: int = 0,
) -> None:
    """Инвайт с авто-сменой сессий.

    - Идёт по users.
    - При PeerFlood — немедленно переключается на следующую сессию.
    - При FloodWait:
        * если FloodWait <= switch_on_floodwait_seconds: ждёт и продолжает в той же сессии
        * если FloodWait >  switch_on_floodwait_seconds: переключается на следующую сессию

    Дополнительно (по желанию):
    - rotate_every: если >0, то переключаться на следующую сессию каждые N успешных инвайтов.
      (Помогает "размазать" нагрузку и снижает риск лимитов на аккаунте.)
    - max_attempts_per_session: если >0, то переключаться, когда в текущей сессии сделано N попыток
      (успех/ошибка/пропуск, кроме уже обработанных в ledger).

    ВАЖНО: target должен быть резолвимым во всех сессиях.
    Лучше передавать target_ref(entity) (см. функцию target_ref).
    """

    if not session_files:
        raise ValueError("Не переданы session_files")

    # ledger общий для всех сессий
    conn = _db()
    target_key = _target_key(target)

    # небольшая рандомизация
    delay = max(1.0, float(base_delay))

    # состояние по сессиям
    idx = 0
    client: Optional[TelegramClient] = None

    # счётчики для плановой ротации
    ok_in_session = 0
    attempts_in_session = 0

    def open_client(i: int) -> TelegramClient:
        sf = session_files[i]
        log_info(f"🔁 Переключаюсь на сессию: {sf}")
        c = _make_client(sf, api_id, api_hash)
        return c

    def close_client(c: Optional[TelegramClient]) -> None:
        try:
            if c is not None:
                c.disconnect()
        except Exception:
            pass

    def next_session() -> bool:
        nonlocal idx, client, ok_in_session, attempts_in_session
        close_client(client)
        idx += 1
        if idx >= len(session_files):
            client = None
            return False
        client = open_client(idx)
        ok_in_session = 0
        attempts_in_session = 0
        return True

    # стартуем первой сессией
    client = open_client(idx)

    log_info(
        f"🚀 Старт инвайта (ротация сессий) в: {target_key}. Кандидатов: {len(users)}. Сессий: {len(session_files)}"
    )

    ok_cnt = 0
    skip_cnt = 0
    fail_cnt = 0

    for raw in users:
        # нормализуем ключ
        if isinstance(raw, int) or (isinstance(raw, str) and raw.strip().isdigit()):
            user_id = int(raw)
            username = None
            user_key = f"id:{user_id}"
            entity = user_id
        else:
            u = str(raw).strip()
            if u.startswith("@"):  # на всякий
                u = u[1:]
            username = u or None
            user_id = None
            user_key = f"u:{username}" if username else "u:None"
            entity = username if username else raw

        prev = ledger_get(conn, target_key, user_key)
        if prev and prev[0] in ("ok", "already", "privacy", "invalid"):
            skip_cnt += 1
            continue

        # если сессии закончились — дальше смысла нет
        if client is None:
            log_stop("⛔ Сессии закончились. Останавливаю прогон.")
            break

        # джиттер перед действием
        time.sleep(delay + random.uniform(0.3, 1.2))

        # резолвим target в текущей сессии (на случай, если target — peer-id/@username)
        try:
            target_entity = client.get_entity(target)
        except Exception:
            target_entity = target

        try:
            attempts_in_session += 1
            client(InviteToChannelRequest(channel=target_entity, users=[entity]))
            ledger_put(conn, target_key, user_key, user_id, username, "ok", "ok")
            ok_cnt += 1
            ok_in_session += 1
            log_ok(f"✅ Инвайт отправлен: {('@'+username) if username else user_key} → {target_key}")
            delay = min(8.0, max(1.5, delay + random.uniform(-0.2, 0.4)))

            # Плановая ротация (если включена)
            if rotate_every and ok_in_session >= int(rotate_every):
                log_info(f"🔁 Плановая ротация: {ok_in_session} успешных инвайтов в текущей сессии — переключаюсь…")
                # небольшая пауза чтобы переключение выглядело "человечно"
                time.sleep(random.uniform(2.0, 5.0))
                if not next_session():
                    log_stop("⛔ Сессии закончились. Останавливаю прогон.")
                    break

        except UserAlreadyParticipantError:
            ledger_put(conn, target_key, user_key, user_id, username, "already", "уже участник")
            skip_cnt += 1
            log_info(f"👤 Уже в чате: {('@'+username) if username else user_key}")

        except UserPrivacyRestrictedError:
            ledger_put(conn, target_key, user_key, user_id, username, "privacy", "закрыты инвайты")
            skip_cnt += 1
            log_warn(f"🔒 Закрыты инвайты: {('@'+username) if username else user_key}")

        except FloodWaitError as e:
            sec = int(getattr(e, "seconds", 0) or 0)
            ledger_put(conn, target_key, user_key, user_id, username, "floodwait", f"{sec}")

            # если большой флуд — переключаемся
            if sec > int(switch_on_floodwait_seconds):
                log_pause(
                    f"💤 FloodWait {sec} сек (>{switch_on_floodwait_seconds}). Переключаю сессию и продолжу…"
                )
                # небольшая пауза, чтобы не долбить мгновенно
                time.sleep(random.uniform(2.0, 5.0))
                if not next_session():
                    log_stop("⛔ Нет доступных сессий для переключения. Останавливаю прогон.")
                    break
                fail_cnt += 1
                continue

            # иначе ждём и продолжаем той же сессией
            log_pause(f"💤 FloodWait {sec} сек. Ожидаю и продолжаю…")
            time.sleep(sec + random.uniform(1.0, 3.0))
            delay = min(12.0, max(delay, 6.0))
            fail_cnt += 1

        except (UsernameInvalidError, UserIdInvalidError):
            ledger_put(conn, target_key, user_key, user_id, username, "invalid", "некорректный пользователь")
            skip_cnt += 1
            log_warn(f"❌ Невалидный пользователь: {raw}")

        except ChatAdminRequiredError:
            ledger_put(conn, target_key, user_key, user_id, username, "stop", "нет прав на инвайт")
            log_stop(f"⛔ Нет прав на инвайт в {target_key}. Останавливаю прогон.")
            break

        except PeerFloodError:
            ledger_put(conn, target_key, user_key, user_id, username, "peerflood", "PeerFlood/лимит на аккаунте")
            log_stop("⛔ PeerFlood: переключаю на следующую сессию, чтобы не улететь в бан.")
            if not next_session():
                log_stop("⛔ Нет доступных сессий для переключения. Останавливаю прогон.")
                break
            fail_cnt += 1

        except RPCError as e:
            ledger_put(conn, target_key, user_key, user_id, username, "failed", f"{type(e).__name__}")
            fail_cnt += 1
            log_warn(f"⚠️ Ошибка RPC ({type(e).__name__}) для {raw}")

        except Exception as e:
            ledger_put(conn, target_key, user_key, user_id, username, "failed", f"{type(e).__name__}")
            fail_cnt += 1
            log_warn(f"⚠️ Неизвестная ошибка ({type(e).__name__}) для {raw}")

        # Лимит попыток на сессию (если включён)
        if client is not None and max_attempts_per_session and attempts_in_session >= int(max_attempts_per_session):
            log_info(
                f"🔁 Лимит попыток на сессию: {attempts_in_session} (max={int(max_attempts_per_session)}) — переключаюсь…"
            )
            time.sleep(random.uniform(2.0, 5.0))
            if not next_session():
                log_stop("⛔ Сессии закончились. Останавливаю прогон.")
                break

    log_ok(f"🏁 Инвайт завершён. Успех: {ok_cnt}, пропуск: {skip_cnt}, ошибки: {fail_cnt}")
    close_client(client)
    conn.close()


def inviting(client: TelegramClient, target: Union[str, int, Any], users: List[Union[str, int]], base_delay: float = 2.0) -> None:
    """
    Инвайт в target. Пользователи — список user_id (int) или username (str без @ или с @).
    Ведёт ledger, пропускает уже обработанных.
    """
    conn = _db()
    target_key = _target_key(target)

    log_info(f"🚀 Старт инвайта в: {target_key}. Кандидатов: {len(users)}")
    ok_cnt = 0
    skip_cnt = 0
    fail_cnt = 0

    # небольшая рандомизация
    delay = max(1.0, float(base_delay))

    for raw in users:
        # нормализуем ключ
        if isinstance(raw, int) or (isinstance(raw, str) and raw.strip().isdigit()):
            user_id = int(raw)
            username = None
            user_key = f"id:{user_id}"
            entity = user_id
        else:
            u = str(raw).strip()
            if u.startswith("@"):
                u = u[1:]
            username = u or None
            user_id = None
            user_key = f"u:{username}" if username else "u:None"
            entity = username if username else raw

        prev = ledger_get(conn, target_key, user_key)
        if prev and prev[0] in ("ok", "already", "privacy", "invalid"):
            skip_cnt += 1
            continue

        # джиттер перед действием
        time.sleep(delay + random.uniform(0.3, 1.2))

        try:
            client(InviteToChannelRequest(channel=target, users=[entity]))
            ledger_put(conn, target_key, user_key, user_id, username, "ok", "ok")
            ok_cnt += 1
            log_ok(f"✅ Инвайт отправлен: {('@'+username) if username else user_key} → {target_key}")

            # слегка плавающее увеличение/уменьшение
            delay = min(8.0, max(1.5, delay + random.uniform(-0.2, 0.4)))

        except UserAlreadyParticipantError:
            ledger_put(conn, target_key, user_key, user_id, username, "already", "уже участник")
            skip_cnt += 1
            log_info(f"👤 Уже в чате: {('@'+username) if username else user_key}")

        except UserPrivacyRestrictedError:
            ledger_put(conn, target_key, user_key, user_id, username, "privacy", "закрыты инвайты")
            skip_cnt += 1
            log_warn(f"🔒 Закрыты инвайты: {('@'+username) if username else user_key}")

        except FloodWaitError as e:
            # Telegram сказал ждать N секунд
            sec = int(getattr(e, "seconds", 0) or 0)
            ledger_put(conn, target_key, user_key, user_id, username, "floodwait", f"{sec}")
            log_pause(f"💤 FloodWait {sec} сек. Ожидаю и продолжаю…")
            time.sleep(sec + random.uniform(1.0, 3.0))
            # после floodwait делаем задержку побольше
            delay = min(12.0, max(delay, 6.0))
            fail_cnt += 1

        except (UsernameInvalidError, UserIdInvalidError):
            ledger_put(conn, target_key, user_key, user_id, username, "invalid", "некорректный пользователь")
            skip_cnt += 1
            log_warn(f"❌ Невалидный пользователь: {raw}")

        except ChatAdminRequiredError:
            ledger_put(conn, target_key, user_key, user_id, username, "stop", "нет прав на инвайт")
            log_stop(f"⛔ Нет прав на инвайт в {target_key}. Останавливаю прогон.")
            break

        except PeerFloodError:
            ledger_put(conn, target_key, user_key, user_id, username, "stop", "PeerFlood/лимит на аккаунте")
            log_stop("⛔ PeerFlood: аккаунт под лимитом/подозрением. Останавливаю прогон, чтобы не улететь в бан.")
            break

        except RPCError as e:
            # прочие телеграм-ошибки
            ledger_put(conn, target_key, user_key, user_id, username, "failed", f"{type(e).__name__}")
            fail_cnt += 1
            log_warn(f"⚠️ Ошибка RPC ({type(e).__name__}) для {raw}")

        except Exception as e:
            ledger_put(conn, target_key, user_key, user_id, username, "failed", f"{type(e).__name__}")
            fail_cnt += 1
            log_warn(f"⚠️ Неизвестная ошибка ({type(e).__name__}) для {raw}")

    log_ok(f"🏁 Инвайт завершён. Успех: {ok_cnt}, пропуск: {skip_cnt}, ошибки: {fail_cnt}")
    conn.close()

# -------------------- CONFIG UI --------------------

def _list_sessions() -> List[str]:
    return sorted([f for f in os.listdir(".") if f.endswith(".session")])

def _create_account_session(api_id: int, api_hash: str) -> None:
    os.system("cls||clear")
    phone = input("Введите номер телефона аккаунта (формат +79991234567): ").strip()
    if not phone:
        print("Пустой номер.")
        time.sleep(1.5)
        return

    # ВАЖНО: session = phone, Telethon создаст <phone>.session
    client = TelegramClient(
        phone,
        api_id,
        api_hash,
        device_model="iPhone 13 Pro",
        system_version="14.0",
        app_version="10.0",
        lang_code="en",
        system_lang_code="en-US",
    )
    print("Сейчас придёт код в Telegram. Введите код и (если спросит) пароль 2FA.")
    client.start(phone=phone)
    client.disconnect()

    log_ok(f"📲 Аккаунт добавлен: {phone}.session")
    print("Готово. Сессия создана.")
    time.sleep(1.5)

def config() -> None:
    ensure_options()
    while True:
        os.system("cls||clear")
        options = getoptions()
        sessions = _list_sessions()

        print("=== НАСТРОЙКИ ===")
        print(f"1 - Обновить api_id   [{options[0].strip()}]")
        print(f"2 - Обновить api_hash [{options[1].strip()}]")
        print(f"3 - Парсить user-id   [{options[2].strip()}]")
        print(f"4 - Парсить user-name [{options[3].strip()}]")
        print(f"5 - Добавить аккаунт  [{len(sessions)}]")
        print("6 - Сбросить настройки")
        print("e - Выход")
        key = input("Ввод: ").strip()

        if key == "1":
            os.system("cls||clear")
            options[0] = input("Введите API_ID: ").strip() + "\n"
        elif key == "2":
            os.system("cls||clear")
            options[1] = input("Введите API_HASH: ").strip() + "\n"
        elif key == "3":
            options[2] = "False\n" if options[2].strip() == "True" else "True\n"
        elif key == "4":
            options[3] = "False\n" if options[3].strip() == "True" else "True\n"
        elif key == "5":
            # создать новую сессию
            if options[0].strip() in ("NONEID", "") or options[1].strip() in ("NONEHASH", ""):
                print("Сначала задайте API_ID и API_HASH.")
                time.sleep(1.8)
                continue
            try:
                api_id = int(options[0].strip())
            except Exception:
                print("API_ID должен быть числом.")
                time.sleep(1.8)
                continue
            _create_account_session(api_id, options[1].strip())
        elif key == "6":
            os.system("cls||clear")
            answer = input("Сбросить API_ID/API_HASH и опции парсинга?\n1 - Да\n2 - Нет\nВвод: ").strip()
            if answer == "1":
                options = DEFAULT_OPTIONS.copy()
        elif key.lower() == "e":
            break
        else:
            print
            print("Неверный пункт.")
            time.sleep(1.0)
            continue

        # сохраняем изменения настроек
        with open("options.txt", "w", encoding="utf-8") as f:
            f.writelines(options)

        # небольшая пауза, чтобы меню не "мигало"
        time.sleep(0.2)


# -------------------------------------------------------------------
# (Опционально) экспортируем публичные функции для удобного импорта
__all__ = [
    "config",
    "getoptions",
    "parsing",
    "parsing_from_messages",
    "inviting",
    "inviting_rotate_sessions",
    "target_ref",
]
