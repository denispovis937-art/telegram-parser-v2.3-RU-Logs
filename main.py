# -*- coding: utf-8 -*-
"""
telegram-parser-v2.3 (main)
Парсер + инвайтер. Жёсткий фильтр качества. RU-логи.

Файлы:
- options.txt
- usernames.txt / userids.txt
- invite_ledger.db
- app.log
"""

import os
import time
from typing import List, Optional, Union

from telethon.sync import TelegramClient

from defunc import (
    config,
    getoptions,
    parsing,
    parsing_from_messages,
    inviting,
    inviting_rotate_sessions,
    target_ref,
)


def yn(prompt: str) -> bool:
    raw = input(prompt).strip().lower()
    return raw in ("y", "yes", "д", "да")


def _fmt_dialog(d) -> str:
    ent = d.entity
    username = getattr(ent, "username", None)
    did = getattr(ent, "id", None)
    kind = "Чат"
    cls = ent.__class__.__name__.lower()
    if "channel" in cls:
        kind = "Канал"
    if "chat" in cls and "channel" not in cls:
        kind = "Группа"
    name = (d.name or "").strip() or "(без названия)"
    if username:
        return f"{kind}: {name}  (@{username})"
    if did is not None:
        return f"{kind}: {name}  (id:{did})"
    return f"{kind}: {name}"


def pick_dialog(client: TelegramClient, title: str):
    """Показывает список диалогов и возвращает entity (предпочтительно) либо введённую строку."""
    try:
        dialogs = client.get_dialogs(limit=200)
    except Exception:
        dialogs = []

    if not dialogs:
        print("Не удалось получить список диалогов. Вставь @username/ссылку/id вручную.")
        return input(title).strip() or None

    flt = input("Фильтр (часть названия) или Enter чтобы показать последние 50: ").strip().lower()
    if flt:
        dialogs = [d for d in dialogs if flt in (d.name or "").lower()]

    dialogs = dialogs[:50]
    print("\n=== ТВОИ ДИАЛОГИ (последние/по фильтру) ===")
    for i, d in enumerate(dialogs, 1):
        print(f"{i}. {_fmt_dialog(d)}")
    print("0. Ввести вручную")
    raw = input("Выбор: ").strip()
    if raw == "0":
        return input(title).strip() or None
    if not raw.isdigit():
        return None
    idx = int(raw)
    if idx < 1 or idx > len(dialogs):
        return None

    # ВАЖНО: возвращаем entity, а не id.
    return dialogs[idx - 1].entity


def clear() -> None:
    os.system("cls||clear")


def list_sessions() -> List[str]:
    return sorted([f for f in os.listdir(".") if f.endswith(".session")])


def pick_session() -> Optional[str]:
    sessions = list_sessions()
    if not sessions:
        print("Сессии не найдены. Зайди в Настройки → Добавить аккаунт.")
        time.sleep(2)
        return None

    print("=== АККАУНТЫ (.session) ===")
    for i, s in enumerate(sessions, 1):
        print(f"{i}. {s}")
    raw = input("Выбери номер аккаунта: ").strip()
    if not raw.isdigit():
        return None
    idx = int(raw)
    if idx < 1 or idx > len(sessions):
        return None
    return sessions[idx - 1]


def pick_sessions() -> List[str]:
    """Выбор нескольких .session для ротации.

    Ввод:
      - all
      - 1,2,5
      - 3
    """
    sessions = list_sessions()
    if not sessions:
        print("Сессии не найдены. Зайди в Настройки → Добавить аккаунт.")
        time.sleep(2)
        return []

    print("=== АККАУНТЫ (.session) ===")
    for i, s in enumerate(sessions, 1):
        print(f"{i}. {s}")

    raw = input("Выбери аккаунты (all или номера через запятую): ").strip().lower()
    if not raw:
        return []
    if raw == "all":
        return sessions

    out: List[str] = []
    for part in raw.split(","):
        p = part.strip()
        if not p.isdigit():
            continue
        idx = int(p)
        if 1 <= idx <= len(sessions):
            out.append(sessions[idx - 1])

    seen = set()
    uniq: List[str] = []
    for s in out:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


def make_client(session_file: str, api_id: int, api_hash: str) -> TelegramClient:
    session_name = session_file[:-8] if session_file.endswith(".session") else session_file
    client = TelegramClient(session_name, api_id, api_hash)
    client.connect()
    if not client.is_user_authorized():
        print("Сессия не авторизована. Создай её заново в Настройках (пункт 5).")
        raise SystemExit(1)
    return client


def do_parsing() -> None:
    clear()
    opts = getoptions()
    if opts[0].strip() in ("NONEID", "") or opts[1].strip() in ("NONEHASH", ""):
        print("Сначала задай API_ID и API_HASH в Настройках.")
        time.sleep(2)
        return

    sess = pick_session()
    if not sess:
        return

    api_id = int(opts[0].strip())
    api_hash = opts[1].strip()

    parse_id = (opts[2].strip() == "True")
    parse_name = (opts[3].strip() == "True")

    client = make_client(sess, api_id, api_hash)
    src = pick_dialog(client, "Источник (чат/канал) для парсинга (@username/ссылка/id): ")
    if not src:
        client.disconnect()
        return
    try:
        parsing(client, src, parse_id=parse_id, parse_name=parse_name)
        print("Готово. Смотри usernames.txt / userids.txt и app.log")
    finally:
        client.disconnect()
        time.sleep(1.5)


def _load_users_from_files() -> List[Union[str, int]]:
    users: List[Union[str, int]] = []
    if os.path.exists("userids.txt"):
        with open("userids.txt", "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s.isdigit():
                    users.append(int(s))
    if os.path.exists("usernames.txt"):
        with open("usernames.txt", "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                if s.startswith("@"):
                    s = s[1:]
                if s:
                    users.append(s)

    seen = set()
    uniq = []
    for u in users:
        k = ("id", u) if isinstance(u, int) else ("u", u.lower())
        if k in seen:
            continue
        seen.add(k)
        uniq.append(u)
    return uniq


def do_parsing_messages() -> None:
    clear()
    opts = getoptions()
    if opts[0].strip() in ("NONEID", "") or opts[1].strip() in ("NONEHASH", ""):
        print("Сначала задай API_ID и API_HASH в Настройках.")
        time.sleep(2)
        return

    sess = pick_session()
    if not sess:
        return

    api_id = int(opts[0].strip())
    api_hash = opts[1].strip()

    client = make_client(sess, api_id, api_hash)
    src = pick_dialog(client, "Источник (чат/канал/группа): ")
    if not src:
        client.disconnect()
        return

    parse_name = yn("Парсить usernames? (y/n): ")
    parse_id = yn("Парсить user ids? (y/n): ")
    if not (parse_name or parse_id):
        print("Нечего парсить — выбери хотя бы usernames или ids.")
        time.sleep(2)
        return

    lm_raw = input("Сколько сообщений смотреть? (по умолчанию 5000): ").strip()
    limit_messages = int(lm_raw) if lm_raw.isdigit() else 5000
    if limit_messages > 200000:
        print("⚠️ Очень большой лимит сообщений. Обычно хватает 5000–50000.", flush=True)

    days_raw = input("Макс. возраст сообщений в днях (по умолчанию 7): ").strip()
    max_days = int(days_raw) if days_raw.isdigit() else 7
    if max_days > 30:
        print("⚠️ Возраст 30+ дней увеличит время и снизит качество базы.", flush=True)

    try:
        print("Запускаю парсинг из сообщений…", flush=True)
        parsing_from_messages(
            client,
            src,
            parse_id=parse_id,
            parse_name=parse_name,
            limit_messages=limit_messages,
            max_age_days=max_days,
        )
        print("Готово. Смотри usernames.txt / userids.txt и app.log")
    finally:
        client.disconnect()
        time.sleep(1.5)


def do_inviting() -> None:
    clear()
    opts = getoptions()
    if opts[0].strip() in ("NONEID", "") or opts[1].strip() in ("NONEHASH", ""):
        print("Сначала задай API_ID и API_HASH в Настройках.")
        time.sleep(2)
        return

    sess_list = pick_sessions()
    if not sess_list:
        return

    api_id = int(opts[0].strip())
    api_hash = opts[1].strip()

    # Берём первую сессию, чтобы выбрать цель из диалогов
    client = make_client(sess_list[0], api_id, api_hash)
    target_entity = pick_dialog(client, "Куда инвайтить? (@username/ссылка/id): ")
    if not target_entity:
        client.disconnect()
        return

    # Важно: делаем target переносимым между сессиями
    target = target_ref(target_entity)

    users = _load_users_from_files()
    if not users:
        print("Списки пустые. Сначала сделай Парсинг.")
        time.sleep(2)
        return

    raw_delay = input("Базовая задержка между попытками (сек), по умолчанию 2.0: ").strip()
    try:
        base_delay = float(raw_delay) if raw_delay else 2.0
    except Exception:
        base_delay = 2.0

    try:
        if len(sess_list) == 1:
            inviting(client, target_entity, users, base_delay=base_delay)
        else:
            # закрываем первый клиент, дальше будут открываться по мере ротации
            client.disconnect()

            re_raw = input(
                "Плановая смена сессии каждые N успешных инвайтов (0 = только по флуду), по умолчанию 0: "
            ).strip()
            ma_raw = input("Максимум попыток на одну сессию N (0 = без лимита), по умолчанию 0: ").strip()
            try:
                rotate_every = int(re_raw) if re_raw else 0
            except Exception:
                rotate_every = 0
            try:
                max_attempts = int(ma_raw) if ma_raw else 0
            except Exception:
                max_attempts = 0

            inviting_rotate_sessions(
                api_id=api_id,
                api_hash=api_hash,
                session_files=sess_list,
                target=target,
                users=users,
                base_delay=base_delay,
                rotate_every=rotate_every,
                max_attempts_per_session=max_attempts,
            )
        print("Готово. Смотри invite_ledger.db и app.log")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass
        time.sleep(1.5)


def main() -> None:
    while True:
        clear()
        print("=== TELEGRAM PARSER / INVITER v2.3 ===")
        print("1 - Настройки")
        print("2 - Парсинг участников (если список виден)")
        print("3 - Парсинг из сообщений (если список скрыт)")
        print("4 - Инвайт из usernames.txt / userids.txt (с учётом ledger)")
        print("5 - Выход")
        key = input("Ввод: ").strip()

        if key == "1":
            config()
        elif key == "2":
            do_parsing()
        elif key == "3":
            do_parsing_messages()
        elif key == "4":
            do_inviting()
        elif key == "5":
            break
        else:
            print("Неверный пункт.")
            time.sleep(1)


if __name__ == "__main__":
    main()
