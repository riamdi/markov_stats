import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path
from collections import Counter, deque

from config import ALLOWED_CHARS, MAX_ORDER, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def normalize_char(ch):
    ch = ch.lower()
    if ch in ALLOWED_CHARS:
        return ch
    if ch in ['\n', '\r', '\t']:
        return ' '
    return None


def create_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


def create_schema(conn):
    print("Создаём таблицы...")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS global_freqs (
                symbol TEXT PRIMARY KEY,
                count BIGINT NOT NULL,
                prob DOUBLE PRECISION
            );
        """)

        for n in range(1, MAX_ORDER + 1):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS transitions_{n} (
                    prefix TEXT NOT NULL,
                    next_symbol TEXT NOT NULL,
                    count BIGINT NOT NULL,
                    PRIMARY KEY (prefix, next_symbol)
                );
            """)

        conn.commit()


def stream_stats(conn, text_path):
    print("Собираем статистики...")
    print("Считаем глобальные частоты и переходы потоково...")

    global_counts = Counter()

    window = deque(maxlen=MAX_ORDER)

    batch = []
    BATCH_SIZE = 5000

    with conn.cursor() as cur, text_path.open("r", encoding="utf-8") as f:
        for line in f:
            for ch in line:
                norm = normalize_char(ch)
                if norm is None:
                    continue

                # Глобальная частота
                global_counts[norm] += 1

                if len(window) > 0:
                    for n in range(1, len(window) + 1):
                        prefix = "".join(list(window)[-n:])
                        batch.append((n, prefix, norm))

                window.append(norm)

                if len(batch) >= BATCH_SIZE:
                    flush_batch(cur, batch)
                    batch.clear()

        if batch:
            flush_batch(cur, batch)

        conn.commit()

    return global_counts


def flush_batch(cur, batch):
    tables = {}

    for n, prefix, next_ch in batch:
        if n not in tables:
            tables[n] = {}
        key = (prefix, next_ch)
        tables[n][key] = tables[n].get(key, 0) + 1

    for n, rows_dict in tables.items():
        rows = [(p, c, cnt) for (p, c), cnt in rows_dict.items()]

        execute_batch(
            cur,
            f"""
            INSERT INTO transitions_{n}(prefix, next_symbol, count)
            VALUES (%s, %s, %s)
            ON CONFLICT (prefix, next_symbol)
            DO UPDATE SET count = transitions_{n}.count + EXCLUDED.count
            """,
            rows,
            page_size=2000
        )


def save_global_freqs(conn, global_counts):
    total = sum(global_counts.values())
    rows = [(sym, cnt, cnt / total) for sym, cnt in global_counts.items()]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM global_freqs;")
        execute_batch(
            cur,
            "INSERT INTO global_freqs(symbol, count, prob) VALUES (%s, %s, %s)",
            rows
        )
        conn.commit()


def main():
    text_path = Path("../data/text.txt")

    if not text_path.exists():
        print("Файл ../data/text.txt не найден")
        return

    conn = create_connection()

    try:
        create_schema(conn)

        global_counts = stream_stats(conn, text_path)

        print("Сохраняем глобальные частоты...")
        save_global_freqs(conn, global_counts)

        print("Готово!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
