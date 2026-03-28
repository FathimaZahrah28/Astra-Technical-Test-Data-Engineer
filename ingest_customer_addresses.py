import os
import sys
import glob
import logging
import argparse
from datetime import datetime, date

import pandas as pd
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("ingest_customer_addresses.log"),
    ],
)
log = logging.getLogger(__name__)


DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "maju_jaya"),
}

SOURCE_FOLDER   = os.getenv("SOURCE_FOLDER", "./data/incoming")
PROCESSED_FOLDER = os.getenv("PROCESSED_FOLDER", "./data/processed")
FILE_PREFIX     = "customer_addresses_"
TARGET_TABLE    = "customer_addresses"



def get_engine():
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)


def resolve_file(args) -> str:
    if args.file:
        if not os.path.exists(args.file):
            raise FileNotFoundError(f"File tidak ditemukan: {args.file}")
        return args.file

    target_date = args.date or date.today().strftime("%Y%m%d")
    pattern = os.path.join(SOURCE_FOLDER, f"{FILE_PREFIX}{target_date}.csv")
    matches = glob.glob(pattern)

    if not matches:
        raise FileNotFoundError(
            f"File CSV tidak ditemukan untuk tanggal {target_date}: {pattern}"
        )
    if len(matches) > 1:
        log.warning("Ditemukan lebih dari 1 file, menggunakan yang pertama: %s", matches[0])

    return matches[0]


def read_and_validate(filepath: str) -> pd.DataFrame:
    required_cols = {"customer_id", "address", "city", "province"}

    log.info("Membaca file: %s", filepath)
    df = pd.read_csv(filepath, dtype=str)

    # Normalisasi nama kolom (lowercase, strip spasi)
    df.columns = df.columns.str.strip().str.lower()

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Kolom wajib tidak ada: {missing}")

    log.info("Jumlah baris terbaca: %d", len(df))
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Strip whitespace semua kolom string
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    # Buang baris yang customer_id-nya kosong/NaN
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    df["customer_id"] = df["customer_id"].astype(int)
    after = len(df)
    if before != after:
        log.warning("Dibuang %d baris karena customer_id kosong.", before - after)

    # Tambah kolom metadata
    df["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df


def ensure_table(engine):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        customer_id   INT          NOT NULL,
        address       TEXT,
        city          VARCHAR(100),
        province      VARCHAR(100),
        created_at    DATETIME,
        ingested_at   DATETIME,
        UNIQUE KEY uq_customer (customer_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    log.info("Tabel '%s' siap.", TARGET_TABLE)


def upsert(df: pd.DataFrame, engine):
    cols = [c for c in df.columns if c != "id"]
    placeholders = ", ".join([f":{c}" for c in cols])
    col_list     = ", ".join(cols)
    updates      = ", ".join([f"{c} = VALUES({c})" for c in cols if c != "customer_id"])

    sql = text(
        f"INSERT INTO {TARGET_TABLE} ({col_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )

    records = df[cols].to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(sql, records)

    log.info("Upsert selesai: %d baris.", len(records))


def move_to_processed(filepath: str):
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    filename = os.path.basename(filepath)
    dest = os.path.join(PROCESSED_FOLDER, filename)
    os.rename(filepath, dest)
    log.info("File dipindahkan ke: %s", dest)


def main():
    parser = argparse.ArgumentParser(description="Ingest customer_addresses CSV ke MySQL")
    parser.add_argument("--date", help="Tanggal target (format: YYYYMMDD), default hari ini")
    parser.add_argument("--file", help="Path file CSV langsung")
    args = parser.parse_args()

    try:
        filepath = resolve_file(args)
        df       = read_and_validate(filepath)
        df       = clean(df)
        engine   = get_engine()

        ensure_table(engine)
        upsert(df, engine)
        move_to_processed(filepath)

        log.info("Pipeline selesai sukses.")

    except FileNotFoundError as e:
        log.error("File tidak ditemukan: %s", e)
        sys.exit(1)
    except Exception as e:
        log.exception("Pipeline gagal: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
