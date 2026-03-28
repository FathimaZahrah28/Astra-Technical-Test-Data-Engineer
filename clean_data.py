]
import logging
import os
import re
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "maju_jaya"),
}


def get_engine():
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)



def parse_dob(raw: str | None) -> str | None:
    if pd.isna(raw) or str(raw).strip() == "":
        return None

    raw = str(raw).strip()

    # Coba berbagai format umum
    formats = [
        "%Y-%m-%d",   # 1998-08-04
        "%Y/%m/%d",   # 1980/11/15
        "%d/%m/%Y",   # 14/01/1995
        "%d-%m-%Y",   # 14-01-1995
    ]
    for fmt in formats:
        try:
            parsed = pd.to_datetime(raw, format=fmt).date()
            # Validasi: tahun masuk akal (> 1920, tidak di masa depan)
            if parsed.year <= 1920 or parsed > date.today():
                return None          # placeholder / tidak masuk akal
            return str(parsed)
        except (ValueError, TypeError):
            continue

    return None   # tidak bisa di-parse


def detect_customer_type(name: str | None) -> str:
    if pd.isna(name):
        return "UNKNOWN"
    corporat_keywords = ["PT", "CV", "UD", "KOPERASI", "YAYASAN", "FIRMA", "LTD", "LLC"]
    name_upper = str(name).upper()
    if any(kw in name_upper.split() for kw in corporat_keywords):
        return "CORPORATE"
    return "INDIVIDUAL"


def clean_customers(engine) -> pd.DataFrame:
    log.info("Membersihkan customers_raw ...")
    df = pd.read_sql("SELECT * FROM customers_raw", engine)

    df["dob_clean"]       = df["dob"].apply(parse_dob)
    df["dob_flag"]        = df["dob"].apply(
        lambda x: "NULL" if pd.isna(x) else
                  ("INVALID_VALUE" if parse_dob(x) is None else "OK")
    )
    df["customer_type"]   = df["name"].apply(detect_customer_type)
    df["cleaned_at"]      = pd.Timestamp.now()

    log.info("  dob_flag counts:\n%s", df["dob_flag"].value_counts().to_string())
    log.info("  customer_type counts:\n%s", df["customer_type"].value_counts().to_string())

    return df


def clean_sales(engine) -> pd.DataFrame:
    log.info("Membersihkan sales_raw ...")
    df = pd.read_sql("SELECT * FROM sales_raw", engine)

    # Normalisasi price: hapus titik ribuan → integer
    df["price_clean"] = (
        df["price"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

    # Tandai duplikat: customer_id + model + invoice_date + price sama, VIN beda
    dup_cols = ["customer_id", "model", "invoice_date", "price_clean"]
    df["is_duplicate"] = df.duplicated(subset=dup_cols, keep="first")

    n_dup = df["is_duplicate"].sum()
    if n_dup:
        log.warning("  Ditemukan %d baris duplikat di sales_raw (ditandai, tidak dihapus).", n_dup)

    df["cleaned_at"] = pd.Timestamp.now()
    return df


def clean_after_sales(engine) -> pd.DataFrame:
    log.info("Membersihkan after_sales_raw ...")
    df         = pd.read_sql("SELECT * FROM after_sales_raw", engine)
    sales_df   = pd.read_sql("SELECT DISTINCT vin FROM sales_raw", engine)
    known_vins = set(sales_df["vin"])

    df["vin_in_sales"] = df["vin"].isin(known_vins)
    n_unknown = (~df["vin_in_sales"]).sum()
    if n_unknown:
        log.warning(
            "  %d baris after_sales punya VIN yang tidak ada di sales_raw (beli di luar / data error).",
            n_unknown
        )

    df["cleaned_at"] = pd.Timestamp.now()
    return df


def write_cleaned(df: pd.DataFrame, table: str, engine):
    df.to_sql(table, engine, if_exists="replace", index=False)
    log.info("Tabel '%s' ditulis: %d baris.", table, len(df))


def main():
    engine = get_engine()

    customers_clean   = clean_customers(engine)
    sales_clean       = clean_sales(engine)
    after_sales_clean = clean_after_sales(engine)

    write_cleaned(customers_clean,   "customers_clean",   engine)
    write_cleaned(sales_clean,       "sales_clean",       engine)
    write_cleaned(after_sales_clean, "after_sales_clean", engine)

    log.info("Semua tabel berhasil dibersihkan.")


if __name__ == "__main__":
    main()
