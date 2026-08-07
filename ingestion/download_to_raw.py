"""
Ingestion job - Buoc 1 cua pipeline: dua data "the gioi ben ngoai" vao lake
cua chung ta, o dang nguyen ban, khong bien doi (raw / bronze zone).

Nguyen tac quan trong (nen hoc thuoc, day la tu duy cua data engineer thuc su):

1. IMMUTABLE RAW ZONE
   Du lieu trong bucket "raw" la BAN GOC, khong bao gio sua/xoa/ghi de len.
   Neu sau nay phat hien transform sai logic, ta van con nguyen ban goc de
   chay lai tu dau. Day la "single source of truth" cua toan he thong.

2. IDEMPOTENCY (chay lai nhieu lan khong gay loi/khong tao du lieu trung)
   Truoc khi tai + upload 1 thang, ta kiem tra object da ton tai tren MinIO
   chua (head_object). Neu co roi thi skip. Nho vay ban co the chay script
   nay 10 lan/ngay ma khong so bi trung du lieu hay ton bang thong khong can
   thiet - dung y nhu mot Airflow task duoc retry.

3. HIVE-STYLE PARTITIONING (raw/yellow_tripdata/year=2024/month=01/...)
   Dat ten "path" theo year=/month= la quy uoc chuan cua Hive/Spark/Presto.
   Sau nay khi Spark hoac bat ky query engine nao doc thu muc nay, no co the
   "partition pruning" - vi du chi doc year=2024/month=01 ma khong can quet
   toan bo data - giup query nhanh hon rat nhieu tren du lieu lon.

Nguon du lieu: NYC TLC Trip Record Data (Yellow Taxi), file parquet cong khai,
khong can API key: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

import argparse
import io
import logging
import os
import sys
import time

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ingestion")

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VEHICLE_TYPE = "yellow"  # co the doi thanh "green", "fhv", "fhvhv"


def get_s3_client():
    """
    Tao client noi voi MinIO qua giao thuc S3 API.
    Day chinh la diem hay nhat cua MinIO: boto3 (SDK chinh thuc cua AWS S3)
    dung duoc voi MinIO ma khong can sua code gi - chi doi endpoint_url.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def download_month(year_month: str) -> bytes:
    """Tai file parquet cua 1 thang tu server cua NYC TLC ve memory."""
    filename = f"{VEHICLE_TYPE}_tripdata_{year_month}.parquet"
    url = f"{TLC_BASE_URL}/{filename}"
    log.info("Downloading %s ...", url)

    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    buf = io.BytesIO()
    downloaded = 0
    chunk_size = 1024 * 1024  # 1MB

    for chunk in resp.iter_content(chunk_size=chunk_size):
        buf.write(chunk)
        downloaded += len(chunk)
        if total:
            pct = downloaded / total * 100
            print(f"\r  {downloaded/1e6:6.1f}MB / {total/1e6:6.1f}MB ({pct:5.1f}%)", end="")
    print()

    buf.seek(0)
    return buf.read()


def upload_to_raw(s3, bucket: str, year_month: str, content: bytes, force: bool):
    year, month = year_month.split("-")
    filename = f"{VEHICLE_TYPE}_tripdata_{year_month}.parquet"
    # Hive-style partition path: de Spark/Presto/Athena doc sau nay deu hieu
    key = f"{VEHICLE_TYPE}_tripdata/year={year}/month={month}/{filename}"

    if not force and object_exists(s3, bucket, key):
        log.info("SKIP (da ton tai, idempotent check): s3://%s/%s", bucket, key)
        return key

    log.info("Uploading -> s3://%s/%s (%.1f MB)", bucket, key, len(content) / 1e6)
    s3.put_object(Bucket=bucket, Key=key, Body=content)
    return key


def main():
    parser = argparse.ArgumentParser(description="Ingest NYC TLC trip data vao MinIO raw zone")
    parser.add_argument(
        "--year-months",
        nargs="+",
        required=True,
        help="Danh sach thang can tai, dang YYYY-MM. Vi du: 2024-01 2024-02 2024-03",
    )
    parser.add_argument("--force", action="store_true", help="Tai lai va ghi de du lieu da co")
    args = parser.parse_args()

    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw")
    s3 = get_s3_client()

    results = []
    start = time.time()
    for ym in args.year_months:
        try:
            content = download_month(ym)
            key = upload_to_raw(s3, bucket, ym, content, args.force)
            results.append((ym, "OK", key))
        except requests.HTTPError as e:
            log.error("Thang %s: loi HTTP khi tai file - %s", ym, e)
            results.append((ym, "FAILED", str(e)))
        except Exception as e:
            log.error("Thang %s: loi khong xac dinh - %s", ym, e)
            results.append((ym, "FAILED", str(e)))

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info("Tong ket ingestion (%.1fs):", elapsed)
    for ym, status, info in results:
        log.info("  %s -> %s (%s)", ym, status, info)

    if any(status == "FAILED" for _, status, _ in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
