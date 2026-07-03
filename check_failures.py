"""
check_failures.py — IPBD Kelompok 11
CLI tool untuk cek histori job scheduling yang gagal.
Menggunakan Prefect API untuk query flow runs dengan state FAILED.

Usage:
  python check_failures.py                        # default 24 jam
  python check_failures.py --hours 48             # 48 jam terakhir
  python check_failures.py --hours 24 --telegram  # + kirim ke grup
  python check_failures.py --all                  # semua failed runs
"""

import argparse
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from failure_notifier import check_failed_jobs, report_to_telegram


def main():
    parser = argparse.ArgumentParser(
        description="Cek histori job scheduling yang gagal"
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Jumlah jam ke belakang (default: 24)"
    )
    parser.add_argument(
        "--telegram", "-t", action="store_true",
        help="Kirim hasilnya ke grup Telegram"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ambil semua failed runs (tanpa batas jam)"
    )
    args = parser.parse_args()

    hours = args.hours if not args.all else 9999
    logger.info(f"Mencari job gagal dalam {hours} jam terakhir...")

    failed = check_failed_jobs(hours=hours)

    if not failed:
        msg = f"✅ Tidak ada job gagal dalam {hours} jam terakhir."
        print(msg)
        if args.telegram:
            report_to_telegram([], hours=hours)
        return

    print(f"\n{'='*60}")
    print(f"  JOB FAILURE REPORT — {hours}h terakhir")
    print(f"  Total: {len(failed)} flow(s) gagal")
    print(f"{'='*60}\n")

    for i, job in enumerate(failed, 1):
        print(f"{i}. {job['flow_name']}")
        print(f"   Run ID    : {job['run_id']}")
        print(f"   Timestamp : {job['timestamp']}")
        print(f"   Error     : {job['error'][:200]}")
        print()

    if args.telegram:
        print("Mengirim ke Telegram...")
        report_to_telegram(failed, hours=hours)
        print("✅ Terkirim!")


if __name__ == "__main__":
    main()
