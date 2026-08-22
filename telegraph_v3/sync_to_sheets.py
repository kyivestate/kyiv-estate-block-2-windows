#!/usr/bin/env python3
"""Copy only validated Block 3 URLs into the matching Active Google Sheet."""
from __future__ import annotations

import json
import os
import random
import sys
import time
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path

import gspread
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from parser_v2.services.sheets_lock import SheetsLock

load_dotenv(ROOT / ".env")
load_dotenv(ROOT / "houses_v1" / ".env")
CREDS = Path(os.getenv("GOOGLE_CREDENTIALS_FILE", ROOT.parent / "olx-parser" / "ads-collector" / "real-estate-platform-484610-a5a172df3957.json"))
BOOKS = {
    "apartments": (os.getenv("ACTIVE_SHEET_ID", "1RY4BiRospnPYLFoW2LLJleDgi08yomwhtUlKKvSpkr8"), {"rent": "Оренда", "buy": "Продаж"}),
    "houses": (os.getenv("HOUSES_ACTIVE_SHEET_ID", ""), {"rent": "Оренда", "buy": "Продаж"}),
}
COMMERCIAL = ROOT / "commercial_v1" / ".sheets.json"
URL = "https://telegra.ph/"


def column_name(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def retry(call, attempts: int = 6):
    error = None
    for attempt in range(attempts):
        try:
            return call()
        except APIError as exc:
            if getattr(getattr(exc, "response", None), "status_code", 0) not in {429, 500, 502, 503, 504}:
                raise
            error = exc
            time.sleep(min(30, 2**attempt) + random.uniform(0, .5))
    raise error


def worksheet(client, catalog: str, operation: str):
    if catalog == "commercial":
        config = json.loads(COMMERCIAL.read_text(encoding="utf-8"))
        return retry(lambda: client.open_by_key(config["active"]["id"]).worksheet(config["active"]["tabs"][operation]))
    book_id, tabs = BOOKS[catalog]
    if not book_id:
        raise RuntimeError("HOUSES_ACTIVE_SHEET_ID is not configured")
    return retry(lambda: client.open_by_key(book_id).worksheet(tabs[operation]))


def indexes(ws, catalog: str):



    if catalog == "commercial" and ws.col_count > 58:
        retry(lambda: ws.resize(cols=58))
    header = retry(lambda: ws.row_values(1))
    missing = [name for name in ("Telegraph UA", "Telegraph EN") if name not in header]
    if missing:
        start = len(header) + 1
        required_columns = start + len(missing) - 1


        if ws.col_count < required_columns:
            retry(lambda: ws.add_cols(required_columns - ws.col_count))
        retry(lambda: ws.update(range_name=f"{column_name(start)}1:{column_name(start + len(missing)-1)}1", values=[missing], value_input_option="USER_ENTERED"))
        header += missing
    key = "Ext ID" if "Ext ID" in header else "ID"
    if key not in header:
        raise RuntimeError(f"{ws.title}: missing ID column")
    return header.index(key) + 1, {"ua": header.index("Telegraph UA") + 1, "en": header.index("Telegraph EN") + 1}


def pending(cur, include_synced: bool = False):
    query = """
        SELECT p.catalog, p.listing_id, p.ua_url, p.en_url,
               COALESCE(apartment.external_id, house.external_id, commercial.external_id) AS sheet_key
        FROM block3.publications p
        LEFT JOIN active_listings apartment
          ON p.catalog='apartments' AND apartment.id=p.listing_id AND apartment.status='active'
        LEFT JOIN houses_listings house
          ON p.catalog='houses' AND house.id=p.listing_id AND house.status='active'
        LEFT JOIN commercial_listings commercial
          ON p.catalog='commercial' AND commercial.id=p.listing_id AND commercial.status='active'
        WHERE p.status='published'
          AND COALESCE(apartment.id, house.id, commercial.id) IS NOT NULL
    """
    if not include_synced:
        query += " AND (p.synced_at IS NULL OR p.updated_at > p.synced_at)"
    query += """
        ORDER BY p.updated_at,p.catalog,p.listing_id
    """
    cur.execute(query)
    items = []
    for row in cur.fetchall():
        for locale, url in (("ua", row["ua_url"]), ("en", row["en_url"])):
            if isinstance(url, str) and url.startswith(URL):
                items.append({**dict(row), "locale": locale, "url": url})
    return items


def sync_group(cur, client, catalog, operation, items):
    ws = worksheet(client, catalog, operation)
    id_column, outputs = indexes(ws, catalog)
    ids = retry(lambda: ws.col_values(id_column))
    rows = {str(value).strip(): number for number, value in enumerate(ids[1:], 2) if str(value).strip()}
    existing = {
        locale: retry(lambda locale=locale: ws.col_values(outputs[locale]))
        for locale in ("ua", "en")
    }
    updates, unchanged, rejected = [], [], []
    for item in items:
        row = rows.get(str(item["sheet_key"]).strip())
        if row is None:
            rejected.append(item)
        else:
            current = existing[item["locale"]][row - 1] if len(existing[item["locale"]]) >= row else ""
            if current.strip() == item["url"]:
                unchanged.append(item)
            else:
                updates.append((item, {"range": f"{column_name(outputs[item['locale']])}{row}", "values": [[item["url"]]]}))
    for start in range(0, len(updates), 500):
        chunk = updates[start:start + 500]
        retry(lambda: ws.batch_update([change for _, change in chunk], value_input_option="USER_ENTERED"))

    completed = {item["listing_id"] for item, _ in updates} | {item["listing_id"] for item in unchanged}
    rejected_ids = {item["listing_id"] for item in rejected}
    completed -= rejected_ids
    if completed:
        cur.execute("UPDATE block3.publications SET synced_at=now(),sync_error=NULL WHERE catalog=%s AND listing_id = ANY(%s)", (catalog, list(completed)))
    if rejected_ids:
        cur.execute("UPDATE block3.publications SET synced_at=updated_at,sync_error=NULL WHERE catalog=%s AND listing_id = ANY(%s)", (catalog, list(rejected_ids)))
    return {"written": len(updates), "unchanged": len(unchanged), "not_in_active_sheet": len(rejected)}


def main():
    parser = ArgumentParser()
    parser.add_argument("--resync-all", action="store_true")
    args = parser.parse_args()
    if not CREDS.is_file() or not COMMERCIAL.is_file():
        raise RuntimeError("Google Sheets configuration is incomplete")
    with psycopg2.connect(host=os.getenv("PG_HOST", "/tmp"), port=os.getenv("PG_PORT", "5432"), dbname=os.getenv("PG_DBNAME", "real_estate"), user=os.getenv("PG_USER", "admin"), password=os.getenv("PG_PASSWORD", "")) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            items = pending(cur, include_synced=args.resync_all)
            groups = defaultdict(list)

            if items:
                cur.execute("SELECT catalog,listing_id,operation FROM block3.publications WHERE status='published'")
                operations = {(row["catalog"], row["listing_id"]): row["operation"] for row in cur.fetchall()}
                for item in items:
                    groups[(item["catalog"], operations[(item["catalog"], item["listing_id"])])].append(item)
            client = gspread.authorize(Credentials.from_service_account_file(str(CREDS), scopes=["https://www.googleapis.com/auth/spreadsheets"]))
            result = {"written_cells": 0, "unchanged_cells": 0, "not_in_active_sheet_cells": 0, "groups": {}, "resync_all": args.resync_all}
            try:
                with SheetsLock("block3_sync_to_sheets"):
                    for key, group in groups.items():
                        outcome = sync_group(cur, client, *key, group)
                        result["written_cells"] += outcome["written"]
                        result["unchanged_cells"] += outcome["unchanged"]
                        result["not_in_active_sheet_cells"] += outcome["not_in_active_sheet"]
                        result["groups"][":".join(key)] = outcome
            except RuntimeError as error:
                if not str(error).startswith("Sheets writer already running:"):
                    raise
                result["busy"] = True
            print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
