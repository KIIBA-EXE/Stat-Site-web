#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pytz
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from notion_client import Client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ----------------------------
# Config & helpers
# ----------------------------

@dataclass
class Config:
    notion_token: str
    notion_database_id: Optional[str]
    google_sa_json: str
    notion_rate_limit_per_sec: float = 3.0
    # IDs de base optionnels par appareil
    notion_db_desktop: Optional[str] = None
    notion_db_mobile: Optional[str] = None
    notion_db_tablet: Optional[str] = None


def load_config() -> Config:
    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID") or None
    gsa = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "gcp-service-account.json")
    rate = float(os.getenv("NOTION_RATE_LIMIT_PER_SEC", "3"))
    db_desktop = os.getenv("NOTION_DATABASE_ID_DESKTOP") or None
    db_mobile = os.getenv("NOTION_DATABASE_ID_MOBILE") or None
    db_tablet = os.getenv("NOTION_DATABASE_ID_TABLET") or None

    if not token:
        print("ERROR: NOTION_TOKEN est requis dans .env", file=sys.stderr)
        sys.exit(1)

    # Exiger un ID par défaut sauf si les trois bases par appareil sont fournies
    if not dbid and not (db_desktop and db_mobile and db_tablet):
        print(
            "ERROR: Fournissez NOTION_DATABASE_ID (base par défaut) ou bien toutes les bases par appareil (NOTION_DATABASE_ID_DESKTOP/MOBILE/TABLET)",
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.exists(gsa):
        print(f"ERROR: Fichier Service Account JSON introuvable: {gsa}", file=sys.stderr)
        sys.exit(1)

    return Config(token, dbid, gsa, rate, db_desktop, db_mobile, db_tablet)


# ----------------------------
# Google Search Console client
# ----------------------------

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def build_gsc_service(sa_json_path: str):
    creds = service_account.Credentials.from_service_account_file(sa_json_path, scopes=SCOPES)
    service = build("searchconsole", "v1", credentials=creds, cache_discovery=False)
    return service


@dataclass(frozen=True)
class RowKey:
    date: str
    query: str
    page: str
    country: str
    device: str

    def as_string(self) -> str:
        return "|".join([self.date, self.query, self.page, self.country, self.device])


# ----------------------------
# Notion helpers (upsert)
# ----------------------------

class NotionIO:
    def __init__(self, token: str, database_id: str, rate_limit_per_sec: float = 3.0):
        self.client = Client(auth=token)
        self.database_id = database_id
        self.min_interval = 1.0 / max(rate_limit_per_sec, 0.1)
        self.last_call = 0.0

    def _throttle(self):
        now = time.time()
        delta = now - self.last_call
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last_call = time.time()

    def _page_properties_from_row(self, key: RowKey, metrics: Dict[str, float]) -> Dict:
        # Schéma attendu côté Notion (en FR):
        # Clé (Titre), Date (Date), Requête (Rich text), Page (URL), Pays (Select), Appareil (Select),
        # Clics / Impressions / CTR / Position (Number)
        return {
            "Clé": {"title": [{"text": {"content": key.as_string()}}]},
            "Date": {"date": {"start": key.date}},
            "Requête": {"rich_text": [{"text": {"content": key.query[:2000]}}]},
            "Page": {"url": key.page},
            "Pays": {"select": {"name": key.country or ""}},
            "Appareil": {"select": {"name": key.device or ""}},
            "Clics": {"number": float(metrics.get("clicks", 0))},
            "Impressions": {"number": float(metrics.get("impressions", 0))},
            "CTR": {"number": float(metrics.get("ctr", 0))},
            "Position": {"number": float(metrics.get("position", 0))},
        }

    def _page_properties_from_weekly(self, week_start: str, device: str, metrics: Dict[str, float]) -> Dict:
        # Utilise le schéma existant: Clé, Date, Appareil, Clics, Impressions, CTR, Position
        key_str = f"{week_start}|{device}|weekly"
        return {
            "Clé": {"title": [{"text": {"content": key_str}}]},
            "Date": {"date": {"start": week_start}},
            "Appareil": {"select": {"name": device or ""}},
            "Clics": {"number": float(metrics.get("clicks", 0))},
            "Impressions": {"number": float(metrics.get("impressions", 0))},
            "CTR": {"number": float(metrics.get("ctr", 0))},
            "Position": {"number": float(metrics.get("position", 0))},
        }

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def find_page_by_key(self, key_str: str) -> Optional[str]:
        self._throttle()
        resp = self.client.databases.query(
            **{
                "database_id": self.database_id,
                "filter": {"property": "Clé", "title": {"equals": key_str}},
                "page_size": 1,
            }
        )
        results = resp.get("results", [])
        if results:
            return results[0]["id"]
        return None

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def create_page(self, props: Dict) -> str:
        self._throttle()
        resp = self.client.pages.create(parent={"database_id": self.database_id}, properties=props)
        return resp["id"]

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def update_page(self, page_id: str, props: Dict) -> None:
        self._throttle()
        self.client.pages.update(page_id=page_id, properties=props)

    def upsert_row(self, key: RowKey, metrics: Dict[str, float]):
        props = self._page_properties_from_row(key, metrics)
        page_id = self.find_page_by_key(key.as_string())
        if page_id:
            self.update_page(page_id, props)
        else:
            self.create_page(props)

    def upsert_weekly(self, week_start: str, device: str, metrics: Dict[str, float]):
        key_str = f"{week_start}|{device}|weekly"
        props = self._page_properties_from_weekly(week_start, device, metrics)
        page_id = self.find_page_by_key(key_str)
        if page_id:
            self.update_page(page_id, props)
        else:
            self.create_page(props)

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def find_page_by_key_in_db(self, database_id: str, key_str: str) -> Optional[str]:
        self._throttle()
        resp = self.client.databases.query(
            **{
                "database_id": database_id,
                "filter": {"property": "Clé", "title": {"equals": key_str}},
                "page_size": 1,
            }
        )
        results = resp.get("results", [])
        if results:
            return results[0]["id"]
        return None

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def create_page_in_db(self, database_id: str, props: Dict) -> str:
        self._throttle()
        resp = self.client.pages.create(parent={"database_id": database_id}, properties=props)
        return resp["id"]

    # Variante: upsert dans une base spécifique
    def upsert_weekly_in_db(self, database_id: str, week_start: str, device: str, metrics: Dict[str, float]):
        key_str = f"{week_start}|{device}|weekly"
        props = self._page_properties_from_weekly(week_start, device, metrics)
        page_id = self.find_page_by_key_in_db(database_id, key_str)
        if page_id:
            self.update_page(page_id, props)
        else:
            self.create_page_in_db(database_id, props)


# ----------------------------
# GSC fetch
# ----------------------------

@retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(5), reraise=True)
def gsc_query(service, site_url: str, start_date: str, end_date: str, row_limit: int = 25000,
              dimensions: List[str] = None, dimension_filter_groups: List[Dict] = None, start_row: int = 0) -> Dict:
    if dimensions is None:
        dimensions = ["date", "query", "page", "country", "device"]
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": dimensions,
        "rowLimit": row_limit,
        "startRow": start_row,
    }
    if dimension_filter_groups:
        body["dimensionFilterGroups"] = dimension_filter_groups
    return service.searchanalytics().query(siteUrl=site_url, body=body).execute()


def daterange(start: dt.date, end: dt.date):
    for n in range(int((end - start).days) + 1):
        yield start + dt.timedelta(n)


def date_str(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# Helpers semaine

def week_start_date(d: dt.date) -> dt.date:
    # Lundi comme début de semaine (ISO)
    return d - dt.timedelta(days=d.weekday())

# ----------------------------
# Diagnostics
# ----------------------------

def list_sites(service) -> None:
    try:
        resp = service.sites().list().execute()
        entries = resp.get("siteEntry", [])
        print("Sites visibles par ce compte de service:")
        if not entries:
            print("(aucune propriété trouvée)")
        for e in entries:
            print(f"- {e.get('permissionLevel')} | {e.get('siteUrl')}")
    except Exception as e:
        print(f"Echec list_sites: {e}", file=sys.stderr)

# ----------------------------
# Main sync logic
# ----------------------------

def compute_window(args) -> Tuple[str, str]:
    if args.start and args.end:
        return args.start, args.end
    # rolling window: days_back excluding lag_days
    today = dt.datetime.now(pytz.utc).date()
    end = today - dt.timedelta(days=max(args.lag_days, 0))
    start = end - dt.timedelta(days=max(args.days_back - 1, 0))
    return date_str(start), date_str(end)


def build_filters(country: Optional[str], device: Optional[str]) -> Optional[List[Dict]]:
    filters = []
    if country:
        filters.append({"dimension": "country", "operator": "equals", "expression": country})
    if device:
        filters.append({"dimension": "device", "operator": "equals", "expression": device})
    if filters:
        return [{"filters": filters}]
    return None


def main():
    parser = argparse.ArgumentParser(description="Sync Google Search Console data to Notion database")
    parser.add_argument("--site-url", required=True, help="Ex: https://danslesbottes.fr/ ou sc-domain:danslesbottes.fr")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--days-back", type=int, default=3)
    parser.add_argument("--lag-days", type=int, default=2)
    parser.add_argument("--row-limit", type=int, default=25000)
    parser.add_argument("--country", help="Filtre pays, ex: FRA")
    parser.add_argument("--device", help="Filtre device, ex: DESKTOP/MOBILE/TABLET")
    parser.add_argument("--list-sites", action="store_true", help="Lister les propriétés GSC accessibles et quitter")
    parser.add_argument("--mode", choices=["detail", "weekly-device"], default="detail", help="Mode de synchronisation: détail (par requête/page) ou hebdo par appareil")

    args = parser.parse_args()

    cfg = load_config()
    service = build_gsc_service(cfg.google_sa_json)

    if args.list_sites:
        list_sites(service)
        return

    notion = NotionIO(cfg.notion_token, cfg.notion_database_id, cfg.notion_rate_limit_per_sec)

    start_date, end_date = compute_window(args)
    print(f"Sync window: {start_date} -> {end_date}")

    filters = build_filters(args.country, args.device)

    if args.mode == "weekly-device":
        # Mapping optionnel appareil -> base Notion
        device_db_map: Dict[str, str] = {}
        if cfg.notion_db_desktop:
            device_db_map["DESKTOP"] = cfg.notion_db_desktop
        if cfg.notion_db_mobile:
            device_db_map["MOBILE"] = cfg.notion_db_mobile
        if cfg.notion_db_tablet:
            device_db_map["TABLET"] = cfg.notion_db_tablet

        # Récupération et agrégation
        start_row = 0
        agg: Dict[Tuple[str, str], Dict[str, float]] = {}
        while True:
            resp = gsc_query(
                service,
                site_url=args.site_url,
                start_date=start_date,
                end_date=end_date,
                row_limit=args.row_limit,
                dimensions=["date", "device"],
                dimension_filter_groups=filters,
                start_row=start_row,
            )
            rows = resp.get("rows", [])
            if not rows:
                break
            for r in rows:
                keys = r.get("keys", [])
                if len(keys) != 2:
                    continue
                d = dt.datetime.strptime(keys[0], "%Y-%m-%d").date()
                week_start = week_start_date(d)
                week_key = date_str(week_start)
                device = keys[1]
                clicks = float(r.get("clicks", 0))
                impressions = float(r.get("impressions", 0))
                position = float(r.get("position", 0))
                bucket = agg.setdefault((week_key, device), {"clicks": 0.0, "impressions": 0.0, "pos_weighted": 0.0})
                bucket["clicks"] += clicks
                bucket["impressions"] += impressions
                bucket["pos_weighted"] += position * impressions
            start_row += len(rows)
            if len(rows) < args.row_limit:
                break
        # Envoi vers Notion
        for (week_key, device), vals in agg.items():
            impressions = vals["impressions"]
            clicks = vals["clicks"]
            ctr = (clicks / impressions) if impressions else 0.0
            position = (vals["pos_weighted"] / impressions) if impressions else 0.0
            metrics = {"clicks": clicks, "impressions": impressions, "ctr": ctr, "position": position}
            try:
                target_db = device_db_map.get(device) or cfg.notion_database_id
                if target_db == cfg.notion_database_id:
                    notion.upsert_weekly(week_key, device, metrics)
                else:
                    notion.upsert_weekly_in_db(target_db, week_key, device, metrics)
            except Exception as e:
                print(f"Notion upsert weekly failed for {week_key}|{device}: {e}", file=sys.stderr)
        print(f"Hebdo par appareil: {len(agg)} lignes agrégées et synchronisées")
        return

    # Pull per-day to simplify upsert granularity and debug
    for day in daterange(dt.datetime.strptime(start_date, "%Y-%m-%d").date(), dt.datetime.strptime(end_date, "%Y-%m-%d").date()):
        d = date_str(day)
        print(f"Fetching {d} ...")
        start_row = 0
        total_rows = 0
        while True:
            resp = gsc_query(
                service,
                site_url=args.site_url,
                start_date=d,
                end_date=d,
                row_limit=args.row_limit,
                dimensions=["date", "query", "page", "country", "device"],
                dimension_filter_groups=filters,
                start_row=start_row,
            )
            rows = resp.get("rows", [])
            if not rows:
                break
            for r in rows:
                keys = r.get("keys", [])
                if len(keys) != 5:
                    continue
                key = RowKey(
                    date=keys[0],
                    query=keys[1],
                    page=keys[2],
                    country=keys[3],
                    device=keys[4],
                )
                metrics = {
                    "clicks": r.get("clicks", 0),
                    "impressions": r.get("impressions", 0),
                    "ctr": r.get("ctr", 0),
                    "position": r.get("position", 0),
                }
                try:
                    notion.upsert_row(key, metrics)
                except Exception as e:
                    # Log et continue
                    print(f"Notion upsert failed for {key.as_string()}: {e}", file=sys.stderr)
            total_rows += len(rows)
            start_row += len(rows)
            if len(rows) < args.row_limit:
                break
        print(f"{d}: {total_rows} rows processed")


if __name__ == "__main__":
    main()
