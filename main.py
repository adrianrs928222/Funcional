import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
ODDS_API_BASE_URL = os.getenv("ODDS_API_BASE_URL", "https://api.the-odds-api.com").rstrip("/")
TZ_NAME = os.getenv("TZ", "Europe/Madrid")

if not ODDS_API_KEY:
    raise RuntimeError("Falta ODDS_API_KEY en variables de entorno")

app = FastAPI(title="Top Picks Backend", version="23.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "daily_cache.json"
HISTORY_FILE = "history_picks.json"

TARGET_SPORTS = {
    "soccer_spain_la_liga": {"title": "LaLiga", "priority": 84},
    "soccer_epl": {"title": "Premier League", "priority": 85},
    "soccer_italy_serie_a": {"title": "Serie A", "priority": 83},
    "soccer_germany_bundesliga": {"title": "Bundesliga", "priority": 82},
    "soccer_france_ligue_one": {"title": "Ligue 1", "priority": 81},
    "soccer_spain_segunda_division": {"title": "LaLiga 2", "priority": 69},
    "soccer_efl_champ": {"title": "Championship", "priority": 70},
    "soccer_italy_serie_b": {"title": "Serie B", "priority": 67},
    "soccer_germany_bundesliga2": {"title": "Bundesliga 2", "priority": 68},
    "soccer_france_ligue_two": {"title": "Ligue 2", "priority": 66},
    "soccer_uefa_champs_league": {"title": "Champions League", "priority": 100},
    "soccer_uefa_europa_league": {"title": "Europa League", "priority": 95},
    "soccer_uefa_europa_conference_league": {"title": "Conference League", "priority": 90},
    "soccer_fifa_world_cup": {"title": "World Cup", "priority": 98},
    "soccer_uefa_european_championship": {"title": "Euro", "priority": 97},
}

SPORT_KEY_ALIASES = {
    "soccer_france_ligue_one": ["soccer_france_ligue_one", "soccer_france_ligue_1"],
    "soccer_france_ligue_two": ["soccer_france_ligue_two", "soccer_france_ligue_2"],
    "soccer_efl_champ": ["soccer_efl_champ", "soccer_england_efl_championship"],
    "soccer_germany_bundesliga2": ["soccer_germany_bundesliga2", "soccer_germany_bundesliga_2"],
}

BOOKMAKER_PRIORITY = [
    "bet365",
    "pinnacle",
    "unibet",
    "williamhill",
    "bwin",
    "ladbrokes",
    "betfair",
    "1xbet",
]

REGIONS = "uk,eu"
LOOKBACK_HOURS = 3
LOOKAHEAD_HOURS = 18


def log(*args: Any) -> None:
    print("[TOP-PICKS]", *args, flush=True)


def madrid_now() -> datetime:
    return datetime.now(pytz.timezone(TZ_NAME))


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def implied_probability(odds: float) -> float:
    return 1.0 / odds if odds and odds > 0 else 0.0


def iso_to_local_hhmm(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(pytz.timezone(TZ_NAME)).strftime("%H:%M")


def normalize_text(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def sport_priority(sport_key: str) -> int:
    return TARGET_SPORTS.get(sport_key, {}).get("priority", 10)


def bookmaker_rank(key_or_title: str) -> int:
    norm = normalize_text(key_or_title)
    for idx, name in enumerate(BOOKMAKER_PRIORITY):
        if normalize_text(name) == norm:
            return idx
    return 999


def daily_cache_deadline() -> datetime:
    now = madrid_now()
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime.combine(tomorrow, datetime.min.time())
    return pytz.timezone(TZ_NAME).localize(midnight) + timedelta(minutes=5)


def odds_api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{ODDS_API_BASE_URL}{path}"
    params = params or {}
    params["apiKey"] = ODDS_API_KEY
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_cache() -> Optional[Dict[str, Any]]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        cached_until = data.get("cached_until")
        cache_day = data.get("cache_day")
        if not cached_until or not cache_day:
            return None

        until_dt = datetime.fromisoformat(cached_until)
        now = madrid_now()

        if now.strftime("%Y-%m-%d") == cache_day and now < until_dt.astimezone(pytz.timezone(TZ_NAME)):
            return data
        return None
    except Exception:
        return None


def save_cache(data: Dict[str, Any]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def clear_cache() -> None:
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception:
        pass


def load_history() -> Dict[str, Any]:
    if not os.path.exists(HISTORY_FILE):
        return {"days": {}}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "days" not in data or not isinstance(data["days"], dict):
            return {"days": {}}
        return data
    except Exception:
        return {"days": {}}


def save_history(data: Dict[str, Any]) -> None:
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def fetch_events_for_sport(sport_key: str) -> List[Dict[str, Any]]:
    aliases = SPORT_KEY_ALIASES.get(sport_key, [sport_key])

    for alias in aliases:
        try:
            data = odds_api_get(
                f"/v4/sports/{alias}/odds",
                {
                    "regions": REGIONS,
                    "markets": "h2h",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                },
            )
            if isinstance(data, list):
                return data
        except Exception as e:
            log("Error odds", alias, str(e))
            continue

    return []


def fetch_scores_for_sport(sport_key: str, days_from: int = 3) -> List[Dict[str, Any]]:
    aliases = SPORT_KEY_ALIASES.get(sport_key, [sport_key])

    for alias in aliases:
        try:
            data = odds_api_get(
                f"/v4/sports/{alias}/scores",
                {
                    "daysFrom": days_from,
                    "dateFormat": "iso",
                },
            )
            if isinstance(data, list):
                return data
        except Exception as e:
            log("Error scores", alias, str(e))
            continue

    return []


def get_nearby_fixtures() -> List[