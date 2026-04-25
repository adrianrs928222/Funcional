import json
import math
import os
import unicodedata
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG
# =========================================================

TZ = pytz.timezone("Europe/Madrid")

SPORTSDB_API_KEY = os.getenv("SPORTSDB_API_KEY", "123").strip()
SPORTSDB_BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}"

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"
API_STATE_FILE = "api_state.json"
MODEL_STATS_FILE = "model_stats.json"

LOOKAHEAD_HOURS = 168
CACHE_REFRESH_MINUTES = 15

MAX_PICKS = 10
TARGET_PICKS = 10

MIN_BUILDER_SELECTIONS = 2
MAX_BUILDER_SELECTIONS = 6

MIN_BUILDER_ODDS = 1.50
MAX_BUILDER_ODDS = 6.00

MIN_PUBLIC_CONFIDENCE = 10
MAX_PUBLIC_CONFIDENCE = 80

MAX_HISTORY_DAYS = 30
API_COOLDOWN_MINUTES = 10
HISTORY_PAGE_SIZE = 12

API_PRIORITY = ["api_football", "football_data", "sportsdb"]

SPORTSDB_LEAGUES = {
    "4328": "LaLiga",
    "4400": "Segunda División",
    "4480": "Champions League",
    "4429": "Mundial",
}

API_FOOTBALL_LEAGUES = {
    140: "LaLiga",
    2: "Champions League",
    1: "Mundial",
}

FOOTBALL_DATA_LEAGUES = {
    "PD": "LaLiga",
    "SD": "Segunda División",
    "CL": "Champions League",
}

SEASON_CANDIDATES_SPORTSDB = ["2025-2026", "2024-2025", "2026"]

ODDS_SPORT_KEYS = {
    "LaLiga": "soccer_spain_la_liga",
    "Segunda División": "soccer_spain_segunda_division",
    "Champions League": "soccer_uefa_champs_league",
}

TRACKABLE_MARKETS = {
    "winner",
    "double_chance",
    "over_2_5",
    "under_2_5",
    "under_3_5",
    "btts_yes",
    "btts_no",
    "bet_builder",
}

SAFE_COMBO_MARKETS = {
    "double_chance",
    "under_3_5",
    "btts_no",
    "bet_builder",
}

TEAM_RATINGS = {
    "Real Madrid": 93,
    "Real Madrid CF": 93,
    "Barcelona": 91,
    "FC Barcelona": 91,
    "Atletico Madrid": 87,
    "Atlético Madrid": 87,
    "Club Atlético de Madrid": 87,
    "Athletic Club": 84,
    "Real Sociedad": 82,
    "Villarreal": 81,
    "Real Betis": 80,
    "Girona": 80,
    "Valencia": 77,
    "Sevilla": 78,
    "Osasuna": 76,
    "Getafe": 74,
    "Mallorca": 74,
    "Rayo Vallecano": 75,
    "Celta Vigo": 75,
    "Las Palmas": 72,
    "Alaves": 73,
    "Alavés": 73,

    "Almería": 78,
    "Almeria": 78,
    "Granada": 77,
    "Cádiz": 76,
    "Cadiz": 76,
    "Levante": 76,
    "Real Oviedo": 74,
    "Real Zaragoza": 73,
    "Sporting Gijón": 73,
    "Sporting Gijon": 73,
    "Eibar": 74,
    "Elche": 75,
    "Racing Santander": 73,
    "Tenerife": 71,
    "Huesca": 71,
    "Burgos": 71,
    "Albacete": 71,
    "Castellón": 70,
    "Castellon": 70,
    "Málaga": 72,
    "Malaga": 72,
    "Córdoba": 70,
    "Cordoba": 70,
    "Deportivo La Coruña": 72,

    "Manchester City": 94,
    "Arsenal": 91,
    "Liverpool": 91,
    "Bayern Munich": 92,
    "Borussia Dortmund": 86,
    "Paris Saint Germain": 91,
    "Paris SG": 91,
    "Inter": 90,
    "Juventus": 86,
    "AC Milan": 86,
    "Napoli": 84,
    "Benfica": 84,
    "FC Porto": 83,
    "PSV Eindhoven": 85,
    "RB Leipzig": 84,
    "Sporting CP": 83,
    "Sporting Lisbon": 83,

    "Spain": 90,
    "España": 90,
    "France": 92,
    "Francia": 92,
    "Brazil": 91,
    "Brasil": 91,
    "Argentina": 91,
    "England": 90,
    "Inglaterra": 90,
    "Portugal": 89,
    "Germany": 88,
    "Alemania": 88,
    "Netherlands": 88,
    "Países Bajos": 88,
    "Italy": 87,
    "Italia": 87,
    "Uruguay": 84,
    "Belgium": 84,
    "Bélgica": 84,
    "Croatia": 83,
    "Croacia": 83,
    "USA": 80,
    "United States": 80,
    "Mexico": 80,
    "México": 80,
    "Morocco": 82,
    "Marruecos": 82,
    "Japan": 79,
    "Japón": 79,
}

DRAW_TRAP_TEAMS = {
    "atletico madrid",
    "getafe",
    "osasuna",
    "mallorca",
    "rayo vallecano",
}

AGGRESSIVE_CARD_TEAMS = {
    "getafe",
    "osasuna",
    "atletico madrid",
    "rayo vallecano",
    "mallorca",
    "cadiz",
    "alaves",
    "sporting",
}

app = FastAPI(title="Tipster Tips Pro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
# =========================================================
# BASIC HELPERS
# =========================================================

def read_json(path: str) -> Any:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_json(path: str, data: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def now_local() -> datetime:
    return datetime.now(TZ)


def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")


def normalize_text(v: Optional[str]) -> str:
    return (v or "").strip().lower()


def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def simplify_team_name(name: str) -> str:
    n = strip_accents(normalize_text(name))

    junk = [
        " fc", " cf", " cd", " ud", " sd", " rc", " rcd",
        "fc ", "cf ", "cd ", "ud ", "sd ", "rc ", "rcd ",
        " club", " futbol", " football club",
    ]

    for j in junk:
        n = n.replace(j, " ")

    replacements = {
        "club atletico de madrid": "atletico madrid",
        "atletico de madrid": "atletico madrid",
        "real sociedad de futbol": "real sociedad",
        "real betis balompie": "real betis",
        "deportivo alaves": "alaves",
        "deportivo la coruna": "deportivo",
        "deportivo de la coruna": "deportivo",
        "paris saint germain": "psg",
        "paris saint-germain": "psg",
        "paris sg": "psg",
        "fc bayern munchen": "bayern munich",
        "bayern munchen": "bayern munich",
        "internazionale": "inter",
        "fc internazionale milano": "inter",
        "sporting gijon": "sporting",
        "real sporting": "sporting",
        "racing de santander": "racing santander",
        "levante ud": "levante",
        "granada cf": "granada",
        "cadiz cf": "cadiz",
        "elche cf": "elche",
        "malaga cf": "malaga",
        "cordoba cf": "cordoba",
        "united states": "usa",
        "estados unidos": "usa",
    }

    for old, new in replacements.items():
        n = n.replace(old, new)

    return " ".join(n.split())


def team_names_match(a: str, b: str) -> bool:
    sa = simplify_team_name(a)
    sb = simplify_team_name(b)

    if sa == sb:
        return True

    a_tokens = set(sa.split())
    b_tokens = set(sb.split())

    if not a_tokens or not b_tokens:
        return False

    common = a_tokens & b_tokens
    return len(common) >= min(2, len(a_tokens), len(b_tokens))


def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    generated_at = cache.get("generated_at")
    if not generated_at:
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    return now_local() - dt.astimezone(TZ) < timedelta(minutes=CACHE_REFRESH_MINUTES)


def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]

    key = simplify_team_name(team_name).encode("utf-8")
    digest = hashlib.md5(key).hexdigest()
    value = int(digest[:8], 16) % 1000
    return round(68 + (value / 1000) * 14, 2)


def public_confidence(confidence: int) -> int:
    return int(max(MIN_PUBLIC_CONFIDENCE, min(MAX_PUBLIC_CONFIDENCE, confidence)))


def current_api_football_season() -> int:
    now = now_local()
    return now.year if now.month >= 7 else now.year - 1


def parse_requests_error(e: Exception) -> str:
    text = str(e)
    if "429" in text:
        return "rate_limit"
    return text[:300]


def source_priority(source: str) -> int:
    try:
        return API_PRIORITY.index(source)
    except ValueError:
        return 999


def match_time_bucket(dt: datetime) -> str:
    rounded_minute = 0 if dt.minute < 30 else 30
    return dt.replace(minute=rounded_minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def confidence_band(confidence: int) -> str:
    if confidence >= 70:
        return "alta"
    if confidence >= 55:
        return "media"
    return "intermedia"


def classify_pick(confidence: int) -> str:
    if confidence >= 70:
        return "premium"
    if confidence >= 60:
        return "strong"
    if confidence >= 45:
        return "medium"
    return "risky"


def league_team_sanity_check(league: str, home: str, away: str) -> bool:
    league_n = normalize_text(league)
    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    laliga_teams = {
        "real madrid", "barcelona", "atletico madrid", "athletic club",
        "real sociedad", "villarreal", "real betis", "girona", "valencia",
        "sevilla", "osasuna", "getafe", "mallorca", "rayo vallecano",
        "celta vigo", "las palmas", "alaves",
    }

    segunda_teams = {
        "almeria", "granada", "cadiz", "levante", "real oviedo",
        "real zaragoza", "sporting", "eibar", "elche", "racing santander",
        "tenerife", "huesca", "burgos", "albacete", "castellon",
        "malaga", "cordoba", "deportivo",
    }

    if league_n == "laliga":
        return home_s in laliga_teams and away_s in laliga_teams

    if league_n == "segunda división":
        return home_s in segunda_teams and away_s in segunda_teams

    if league_n in {"champions league", "mundial"}:
        return True

    return True


# =========================================================
# API STATE
# =========================================================

def load_api_state() -> Dict[str, Any]:
    state = read_json(API_STATE_FILE)
    for name in ["sportsdb", "football_data", "api_football", "odds_api"]:
        state.setdefault(name, {})
    return state


def save_api_state(state: Dict[str, Any]) -> None:
    write_json(API_STATE_FILE, state)


def set_api_cooldown(api_name: str, reason: str) -> None:
    state = load_api_state()
    until = now_local() + timedelta(minutes=API_COOLDOWN_MINUTES)
    state.setdefault(api_name, {})
    state[api_name]["cooldown_until"] = until.isoformat()
    state[api_name]["last_error"] = reason
    save_api_state(state)


def clear_api_cooldown(api_name: str) -> None:
    state = load_api_state()
    state.setdefault(api_name, {})
    state[api_name]["cooldown_until"] = None
    state[api_name]["last_error"] = None
    save_api_state(state)


def api_is_available(api_name: str) -> bool:
    state = load_api_state()
    info = state.get(api_name) or {}
    cooldown_until = info.get("cooldown_until")

    if not cooldown_until:
        return True

    try:
        dt = datetime.fromisoformat(cooldown_until)
    except Exception:
        return True

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    return now_local() >= dt.astimezone(TZ)
# =========================================================
# SPORTSDB
# =========================================================

def sportsdb_get(path: str) -> Dict[str, Any]:
    r = requests.get(f"{SPORTSDB_BASE_URL}{path}", timeout=12)
    r.raise_for_status()
    return r.json()


def parse_sportsdb_datetime(date_str: Optional[str], time_str: Optional[str]) -> datetime:
    date_str = (date_str or "").strip()
    time_str = (time_str or "00:00:00").strip().replace("Z", "")

    if not date_str:
        raise ValueError("Missing dateEvent")

    dt_utc = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
    return dt_utc.astimezone(TZ)


def extract_home_away_sportsdb(event: Dict[str, Any]) -> Dict[str, str]:
    home = (event.get("strHomeTeam") or "").strip()
    away = (event.get("strAwayTeam") or "").strip()

    if home and away:
        return {"home": home, "away": away}

    event_name = (event.get("strEvent") or "").strip()

    if " vs " in event_name:
        a, b = event_name.split(" vs ", 1)
        return {"home": a.strip(), "away": b.strip()}

    if " - " in event_name:
        a, b = event_name.split(" - ", 1)
        return {"home": a.strip(), "away": b.strip()}

    raise ValueError("No home/away")


def get_sportsdb_matches() -> List[Dict[str, Any]]:
    if not api_is_available("sportsdb"):
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    out: List[Dict[str, Any]] = []
    seen = set()

    try:
        for league_id, league_name in SPORTSDB_LEAGUES.items():
            events: List[Dict[str, Any]] = []

            for season in SEASON_CANDIDATES_SPORTSDB:
                try:
                    data = sportsdb_get(f"/eventsseason.php?id={league_id}&s={season}")
                    season_events = data.get("events") or []
                    if season_events:
                        events.extend(season_events)
                        break
                except Exception:
                    pass

            try:
                next_data = sportsdb_get(f"/eventsnextleague.php?id={league_id}")
                events.extend(next_data.get("events") or [])
            except Exception:
                pass

            for ev in events:
                try:
                    teams = extract_home_away_sportsdb(ev)
                    dt_local = parse_sportsdb_datetime(ev.get("dateEvent"), ev.get("strTime"))
                except Exception:
                    continue

                if not (start <= dt_local <= end):
                    continue

                if not league_team_sanity_check(league_name, teams["home"], teams["away"]):
                    continue

                key = (league_name, teams["home"], teams["away"], dt_local.isoformat())

                if key in seen:
                    continue

                seen.add(key)

                out.append({
                    "id": ev.get("idEvent") or f"sportsdb-{league_id}-{teams['home']}-{teams['away']}",
                    "match": f"{teams['home']} vs {teams['away']}",
                    "league": league_name,
                    "home_team": teams["home"],
                    "away_team": teams["away"],
                    "dt_local": dt_local,
                    "source": "sportsdb",
                })

        clear_api_cooldown("sportsdb")
        out.sort(key=lambda x: x["dt_local"])
        return out

    except Exception as e:
        set_api_cooldown("sportsdb", parse_requests_error(e))
        return []


# =========================================================
# API-FOOTBALL
# =========================================================

def api_football_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not API_FOOTBALL_KEY:
        raise RuntimeError("Falta API_FOOTBALL_KEY")

    headers = {"x-apisports-key": API_FOOTBALL_KEY}

    r = requests.get(
        f"{API_FOOTBALL_BASE_URL}{path}",
        headers=headers,
        params=params or {},
        timeout=15,
    )

    r.raise_for_status()
    return r.json()


def get_api_football_matches() -> List[Dict[str, Any]]:
    if not api_is_available("api_football"):
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    season = current_api_football_season()

    out: List[Dict[str, Any]] = []

    try:
        for league_id, league_name in API_FOOTBALL_LEAGUES.items():
            data = api_football_get(
                "/fixtures",
                {
                    "league": league_id,
                    "season": season,
                    "from": start.date().isoformat(),
                    "to": end.date().isoformat(),
                    "timezone": "Europe/Madrid",
                },
            )

            for item in data.get("response") or []:
                try:
                    fixture = item.get("fixture") or {}
                    teams = item.get("teams") or {}

                    home = (teams.get("home") or {}).get("name")
                    away = (teams.get("away") or {}).get("name")
                    date_str = fixture.get("date")

                    if not home or not away or not date_str:
                        continue

                    dt_local = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(TZ)

                    if not (start <= dt_local <= end):
                        continue

                    if not league_team_sanity_check(league_name, home, away):
                        continue

                    out.append({
                        "id": fixture.get("id"),
                        "match": f"{home} vs {away}",
                        "league": league_name,
                        "home_team": home,
                        "away_team": away,
                        "dt_local": dt_local,
                        "source": "api_football",
                    })

                except Exception:
                    continue

        clear_api_cooldown("api_football")
        out.sort(key=lambda x: x["dt_local"])
        return out

    except Exception as e:
        set_api_cooldown("api_football", parse_requests_error(e))
        return []


# =========================================================
# FOOTBALL-DATA
# =========================================================

def football_data_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not FOOTBALL_DATA_API_KEY:
        raise RuntimeError("Falta FOOTBALL_DATA_API_KEY")

    headers = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}

    r = requests.get(
        f"{FOOTBALL_DATA_BASE_URL}{path}",
        headers=headers,
        params=params or {},
        timeout=15,
    )

    r.raise_for_status()
    return r.json()


def get_football_data_matches() -> List[Dict[str, Any]]:
    if not api_is_available("football_data"):
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    out: List[Dict[str, Any]] = []

    try:
        for code, league_name in FOOTBALL_DATA_LEAGUES.items():
            data = football_data_get(
                f"/competitions/{code}/matches",
                {
                    "dateFrom": start.date().isoformat(),
                    "dateTo": end.date().isoformat(),
                },
            )

            for item in data.get("matches") or []:
                try:
                    utc_date = item.get("utcDate")
                    home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                    away = ((item.get("awayTeam") or {}).get("name") or "").strip()

                    if not utc_date or not home or not away:
                        continue

                    dt_local = datetime.fromisoformat(utc_date.replace("Z", "+00:00")).astimezone(TZ)

                    if not (start <= dt_local <= end):
                        continue

                    if not league_team_sanity_check(league_name, home, away):
                        continue

                    out.append({
                        "id": item.get("id"),
                        "match": f"{home} vs {away}",
                        "league": league_name,
                        "home_team": home,
                        "away_team": away,
                        "dt_local": dt_local,
                        "source": "football_data",
                    })

                except Exception:
                    continue

        clear_api_cooldown("football_data")
        out.sort(key=lambda x: x["dt_local"])
        return out

    except Exception as e:
        set_api_cooldown("football_data", parse_requests_error(e))
        return []
# =========================================================
# ODDS API
# =========================================================

def odds_api_get(path: str, params: Dict[str, Any]) -> Any:
    if not ODDS_API_KEY:
        raise RuntimeError("Falta ODDS_API_KEY")

    merged = {"apiKey": ODDS_API_KEY}
    merged.update(params)

    r = requests.get(f"{ODDS_API_BASE_URL}{path}", params=merged, timeout=15)
    r.raise_for_status()
    return r.json()


def select_best_h2h_market(bookmakers: List[Dict[str, Any]], home: str, away: str) -> Optional[Dict[str, Any]]:
    best = None

    for book in bookmakers or []:
        title = book.get("title") or book.get("key") or "Bookmaker"

        for market in book.get("markets") or []:
            if market.get("key") != "h2h":
                continue

            home_price = None
            away_price = None
            draw_price = None

            for outcome in market.get("outcomes") or []:
                outcome_name = outcome.get("name", "")
                price = outcome.get("price")

                if price is None:
                    continue

                if team_names_match(outcome_name, home):
                    home_price = price
                elif team_names_match(outcome_name, away):
                    away_price = price
                elif simplify_team_name(outcome_name) in {"draw", "empate"}:
                    draw_price = price

            nums = [x for x in [home_price, away_price, draw_price] if isinstance(x, (int, float))]
            if not nums:
                continue

            candidate = {
                "bookmaker": title,
                "market": "1X2",
                "home": home_price,
                "draw": draw_price,
                "away": away_price,
                "avg": sum(nums) / len(nums),
            }

            if best is None or candidate["avg"] < best["avg"]:
                best = candidate

    return best


def fetch_live_odds_index() -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    if not ODDS_API_KEY or not api_is_available("odds_api"):
        return index

    try:
        for league_name, sport_key in ODDS_SPORT_KEYS.items():
            try:
                data = odds_api_get(
                    f"/sports/{sport_key}/odds",
                    {
                        "regions": "eu,uk",
                        "markets": "h2h",
                        "oddsFormat": "decimal",
                        "dateFormat": "iso",
                    },
                )
            except Exception:
                continue

            for event in data or []:
                home = (event.get("home_team") or "").strip()
                away = (event.get("away_team") or "").strip()

                if not away:
                    teams = event.get("teams") or []
                    if len(teams) == 2:
                        away = teams[1] if team_names_match(teams[0], home) else teams[0]

                if not home or not away:
                    continue

                if not league_team_sanity_check(league_name, home, away):
                    continue

                best_market = select_best_h2h_market(event.get("bookmakers") or [], home, away)

                if not best_market:
                    continue

                key = (
                    simplify_team_name(home),
                    simplify_team_name(away),
                    normalize_text(league_name),
                )

                index[key] = best_market

        clear_api_cooldown("odds_api")
        return index

    except Exception as e:
        set_api_cooldown("odds_api", parse_requests_error(e))
        return {}


# =========================================================
# LIGHT LEARNING
# =========================================================

def load_model_stats() -> Dict[str, Any]:
    stats = read_json(MODEL_STATS_FILE)
    stats.setdefault("by_market", {})
    stats.setdefault("by_league", {})
    return stats


def save_model_stats(stats: Dict[str, Any]) -> None:
    write_json(MODEL_STATS_FILE, stats)


def rebuild_model_stats_from_history(history: Dict[str, Any]) -> Dict[str, Any]:
    stats = {"by_market": {}, "by_league": {}}

    for _, day in history.get("days", {}).items():
        for pick in day.get("picks", []):
            status = pick.get("status")
            pick_type = pick.get("pick_type", "unknown")

            if pick_type not in TRACKABLE_MARKETS:
                continue

            if status not in ["won", "lost"]:
                continue

            league = pick.get("league", "unknown")

            stats["by_market"].setdefault(pick_type, {"won": 0, "lost": 0})
            stats["by_league"].setdefault(league, {"won": 0, "lost": 0})

            stats["by_market"][pick_type][status] += 1
            stats["by_league"][league][status] += 1

    return stats


def get_adjustment_from_stats(league: str, pick_type: str) -> int:
    stats = load_model_stats()

    def ratio(bucket: Dict[str, int]) -> Optional[float]:
        total = bucket.get("won", 0) + bucket.get("lost", 0)
        if total < 12:
            return None
        return bucket.get("won", 0) / total

    league_ratio = ratio(stats["by_league"].get(league, {}))
    market_ratio = ratio(stats["by_market"].get(pick_type, {}))

    adjustment = 0

    if league_ratio is not None:
        if league_ratio >= 0.67:
            adjustment += 2
        elif league_ratio <= 0.50:
            adjustment -= 3

    if market_ratio is not None:
        if market_ratio >= 0.68:
            adjustment += 3
        elif market_ratio <= 0.55:
            adjustment -= 5

    return adjustment


def refresh_model_stats_from_history(history: Dict[str, Any]) -> None:
    save_model_stats(rebuild_model_stats_from_history(history))


# =========================================================
# MATCH MERGE
# =========================================================

def get_real_matches() -> List[Dict[str, Any]]:
    matches_by_source = {
        "api_football": get_api_football_matches(),
        "football_data": get_football_data_matches(),
        "sportsdb": get_sportsdb_matches(),
    }

    combined: List[Dict[str, Any]] = []

    for src in API_PRIORITY:
        combined.extend(matches_by_source.get(src, []))

    dedup: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for m in combined:
        if not league_team_sanity_check(m["league"], m["home_team"], m["away_team"]):
            continue

        key = (
            simplify_team_name(m["home_team"]),
            simplify_team_name(m["away_team"]),
            normalize_text(m["league"]),
            match_time_bucket(m["dt_local"]),
        )

        if key not in dedup:
            dedup[key] = m
            continue

        old = dedup[key]
        if source_priority(m["source"]) < source_priority(old["source"]):
            dedup[key] = m

    unique = list(dedup.values())
    unique.sort(key=lambda x: x["dt_local"])
    return unique[:60]


# =========================================================
# MARKET HELPERS
# =========================================================

def implied_probability(odds: float) -> Optional[float]:
    if not odds or odds <= 1:
        return None
    return round(100 / odds, 1)


def is_draw_trap(home: str, away: str, abs_diff: float) -> bool:
    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    if abs_diff < 4.5:
        return True

    if home_s in DRAW_TRAP_TEAMS or away_s in DRAW_TRAP_TEAMS:
        return True

    return False


def anti_draw_penalty(home: str, away: str, abs_diff: float) -> int:
    penalty = 0

    if abs_diff < 4.5:
        penalty += 10
    elif abs_diff < 6.0:
        penalty += 6

    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    if home_s in DRAW_TRAP_TEAMS or away_s in DRAW_TRAP_TEAMS:
        penalty += 5

    return penalty


def market_read_adjustment(bookmaker_odds: Optional[float], confidence: int) -> Tuple[int, str]:
    if not bookmaker_odds:
        return confidence, ""

    market_prob = implied_probability(bookmaker_odds)
    if market_prob is None:
        return confidence, ""

    diff = confidence - market_prob

    if diff >= 10:
        confidence -= 3
        return max(confidence, 55), "El mercado es más prudente y ajusto la confianza."

    if diff <= -6:
        confidence += 2
        return min(confidence, 90), "La cuota acompaña bien esta lectura."

    return confidence, ""


def safe_odds_from_confidence(confidence: int, market_type: str) -> float:
    if market_type == "double_chance":
        base = 1.25 + (100 - confidence) * 0.006
        return round(min(max(base, 1.22), 1.65), 2)

    if market_type == "under_3_5":
        base = 1.32 + (100 - confidence) * 0.006
        return round(min(max(base, 1.28), 1.85), 2)

    if market_type == "over_2_5":
        base = 1.58 + (100 - confidence) * 0.007
        return round(min(max(base, 1.45), 2.25), 2)

    if market_type == "btts_yes":
        base = 1.62 + (100 - confidence) * 0.007
        return round(min(max(base, 1.50), 2.30), 2)

    if market_type == "btts_no":
        base = 1.58 + (100 - confidence) * 0.007
        return round(min(max(base, 1.48), 2.25), 2)

    if market_type == "team_cards":
        base = 1.45 + (100 - confidence) * 0.006
        return round(min(max(base, 1.35), 2.00), 2)

    if market_type == "team_score_first_half":
        base = 1.85 + (100 - confidence) * 0.008
        return round(min(max(base, 1.65), 2.45), 2)

    if market_type == "team_score_second_half":
        base = 1.72 + (100 - confidence) * 0.008
        return round(min(max(base, 1.55), 2.30), 2)

    base = 1.55 + (100 - confidence) * 0.007
    return round(min(max(base, 1.35), 2.50), 2)


def market_reliability_bonus(pick_type: str) -> int:
    if pick_type == "double_chance":
        return 10
    if pick_type == "under_3_5":
        return 9
    if pick_type == "team_cards":
        return 6
    if pick_type == "btts_no":
        return 5
    if pick_type == "over_2_5":
        return 2
    if pick_type == "btts_yes":
        return 1
    if pick_type == "team_score_second_half":
        return 1
    if pick_type == "team_score_first_half":
        return 0
    if pick_type == "winner":
        return -4
    return 0
# =========================================================
# MARKET BUILDERS
# =========================================================

def predict_cards(league: str, home_strength: float, away_strength: float, home: str, away: str) -> Dict[str, int]:
    base_cards = {
        "LaLiga": 5,
        "Segunda División": 6,
        "Champions League": 4,
        "Mundial": 4,
    }

    total = base_cards.get(league, 5)

    if home_strength > away_strength:
        away_cards = min(total - 1, max(2, round(total * 0.58)))
        home_cards = total - away_cards
    elif away_strength > home_strength:
        home_cards = min(total - 1, max(2, round(total * 0.58)))
        away_cards = total - home_cards
    else:
        home_cards = total // 2
        away_cards = total - home_cards

    return {home: int(home_cards), away: int(away_cards)}


def team_cards_market(home: str, away: str, home_strength: float, away_strength: float, league: str) -> Dict[str, Any]:
    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    diff = home_strength - away_strength
    abs_diff = abs(diff)

    if diff >= 4:
        card_team = away
        card_team_s = away_s
        line = 1.5 if league in {"Champions League", "Mundial"} else 2.5
        conf = 72 + min(abs_diff * 1.2, 10)
    elif diff <= -4:
        card_team = home
        card_team_s = home_s
        line = 1.5 if league in {"Champions League", "Mundial"} else 2.5
        conf = 72 + min(abs_diff * 1.2, 10)
    else:
        if away_s in AGGRESSIVE_CARD_TEAMS:
            card_team = away
            card_team_s = away_s
            line = 1.5
            conf = 75
        else:
            card_team = home
            card_team_s = home_s
            line = 1.5
            conf = 72

    if card_team_s in AGGRESSIVE_CARD_TEAMS:
        conf += 5

    return {
        "pick": f"Más de {line} tarjetas {card_team}",
        "pick_type": "team_cards",
        "confidence": int(min(conf, 88)),
        "cards_team": card_team,
        "cards_line": line,
        "trackable": False,
    }


def team_to_score_half_markets(
    home: str,
    away: str,
    home_strength: float,
    away_strength: float,
    home_xg: float,
    away_xg: float,
    league: str,
) -> List[Dict[str, Any]]:
    options: List[Dict[str, Any]] = []

    diff = home_strength - away_strength
    total_xg = home_xg + away_xg

    if total_xg < 2.1:
        return options

    home_xg_1h = home_xg * 0.42
    away_xg_1h = away_xg * 0.40
    home_xg_2h = home_xg * 0.58
    away_xg_2h = away_xg * 0.60

    def conf_from_half_xg(xg_half: float, strength_edge: float, is_second_half: bool) -> int:
        base = 54 + int(xg_half * 18)

        if strength_edge > 0:
            base += min(int(strength_edge * 0.8), 8)
        elif strength_edge < 0:
            base -= min(int(abs(strength_edge) * 0.5), 5)

        if is_second_half:
            base += 3

        return max(58, min(base, 84))

    if home_xg_1h >= away_xg_1h:
        team_1h = home
        team_1h_conf = conf_from_half_xg(home_xg_1h, diff, False)
    else:
        team_1h = away
        team_1h_conf = conf_from_half_xg(away_xg_1h, -diff, False)

    if home_xg_2h >= away_xg_2h:
        team_2h = home
        team_2h_conf = conf_from_half_xg(home_xg_2h, diff, True)
    else:
        team_2h = away
        team_2h_conf = conf_from_half_xg(away_xg_2h, -diff, True)

    options.append({
        "pick": f"Marca {team_1h} en 1ª parte",
        "pick_type": "team_score_first_half",
        "confidence": int(team_1h_conf),
        "scoring_team": team_1h,
        "half": "1H",
        "trackable": False,
    })

    options.append({
        "pick": f"Marca {team_2h} en 2ª parte",
        "pick_type": "team_score_second_half",
        "confidence": int(team_2h_conf),
        "scoring_team": team_2h,
        "half": "2H",
        "trackable": False,
    })

    return options


def build_market_options(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    home = match["home_team"]
    away = match["away_team"]
    league = match["league"]
    is_world_cup = normalize_text(league) == "mundial"

    home_strength = stable_team_rating(home) + 3.2
    away_strength = stable_team_rating(away)

    if league == "Segunda División":
        home_strength -= 1.0

    diff = home_strength - away_strength
    abs_diff = abs(diff)

    home_xg = max(0.55, min(1.20 + diff * 0.035, 2.80))
    away_xg = max(0.40, min(1.00 - diff * 0.022, 2.30))
    total_xg = home_xg + away_xg

    if is_world_cup:
        home_xg *= 0.92
        away_xg *= 0.92
        total_xg *= 0.92

    winner = home if home_strength >= away_strength else away
    loser = away if winner == home else home

    draw_trap = is_draw_trap(home, away, abs_diff)
    draw_penalty = anti_draw_penalty(home, away, abs_diff)

    options: List[Dict[str, Any]] = []

    winner_conf = int(max(68, min(89, 69 + min(abs_diff * 1.7, 18))))
    winner_conf += get_adjustment_from_stats(league, "winner")
    winner_conf -= draw_penalty

    options.append({
        "pick": f"Gana {winner}",
        "pick_type": "winner",
        "confidence": int(max(58, min(92, winner_conf))),
        "winner_team": winner,
        "trackable": True,
    })

    dc_pick = f"1X {home}" if diff >= 0 else f"X2 {away}"
    dc_conf = 74 + min(abs_diff * 1.1, 10)

    if draw_trap:
        dc_conf += 6

    if abs_diff < 4:
        dc_conf += 4

    if is_world_cup:
        dc_conf += 2

    dc_conf += get_adjustment_from_stats(league, "double_chance")

    options.append({
        "pick": dc_pick,
        "pick_type": "double_chance",
        "confidence": int(max(68, min(91, dc_conf))),
        "trackable": True,
    })

    over_conf = 66

    if total_xg >= 2.45:
        over_conf += max(0, (total_xg - 2.25) * 12)

    if home_xg >= 1.2 and away_xg >= 0.9:
        over_conf += 4

    if abs_diff < 5.5:
        over_conf += 2

    over_conf += get_adjustment_from_stats(league, "over_2_5")

    options.append({
        "pick": "Más de 2.5 goles",
        "pick_type": "over_2_5",
        "confidence": int(max(56, min(89, over_conf))),
        "trackable": True,
    })

    under35_conf = 73

    if total_xg <= 2.90:
        under35_conf += max(0, (3.05 - total_xg) * 9)

    if draw_trap:
        under35_conf += 5

    if is_world_cup:
        under35_conf += 3

    under35_conf += get_adjustment_from_stats(league, "under_3_5")

    options.append({
        "pick": "Menos de 3.5 goles",
        "pick_type": "under_3_5",
        "confidence": int(max(68, min(90, under35_conf))),
        "trackable": True,
    })

    btts_yes_conf = 64

    if home_xg >= 1.0:
        btts_yes_conf += 4

    if away_xg >= 0.9:
        btts_yes_conf += 5

    if abs_diff < 7.5:
        btts_yes_conf += 3

    if draw_trap:
        btts_yes_conf += 2

    if is_world_cup:
        btts_yes_conf -= 2

    btts_yes_conf += get_adjustment_from_stats(league, "btts_yes")

    options.append({
        "pick": "Ambos marcan: Sí",
        "pick_type": "btts_yes",
        "confidence": int(max(56, min(87, btts_yes_conf))),
        "trackable": True,
    })

    btts_no_conf = 70

    if total_xg <= 2.35:
        btts_no_conf += 4

    if abs_diff >= 6.5:
        btts_no_conf += 3

    if draw_trap:
        btts_no_conf += 3

    if is_world_cup:
        btts_no_conf += 2

    btts_no_conf += get_adjustment_from_stats(league, "btts_no")

    options.append({
        "pick": "Ambos marcan: No",
        "pick_type": "btts_no",
        "confidence": int(max(60, min(88, btts_no_conf))),
        "trackable": True,
    })

    options.append(team_cards_market(home, away, home_strength, away_strength, league))

    options.extend(
        team_to_score_half_markets(
            home,
            away,
            home_strength,
            away_strength,
            home_xg,
            away_xg,
            league,
        )
    )

    for o in options:
        o["home_strength"] = home_strength
        o["away_strength"] = away_strength
        o["home_xg"] = round(home_xg, 2)
        o["away_xg"] = round(away_xg, 2)
        o["total_xg"] = round(total_xg, 2)
        o["draw_trap"] = draw_trap
        o["winner_team"] = o.get("winner_team", winner)
        o["loser_team"] = loser

    return options


# =========================================================
# ENRICH / BUILDER
# =========================================================

def tipster_explanation(option: Dict[str, Any]) -> str:
    pick_type = option.get("pick_type")

    if pick_type == "winner":
        return "Lectura principal basada en diferencia competitiva, localía y riesgo de empate."

    if pick_type == "double_chance":
        return "Mercado protegido para cubrir empate y reducir riesgo."

    if pick_type == "over_2_5":
        return "Lectura de partido con ritmo y opciones de superar la línea de goles."

    if pick_type == "under_3_5":
        return "Mercado de control para partidos que no apuntan a marcador descontrolado."

    if pick_type == "btts_yes":
        return "Ambos equipos tienen argumentos para encontrar portería."

    if pick_type == "btts_no":
        return "El escenario permite que uno de los dos equipos se quede sin marcar."

    if pick_type == "team_cards":
        return "Predicción de tarjetas basada en presión defensiva, perfil del equipo y contexto del partido."

    if pick_type == "team_score_first_half":
        return "Predicción de gol en primera parte según ritmo inicial y producción esperada."

    if pick_type == "team_score_second_half":
        return "Predicción de gol en segunda parte, donde suele haber más espacios."

    return "Selección generada por el modelo."


def enrich_option_with_market(
    match: Dict[str, Any],
    option: Dict[str, Any],
    odds_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    home = match["home_team"]
    away = match["away_team"]
    league = match["league"]

    direct_key = (
        simplify_team_name(home),
        simplify_team_name(away),
        normalize_text(league),
    )

    reverse_key = (
        simplify_team_name(away),
        simplify_team_name(home),
        normalize_text(league),
    )

    odds_data = odds_index.get(direct_key) or odds_index.get(reverse_key)

    bookmaker = None
    bookmaker_market = None
    bookmaker_odds = None
    odds_source = "synthetic"

    if option["pick_type"] == "winner" and odds_data:
        bookmaker = odds_data.get("bookmaker")
        bookmaker_market = odds_data.get("market")

        winner_team = option.get("winner_team")

        if winner_team == home:
            bookmaker_odds = odds_data.get("home")
        elif winner_team == away:
            bookmaker_odds = odds_data.get("away")

        if bookmaker_odds:
            odds_source = "real"
            adjusted_conf, _ = market_read_adjustment(bookmaker_odds, option["confidence"])
            option["confidence"] = adjusted_conf

    if bookmaker_odds is None:
        bookmaker_odds = safe_odds_from_confidence(option["confidence"], option["pick_type"])

    score = option["confidence"] + market_reliability_bonus(option["pick_type"])

    enriched = {
        "id": match["id"],
        "match": match["match"],
        "league": league,
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": option["pick"],
        "pick_type": option["pick_type"],
        "confidence": int(option["confidence"]),
        "confidence_band": confidence_band(public_confidence(int(option["confidence"]))),
        "tier": classify_pick(public_confidence(int(option["confidence"]))),
        "score": score,
        "odds_estimate": round(bookmaker_odds, 2) if bookmaker_odds is not None else None,
        "odds_source": odds_source,
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "source": match.get("source", "unknown"),
        "bookmaker": bookmaker,
        "bookmaker_market": bookmaker_market,
        "tipster_explanation": tipster_explanation(option),
        "cards": predict_cards(
            league,
            option.get("home_strength", stable_team_rating(home)),
            option.get("away_strength", stable_team_rating(away)),
            home,
            away,
        ),
        "cards_team": option.get("cards_team"),
        "cards_line": option.get("cards_line"),
        "scoring_team": option.get("scoring_team"),
        "half": option.get("half"),
        "trackable": bool(option.get("trackable", False)),
        "recommended_for_combo": option["pick_type"] in SAFE_COMBO_MARKETS,
    }

    return enriched
def build_all_markets_for_match(
    match: Dict[str, Any],
    odds_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    options = build_market_options(match)
    enriched_options = [enrich_option_with_market(match, dict(o), odds_index) for o in options]

    enriched_options.sort(
        key=lambda x: (
            x.get("score", 0),
            x.get("confidence", 0),
        ),
        reverse=True,
    )

    return {
        "id": match["id"],
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "source": match.get("source", "unknown"),
        "markets": enriched_options,
    }


def compatible_with_builder(existing: List[Dict[str, Any]], candidate: Dict[str, Any]) -> bool:
    existing_types = {x.get("pick_type") for x in existing}
    ctype = candidate.get("pick_type")

    incompatible_pairs = [
        ("btts_yes", "btts_no"),
        ("over_2_5", "under_3_5"),
        ("winner", "double_chance"),
        ("team_score_first_half", "btts_no"),
    ]

    for a, b in incompatible_pairs:
        if ctype == a and b in existing_types:
            return False
        if ctype == b and a in existing_types:
            return False

    if ctype in existing_types:
        return False

    return True


def build_bet_builder_for_match(
    match: Dict[str, Any],
    odds_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not league_team_sanity_check(match["league"], match["home_team"], match["away_team"]):
        return None

    options = build_market_options(match)
    enriched = [enrich_option_with_market(match, dict(o), odds_index) for o in options]

    enriched.sort(
        key=lambda x: (
            x.get("confidence", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    base_candidates = [
        x for x in enriched
        if x.get("pick_type") in {"double_chance", "under_3_5", "winner"}
        and x.get("confidence", 0) >= 68
    ]

    base_candidates.sort(
        key=lambda x: (
            1 if x.get("pick_type") == "double_chance" else 0,
            1 if x.get("pick_type") == "under_3_5" else 0,
            x.get("confidence", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    if not base_candidates:
        return None

    builder: List[Dict[str, Any]] = [base_candidates[0]]

    preferred_order = [
        "under_3_5",
        "over_2_5",
        "btts_yes",
        "btts_no",
        "team_score_second_half",
        "team_cards",
        "team_score_first_half",
    ]

    for wanted_type in preferred_order:
        if len(builder) >= MAX_BUILDER_SELECTIONS:
            break

        candidates = [
            x for x in enriched
            if x.get("pick_type") == wanted_type
            and compatible_with_builder(builder, x)
            and x.get("confidence", 0) >= 68
        ]

        if wanted_type == "team_cards":
            candidates = [x for x in candidates if x.get("confidence", 0) >= 74]

        if wanted_type in {"team_score_first_half", "team_score_second_half"}:
            candidates = [x for x in candidates if x.get("confidence", 0) >= 70]

        candidates.sort(
            key=lambda x: (
                x.get("confidence", 0),
                x.get("score", 0),
            ),
            reverse=True,
        )

        if not candidates:
            continue

        test_builder = builder + [candidates[0]]

        total_odds_test = 1.0
        for item in test_builder:
            total_odds_test *= float(item.get("odds_estimate") or 1)

        if total_odds_test <= MAX_BUILDER_ODDS:
            builder.append(candidates[0])

    if len(builder) < MIN_BUILDER_SELECTIONS:
        return None

    total_odds = 1.0
    for item in builder:
        odds = item.get("odds_estimate")
        if not odds:
            return None
        total_odds *= float(odds)

    if total_odds < MIN_BUILDER_ODDS or total_odds > MAX_BUILDER_ODDS:
        return None

    confidence_raw = int(sum(x.get("confidence", 0) for x in builder) / len(builder))
    confidence = public_confidence(confidence_raw)

    selections = [x.get("pick", "--") for x in builder]
    builder_text = "Crear apuesta: " + " + ".join(selections)

    return {
        "id": match["id"],
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick_type": "bet_builder",
        "pick": builder_text,
        "selections": selections,
        "legs": builder,
        "confidence": confidence,
        "confidence_band": confidence_band(confidence),
        "tier": classify_pick(confidence),
        "score": sum(x.get("score", 0) for x in builder),
        "odds_estimate": round(total_odds, 2),
        "odds_source": "mixed",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "status": "pending",
        "score_line": "",
        "source": match.get("source", "unknown"),
        "recommended_for_combo": False,
        "trackable": False,
        "stake": None,
        "tipster_explanation": (
            "Apuesta creada por Tipster Tips Pro con mercados compatibles del mismo partido. "
            "El modelo prioriza lectura principal, control de goles y complementos solo si superan el filtro."
        ),
        "cards": predict_cards(
            match["league"],
            stable_team_rating(match["home_team"]) + 3.2,
            stable_team_rating(match["away_team"]),
            match["home_team"],
            match["away_team"],
        ),
    }


def extract_simple_combo_candidates(match_catalog: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []
    allowed = {"double_chance", "under_3_5", "btts_no"}

    for item in match_catalog:
        for market in item.get("markets", []):
            if market.get("pick_type") not in allowed:
                continue

            if market.get("confidence", 0) < 72:
                continue

            if not market.get("odds_estimate"):
                continue

            candidates.append(market)

    candidates.sort(
        key=lambda x: (
            x.get("confidence", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    return candidates


def is_today_or_tomorrow_pick(pick: Dict[str, Any]) -> bool:
    kickoff_iso = pick.get("kickoff_iso")

    if not kickoff_iso:
        return False

    try:
        dt = datetime.fromisoformat(kickoff_iso)
    except Exception:
        return False

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    pick_date = dt.astimezone(TZ).date()
    today = now_local().date()
    tomorrow = today + timedelta(days=1)

    return pick_date in {today, tomorrow}


def get_premium_single_pick(
    picks: List[Dict[str, Any]],
    combo: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    combo_matches = {p.get("match") for p in combo.get("picks", [])}

    candidates = [
        p for p in picks
        if p.get("match") not in combo_matches
    ]

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x.get("confidence", 0),
            x.get("score", 0),
            -abs(float(x.get("odds_estimate", 0) or 0) - 2.50),
        ),
        reverse=True,
    )

    return candidates[0]


def build_picks() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    matches = get_real_matches()
    odds_index = fetch_live_odds_index()

    catalog = [build_all_markets_for_match(m, odds_index) for m in matches]

    candidates: List[Dict[str, Any]] = []

    for match in matches:
        builder = build_bet_builder_for_match(match, odds_index)

        if not builder:
            continue

        odds = float(builder.get("odds_estimate") or 0)

        if MIN_BUILDER_ODDS <= odds <= MAX_BUILDER_ODDS:
            candidates.append(builder)

    candidates.sort(
        key=lambda x: (
            x.get("confidence", 0),
            -abs(float(x.get("odds_estimate", 0) or 0) - 3.0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    return candidates[:TARGET_PICKS], catalog


def build_combo(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not picks:
        return {"size": 0, "estimated_total_odds": None, "confidence": 0, "picks": []}

    combo_candidates = []

    for p in picks:
        pick_type = p.get("pick_type")
        odds = p.get("odds_estimate")
        confidence = p.get("confidence", 0)

        if not odds:
            continue

        odds = float(odds)

        if odds < 1.20 or odds > 3.50:
            continue

        if pick_type == "bet_builder":
            combo_score = confidence + 8
        elif pick_type == "double_chance":
            combo_score = confidence + 10
        elif pick_type == "under_3_5":
            combo_score = confidence + 9
        elif pick_type == "btts_no":
            combo_score = confidence + 5
        else:
            continue

        combo_candidates.append({**p, "combo_score": combo_score})

    combo_candidates.sort(
        key=lambda x: (
            x.get("combo_score", 0),
            x.get("confidence", 0),
            -float(x.get("odds_estimate", 0) or 0),
        ),
        reverse=True,
    )

    combo = []
    used_matches = set()

    for pick in combo_candidates:
        if pick.get("match") in used_matches:
            continue

        combo.append(pick)
        used_matches.add(pick.get("match"))

        if len(combo) == 3:
            break

    if len(combo) < 3:
        return {
            "size": len(combo),
            "estimated_total_odds": None,
            "confidence": 0,
            "picks": combo,
        }

    total_odds = 1.0

    for p in combo:
        total_odds *= float(p["odds_estimate"])

    confidence = public_confidence(
        int(sum(p["confidence"] for p in combo) / len(combo))
    )

    return {
        "size": 3,
        "estimated_total_odds": round(total_odds, 2),
        "confidence": confidence,
        "picks": combo,
    }


def group_picks(picks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "premium": [p for p in picks if p["tier"] == "premium"],
        "strong": [p for p in picks if p["tier"] == "strong"],
        "medium": [p for p in picks if p["tier"] == "medium"],
        "risky": [p for p in picks if p["tier"] == "risky"],
    }


def evaluate_pick_result(pick: Dict[str, Any], home_goals: int, away_goals: int) -> str:
    pick_type = pick.get("pick_type")
    selected_pick = pick.get("pick", "")

    if pick_type == "bet_builder":
        legs = pick.get("legs") or []
        if not legs:
            return "pending"

        for leg in legs:
            leg_type = leg.get("pick_type")

            if leg_type in {"team_cards", "team_score_first_half", "team_score_second_half"}:
                return "pending"

            leg_pick = dict(pick)
            leg_pick["pick_type"] = leg_type
            leg_pick["pick"] = leg.get("pick", "")

            result = evaluate_pick_result(leg_pick, home_goals, away_goals)

            if result == "lost":
                return "lost"

            if result != "won":
                return "pending"

        return "won"

    if pick_type == "winner":
        if home_goals > away_goals and selected_pick == f"Gana {pick.get('home_team')}":
            return "won"
        if away_goals > home_goals and selected_pick == f"Gana {pick.get('away_team')}":
            return "won"
        return "lost"

    if pick_type == "double_chance":
        home = pick.get("home_team")
        away = pick.get("away_team")

        if selected_pick == f"1X {home}":
            return "won" if home_goals >= away_goals else "lost"

        if selected_pick == f"X2 {away}":
            return "won" if away_goals >= home_goals else "lost"

        return "lost"

    if pick_type == "over_2_5":
        return "won" if (home_goals + away_goals) > 2 else "lost"

    if pick_type == "under_2_5":
        return "won" if (home_goals + away_goals) < 3 else "lost"

    if pick_type == "under_3_5":
        return "won" if (home_goals + away_goals) < 4 else "lost"

    if pick_type == "btts_yes":
        return "won" if home_goals > 0 and away_goals > 0 else "lost"

    if pick_type == "btts_no":
        return "won" if home_goals == 0 or away_goals == 0 else "lost"

    return "pending"


def get_finished_scores_football_data() -> List[Dict[str, Any]]:
    results = []

    try:
        start_date = (now_local() - timedelta(days=10)).date().isoformat()
        end_date = now_local().date().isoformat()

        for code, league_name in FOOTBALL_DATA_LEAGUES.items():
            data = football_data_get(
                f"/competitions/{code}/matches",
                {
                    "dateFrom": start_date,
                    "dateTo": end_date,
                },
            )

            for item in data.get("matches") or []:
                status = (item.get("status") or "").upper()

                if status not in ["FINISHED", "AWARDED"]:
                    continue

                home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                away = ((item.get("awayTeam") or {}).get("name") or "").strip()
                full_time = ((item.get("score") or {}).get("fullTime") or {})

                home_goals = full_time.get("home")
                away_goals = full_time.get("away")

                if not home or not away or home_goals is None or away_goals is None:
                    continue

                dt_local = datetime.fromisoformat(item["utcDate"].replace("Z", "+00:00")).astimezone(TZ)

                results.append({
                    "home_team": home,
                    "away_team": away,
                    "league": league_name,
                    "kickoff_iso": dt_local.isoformat(),
                    "home_goals": int(home_goals),
                    "away_goals": int(away_goals),
                    "score_line": f"{home_goals}-{away_goals}",
                })

    except Exception:
        pass

    return results


def update_history_finished_matches(history: Dict[str, Any]) -> Dict[str, Any]:
    finished_results = get_finished_scores_football_data()

    def same_match(pick: Dict[str, Any], result: Dict[str, Any]) -> bool:
        if normalize_text(pick.get("league")) != normalize_text(result.get("league")):
            return False

        if simplify_team_name(pick.get("home_team", "")) != simplify_team_name(result.get("home_team", "")):
            return False

        if simplify_team_name(pick.get("away_team", "")) != simplify_team_name(result.get("away_team", "")):
            return False

        try:
            pick_dt = datetime.fromisoformat(pick.get("kickoff_iso"))
            result_dt = datetime.fromisoformat(result.get("kickoff_iso"))
            return pick_dt.date() == result_dt.date()
        except Exception:
            return True

    for _, day_data in history.get("days", {}).items():
        for pick in day_data.get("picks", []):
            if pick.get("pick_type") not in TRACKABLE_MARKETS:
                continue

            if pick.get("status") in ["won", "lost"]:
                continue

            matched_result = None

            for result in finished_results:
                if same_match(pick, result):
                    matched_result = result
                    break

            if not matched_result:
                continue

            pick["score_line"] = matched_result["score_line"]
            pick["status"] = evaluate_pick_result(
                pick,
                matched_result["home_goals"],
                matched_result["away_goals"],
            )

    return refresh_history_stats(history)


def refresh_history_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    history.setdefault("days", {})

    for _, day_data in history["days"].items():
        picks = day_data.get("picks", [])
        tracked = [p for p in picks if p.get("pick_type") in TRACKABLE_MARKETS]

        day_data["stats"] = {
            "won": sum(1 for p in tracked if p.get("status") == "won"),
            "lost": sum(1 for p in tracked if p.get("status") == "lost"),
            "pending": sum(1 for p in tracked if p.get("status") == "pending"),
        }

    return history


def trim_history(history: Dict[str, Any]) -> Dict[str, Any]:
    days_obj = history.get("days", {})
    sorted_keys = sorted(days_obj.keys(), reverse=True)
    keep = set(sorted_keys[:MAX_HISTORY_DAYS])
    history["days"] = {k: v for k, v in days_obj.items() if k in keep}
    return history


def merge_today_history(history: Dict[str, Any], picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    history.setdefault("days", {})
    day = today_key()

    existing_day = history["days"].get(day, {"picks": []})
    existing_picks = existing_day.get("picks", [])

    existing_index = {}

    for p in existing_picks:
        key = (
            simplify_team_name(p.get("home_team")),
            simplify_team_name(p.get("away_team")),
            normalize_text(p.get("league")),
            p.get("kickoff_iso"),
            p.get("pick_type"),
            p.get("pick"),
        )
        existing_index[key] = p

    for p in picks:
        key = (
            simplify_team_name(p.get("home_team")),
            simplify_team_name(p.get("away_team")),
            normalize_text(p.get("league")),
            p.get("kickoff_iso"),
            p.get("pick_type"),
            p.get("pick"),
        )

        if key not in existing_index:
            existing_picks.append(p)
        else:
            old = existing_index[key]
            for field in [
                "confidence", "confidence_band", "tier", "score", "odds_estimate",
                "odds_source", "tipster_explanation", "source", "selections", "legs",
                "cards",
            ]:
                if field in p:
                    old[field] = p[field]

    history["days"][day] = {"picks": existing_picks}
    return trim_history(refresh_history_stats(history))


def history_to_frontend(history: Dict[str, Any], page: int = 1, page_size: int = HISTORY_PAGE_SIZE) -> Dict[str, Any]:
    days_obj = history.get("days", {})
    all_picks = []

    for day, data in sorted(days_obj.items(), reverse=True):
        for p in data.get("picks", []):
            item = dict(p)
            item["history_date"] = day
            all_picks.append(item)

    total_items = len(all_picks)
    total_pages = max(1, math.ceil(total_items / page_size))
    page = max(1, min(page, total_pages))

    start = (page - 1) * page_size
    end = start + page_size

    return {
        "page": page,
        "page_size": page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "items": all_picks[start:end],
    }


def compute_dashboard_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    won = 0
    lost = 0
    pending = 0
    total = 0

    for _, day in history.get("days", {}).items():
        for pick in day.get("picks", []):
            if pick.get("pick_type") not in TRACKABLE_MARKETS:
                continue

            status = pick.get("status")

            if status == "pending":
                pending += 1
                total += 1
            elif status == "won":
                won += 1
                total += 1
            elif status == "lost":
                lost += 1
                total += 1

    resolved = won + lost
    effectiveness = round((won / resolved) * 100, 1) if resolved > 0 else 0.0

    return {
        "hits": f"{won}/{resolved}" if resolved > 0 else "0/0",
        "effectiveness": effectiveness,
        "profit": 0.0,
        "total_picks": total,
        "pending": pending,
    }


def build_payload() -> Dict[str, Any]:
    try:
        picks, match_catalog = build_picks()
    except Exception:
        picks, match_catalog = [], []

    history = read_json(HISTORY_FILE)

    if picks:
        history = merge_today_history(history, picks)

    history = update_history_finished_matches(history)
    refresh_model_stats_from_history(history)
    dashboard_stats = compute_dashboard_stats(history)

    combo_pool = [
        p for p in (picks + extract_simple_combo_candidates(match_catalog))
        if is_today_or_tomorrow_pick(p)
    ]

    combo = build_combo(combo_pool) if combo_pool else {}
    premium_single = get_premium_single_pick(picks, combo) if picks else None

    payload = {
        "generated_at": now_local().isoformat(),
        "cache_day": today_key(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
        "match_catalog": match_catalog,
        "combo_of_day": combo,
        "premium_single": premium_single,
        "groups": group_picks(picks) if picks else {
            "premium": [],
            "strong": [],
            "medium": [],
            "risky": [],
        },
        "dashboard_stats": dashboard_stats,
    }

    try:
        write_json(HISTORY_FILE, history)
    except Exception:
        pass

    try:
        write_json(CACHE_FILE, payload)
    except Exception:
        pass

    return payload


def get_cached_or_refresh(force_refresh: bool = False) -> Dict[str, Any]:
    cache = read_json(CACHE_FILE)

    if not force_refresh and cache_is_valid(cache):
        return cache

    return build_payload()


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "msg": "Tipster Tips Pro API funcionando",
    }


@app.get("/test")
def test() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/test-api")
def test_api() -> Dict[str, Any]:
    try:
        sportsdb_matches = get_sportsdb_matches()
        football_data_matches = get_football_data_matches()
        api_football_matches = get_api_football_matches()
        merged = get_real_matches()
        odds_index = fetch_live_odds_index()
        state = load_api_state()

        return {
            "ok": True,
            "sportsdb_count": len(sportsdb_matches),
            "football_data_count": len(football_data_matches),
            "api_football_count": len(api_football_matches),
            "final_count": len(merged),
            "odds_count": len(odds_index),
            "api_state": state,
            "matches": [
                {
                    "match": m["match"],
                    "league": m["league"],
                    "time_local": m["dt_local"].strftime("%d/%m %H:%M"),
                    "source": m["source"],
                }
                for m in merged[:20]
            ],
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/picks")
def picks(force_refresh: bool = Query(False)) -> Dict[str, Any]:
    try:
        return get_cached_or_refresh(force_refresh=force_refresh)
    except Exception as e:
        cache = read_json(CACHE_FILE)

        if cache:
            return cache

        return {
            "error": True,
            "message": str(e),
            "count": 0,
            "picks": [],
            "match_catalog": [],
            "combo_of_day": {},
            "premium_single": None,
            "groups": {
                "premium": [],
                "strong": [],
                "medium": [],
                "risky": [],
            },
            "dashboard_stats": {
                "hits": "0/0",
                "effectiveness": 0.0,
                "profit": 0.0,
                "total_picks": 0,
                "pending": 0,
            },
        }


@app.get("/api/history")
def history(
    page: int = Query(1, ge=1),
    page_size: int = Query(HISTORY_PAGE_SIZE, ge=1, le=100),
) -> Dict[str, Any]:
    try:
        raw = read_json(HISTORY_FILE)
        raw = update_history_finished_matches(raw)
        raw = refresh_history_stats(raw)
        raw = trim_history(raw)
        write_json(HISTORY_FILE, raw)
        refresh_model_stats_from_history(raw)
        return history_to_frontend(raw, page=page, page_size=page_size)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get("/api/odds")
def odds_snapshot() -> Dict[str, Any]:
    try:
        odds = fetch_live_odds_index()
        items = []

        for key, value in odds.items():
            items.append({
                "match_key": key,
                "bookmaker": value.get("bookmaker"),
                "market": value.get("market"),
                "home": value.get("home"),
                "draw": value.get("draw"),
                "away": value.get("away"),
            })

        return {"count": len(items), "items": items}

    except Exception as e:
        return {"count": 0, "items": [], "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)