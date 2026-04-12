import json
import math
import os
import unicodedata
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

ALLSPORTS_API_KEY = os.getenv("ALLSPORTS_API_KEY", "").strip()
ALLSPORTS_BASE_URL = "https://apiv2.allsportsapi.com/football/"

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"
API_STATE_FILE = "api_state.json"
MODEL_STATS_FILE = "model_stats.json"

LOOKAHEAD_HOURS = 168
CACHE_REFRESH_MINUTES = 15
MAX_PICKS = 20
MIN_PICKS_ALWAYS = 6
MAX_HISTORY_DAYS = 30
API_COOLDOWN_MINUTES = 10
MIN_CONFIDENCE = 66
HISTORY_PAGE_SIZE = 12

API_PRIORITY = ["api_football", "football_data", "allsports", "sportsdb"]

SPORTSDB_LEAGUES = {
    "4328": "LaLiga",
    "4400": "Segunda División",
    "4480": "Champions League",
}

API_FOOTBALL_LEAGUES = {
    140: "LaLiga",
    2: "Champions League",
}

FOOTBALL_DATA_LEAGUES = {
    "PD": "LaLiga",
    "SD": "Segunda División",
    "CL": "Champions League",
}

ALLSPORTS_LEAGUES = {
    302: "LaLiga",
    3: "Champions League",
}

SEASON_CANDIDATES_SPORTSDB = ["2025-2026", "2024-2025"]

ODDS_SPORT_KEYS = {
    "LaLiga": "soccer_spain_la_liga",
    "Segunda División": "soccer_spain_segunda_division",
    "Champions League": "soccer_uefa_champs_league",
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
    "Real Sociedad de Fútbol": 82,
    "Villarreal": 81,
    "Villarreal CF": 81,
    "Real Betis": 80,
    "Real Betis Balompié": 80,
    "Girona": 80,
    "Girona FC": 80,
    "Valencia": 77,
    "Valencia CF": 77,
    "Sevilla": 78,
    "Sevilla FC": 78,
    "Osasuna": 76,
    "CA Osasuna": 76,
    "Getafe": 74,
    "Getafe CF": 74,
    "Mallorca": 74,
    "RCD Mallorca": 74,
    "Rayo Vallecano": 75,
    "Rayo Vallecano de Madrid": 75,
    "Celta Vigo": 75,
    "RC Celta de Vigo": 75,
    "Las Palmas": 72,
    "UD Las Palmas": 72,
    "Alaves": 73,
    "Alavés": 73,
    "Deportivo Alavés": 73,

    "Almería": 78,
    "Almeria": 78,
    "UD Almería": 78,
    "Granada": 77,
    "Granada CF": 77,
    "Cádiz": 76,
    "Cadiz": 76,
    "Cádiz CF": 76,
    "Levante": 76,
    "Levante UD": 76,
    "Real Oviedo": 74,
    "Real Zaragoza": 73,
    "Sporting Gijón": 73,
    "Sporting Gijon": 73,
    "Real Sporting": 73,
    "Eibar": 74,
    "SD Eibar": 74,
    "Elche": 75,
    "Elche CF": 75,
    "Racing Santander": 73,
    "Racing de Santander": 73,
    "Tenerife": 71,
    "CD Tenerife": 71,
    "Huesca": 71,
    "SD Huesca": 71,
    "Burgos": 71,
    "Burgos CF": 71,
    "Albacete": 71,
    "Albacete Balompié": 71,
    "Castellón": 70,
    "Castellon": 70,
    "CD Castellón": 70,
    "CD Castellon": 70,
    "Málaga": 72,
    "Malaga": 72,
    "Málaga CF": 72,
    "Malaga CF": 72,
    "Córdoba": 70,
    "Cordoba": 70,
    "Córdoba CF": 70,
    "Cordoba CF": 70,
    "Deportivo La Coruña": 72,
    "Deportivo de La Coruña": 72,
    "Deportivo La Coruna": 72,
    "Deportivo de La Coruna": 72,

    "Manchester City": 94,
    "Manchester City FC": 94,
    "Arsenal": 91,
    "Arsenal FC": 91,
    "Liverpool": 91,
    "Liverpool FC": 91,
    "Bayern Munich": 92,
    "FC Bayern München": 92,
    "FC Bayern Munchen": 92,
    "Borussia Dortmund": 86,
    "Paris Saint Germain": 91,
    "Paris SG": 91,
    "Paris Saint-Germain FC": 91,
    "Inter": 90,
    "FC Internazionale Milano": 90,
    "Juventus": 86,
    "Juventus FC": 86,
    "AC Milan": 86,
    "Napoli": 84,
    "SSC Napoli": 84,
    "Benfica": 84,
    "SL Benfica": 84,
    "FC Porto": 83,
    "PSV Eindhoven": 85,
    "PSV": 85,
    "RB Leipzig": 84,
    "Sporting CP": 83,
    "Sporting Lisbon": 83,
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
    "alaves",
    "sporting",
}

app = FastAPI(title="Top Picks Pro Premium")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# FILE HELPERS
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

# =========================================================
# TIME / NORMALIZE
# =========================================================

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
        " club", " futbol", " football club"
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

    age = now_local() - dt.astimezone(TZ)
    return age < timedelta(minutes=CACHE_REFRESH_MINUTES)


def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]
    h = abs(hash(team_name)) % 1000
    return 68 + (h / 1000) * 14


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

# =========================================================
# API STATE / COOLDOWN
# =========================================================

def load_api_state() -> Dict[str, Any]:
    state = read_json(API_STATE_FILE)
    for name in ["sportsdb", "football_data", "api_football", "allsports", "odds_api"]:
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

    raise ValueError("No se pudo extraer home/away")


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
    start_date = start.date().isoformat()
    end_date = end.date().isoformat()
    season = current_api_football_season()
    out: List[Dict[str, Any]] = []

    try:
        for league_id, league_name in API_FOOTBALL_LEAGUES.items():
            data = api_football_get(
                "/fixtures",
                {
                    "league": league_id,
                    "season": season,
                    "from": start_date,
                    "to": end_date,
                    "timezone": "Europe/Madrid",
                },
            )

            items = data.get("response") or []
            for item in items:
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
    start_date = start.date().isoformat()
    end_date = end.date().isoformat()
    out: List[Dict[str, Any]] = []

    try:
        for code, league_name in FOOTBALL_DATA_LEAGUES.items():
            data = football_data_get(
                f"/competitions/{code}/matches",
                {
                    "dateFrom": start_date,
                    "dateTo": end_date,
                },
            )

            items = data.get("matches") or []
            for item in items:
                try:
                    utc_date = item.get("utcDate")
                    home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                    away = ((item.get("awayTeam") or {}).get("name") or "").strip()

                    if not utc_date or not home or not away:
                        continue

                    dt_local = datetime.fromisoformat(utc_date.replace("Z", "+00:00")).astimezone(TZ)
                    if not (start <= dt_local <= end):
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
# ALLSPORTS
# =========================================================

def allsports_get(params: Dict[str, Any]) -> Dict[str, Any]:
    if not ALLSPORTS_API_KEY:
        raise RuntimeError("Falta ALLSPORTS_API_KEY")

    merged_params = {"APIkey": ALLSPORTS_API_KEY}
    merged_params.update(params)

    r = requests.get(ALLSPORTS_BASE_URL, params=merged_params, timeout=15)
    r.raise_for_status()
    return r.json()


def parse_allsports_datetime(event_date: Optional[str], event_time: Optional[str]) -> datetime:
    date_str = (event_date or "").strip()
    time_str = (event_time or "00:00").strip()

    if not date_str:
        raise ValueError("Missing event_date")

    if len(time_str) == 5:
        dt_naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    else:
        dt_naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

    return TZ.localize(dt_naive)


def get_allsports_matches() -> List[Dict[str, Any]]:
    if not api_is_available("allsports"):
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    start_date = start.date().isoformat()
    end_date = end.date().isoformat()
    out: List[Dict[str, Any]] = []

    try:
        for league_id, league_name in ALLSPORTS_LEAGUES.items():
            data = allsports_get(
                {
                    "met": "Fixtures",
                    "leagueId": league_id,
                    "from": start_date,
                    "to": end_date,
                }
            )

            items = data.get("result") or []
            for item in items:
                try:
                    home = (item.get("event_home_team") or "").strip()
                    away = (item.get("event_away_team") or "").strip()
                    event_date = item.get("event_date")
                    event_time = item.get("event_time")

                    if not home or not away or not event_date:
                        continue

                    dt_local = parse_allsports_datetime(event_date, event_time)
                    if not (start <= dt_local <= end):
                        continue

                    out.append({
                        "id": item.get("event_key"),
                        "match": f"{home} vs {away}",
                        "league": league_name,
                        "home_team": home,
                        "away_team": away,
                        "dt_local": dt_local,
                        "source": "allsports",
                    })
                except Exception:
                    continue

        clear_api_cooldown("allsports")
        out.sort(key=lambda x: x["dt_local"])
        return out
    except Exception as e:
        set_api_cooldown("allsports", parse_requests_error(e))
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
        markets = book.get("markets") or []
        for market in markets:
            if market.get("key") != "h2h":
                continue

            outcomes = market.get("outcomes") or []
            home_price = None
            away_price = None
            draw_price = None

            for outcome in outcomes:
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

            if home_price is None and away_price is None:
                continue

            nums = [x for x in [home_price, away_price, draw_price] if isinstance(x, (int, float))]
            avg = sum(nums) / len(nums) if nums else 999.0

            candidate = {
                "bookmaker": title,
                "market": "1X2",
                "home": home_price,
                "draw": draw_price,
                "away": away_price,
                "avg": avg,
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
                        if team_names_match(teams[0], home):
                            away = teams[1]
                        else:
                            away = teams[0]

                if not home or not away:
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
# MODEL STATS / LEARNING
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
            if status not in ["won", "lost"]:
                continue

            market = pick.get("pick_type", "unknown")
            league = pick.get("league", "unknown")

            stats["by_market"].setdefault(market, {"won": 0, "lost": 0})
            stats["by_league"].setdefault(league, {"won": 0, "lost": 0})

            stats["by_market"][market][status] += 1
            stats["by_league"][league][status] += 1

    return stats


def get_adjustment_from_stats(league: str, pick_type: str) -> int:
    stats = load_model_stats()

    def ratio(bucket: Dict[str, int]) -> Optional[float]:
        total = bucket.get("won", 0) + bucket.get("lost", 0)
        if total < 8:
            return None
        return bucket.get("won", 0) / total

    league_ratio = ratio(stats["by_league"].get(league, {}))
    market_ratio = ratio(stats["by_market"].get(pick_type, {}))

    adjustment = 0

    if league_ratio is not None:
        if league_ratio >= 0.64:
            adjustment += 2
        elif league_ratio <= 0.45:
            adjustment -= 2

    if market_ratio is not None:
        if market_ratio >= 0.62:
            adjustment += 2
        elif market_ratio <= 0.45:
            adjustment -= 3

    return adjustment

# =========================================================
# MERGE / DEDUP
# =========================================================

def get_real_matches() -> List[Dict[str, Any]]:
    matches_by_source = {
        "api_football": get_api_football_matches(),
        "football_data": get_football_data_matches(),
        "allsports": get_allsports_matches(),
        "sportsdb": get_sportsdb_matches(),
    }

    combined: List[Dict[str, Any]] = []
    for src in API_PRIORITY:
        combined.extend(matches_by_source.get(src, []))

    dedup: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for m in combined:
        date_key = m["dt_local"].strftime("%Y-%m-%d %H:%M")
        key = (
            simplify_team_name(m["home_team"]),
            simplify_team_name(m["away_team"]),
            normalize_text(m["league"]),
            date_key,
        )

        if key not in dedup:
            dedup[key] = m
            continue

        old = dedup[key]
        if source_priority(m["source"]) < source_priority(old["source"]):
            dedup[key] = m

    unique = list(dedup.values())
    unique.sort(key=lambda x: x["dt_local"])
    return unique[:MAX_PICKS]