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
# =========================================================
# HELPERS DE MERCADO / VALOR
# =========================================================

def implied_probability(odds: float) -> Optional[float]:
    if not odds or odds <= 1:
        return None
    return round(100 / odds, 1)


def calculate_value(confidence: int, odds: Optional[float]) -> Dict[str, Any]:
    if not odds:
        return {
            "model_prob": None,
            "book_prob": None,
            "edge": None,
            "has_value": False,
            "stake": 0,
        }

    model_prob = float(confidence)
    book_prob = implied_probability(odds)
    edge = round(model_prob - book_prob, 1) if book_prob is not None else None

    stake = 0
    if edge is not None:
        if edge >= 12:
            stake = 5
        elif edge >= 8:
            stake = 4
        elif edge >= 5:
            stake = 3
        elif edge >= 3:
            stake = 2
        elif edge >= 1.5:
            stake = 1

    return {
        "model_prob": round(model_prob, 1),
        "book_prob": book_prob,
        "edge": edge,
        "has_value": edge is not None and edge >= 1.5,
        "stake": stake,
    }


def confidence_band(confidence: int) -> str:
    if confidence >= 80:
        return "alta"
    if confidence >= 72:
        return "media"
    return "intermedia"


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
        confidence += 2
        return min(confidence, 92), "El mercado acompaña bastante bien la lectura del pick."
    if diff <= -8:
        confidence -= 6
        return max(confidence, 60), "El mercado es mucho más prudente y eso obliga a bajar la confianza."
    if diff <= -4:
        confidence -= 3
        return max(confidence, 60), "El mercado no va tan fuerte en esta dirección y ajusto algo la confianza."

    return confidence, ""


def safe_odds_from_confidence(confidence: int, market_type: str) -> float:
    # fallback interno para evitar que falten picks.
    if market_type == "double_chance":
        base = 1.28 + (100 - confidence) * 0.006
        return round(min(max(base, 1.22), 1.65), 2)

    if market_type == "under_3_5":
        base = 1.38 + (100 - confidence) * 0.007
        return round(min(max(base, 1.28), 1.88), 2)

    if market_type == "under_2_5":
        base = 1.62 + (100 - confidence) * 0.008
        return round(min(max(base, 1.45), 2.25), 2)

    if market_type == "over_2_5":
        base = 1.68 + (100 - confidence) * 0.008
        return round(min(max(base, 1.52), 2.30), 2)

    if market_type == "btts_yes":
        base = 1.70 + (100 - confidence) * 0.008
        return round(min(max(base, 1.55), 2.35), 2)

    if market_type == "btts_no":
        base = 1.66 + (100 - confidence) * 0.008
        return round(min(max(base, 1.50), 2.30), 2)

    if market_type == "team_cards":
        base = 1.55 + (100 - confidence) * 0.007
        return round(min(max(base, 1.40), 2.10), 2)

    base = 1.65 + (100 - confidence) * 0.008
    return round(min(max(base, 1.35), 2.60), 2)


# =========================================================
# HELPERS DE PRONÓSTICO
# =========================================================

def predict_cards(league: str, home_strength: float, away_strength: float, home: str, away: str) -> Dict[str, int]:
    base_cards = {
        "LaLiga": 5,
        "Segunda División": 6,
        "Champions League": 4,
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
        line = 1.5 if league == "Champions League" else 2.5
        conf = 72 + min(abs_diff * 1.2, 10)
    elif diff <= -4:
        card_team = home
        card_team_s = home_s
        line = 1.5 if league == "Champions League" else 2.5
        conf = 72 + min(abs_diff * 1.2, 10)
    else:
        if away_s in AGGRESSIVE_CARD_TEAMS:
            card_team = away
            card_team_s = away_s
            line = 1.5
            conf = 73
        else:
            card_team = home
            card_team_s = home_s
            line = 1.5
            conf = 72

    if card_team_s in AGGRESSIVE_CARD_TEAMS:
        conf += 5

    conf = int(min(conf, 88))
    pick_text = f"Más de {line} tarjetas {card_team}"

    return {
        "pick": pick_text,
        "pick_type": "team_cards",
        "confidence": conf,
        "cards_team": card_team,
        "cards_line": line,
    }


def build_market_options(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    home = match["home_team"]
    away = match["away_team"]
    league = match["league"]

    home_strength = stable_team_rating(home) + 3.2
    away_strength = stable_team_rating(away)

    if league == "Segunda División":
        home_strength -= 1.0

    diff = home_strength - away_strength
    abs_diff = abs(diff)

    home_xg = max(0.55, min(1.20 + diff * 0.035, 2.80))
    away_xg = max(0.40, min(1.00 - diff * 0.022, 2.30))
    total_xg = home_xg + away_xg

    winner = home if home_strength >= away_strength else away
    loser = away if winner == home else home

    draw_trap = is_draw_trap(home, away, abs_diff)
    draw_penalty = anti_draw_penalty(home, away, abs_diff)

    options: List[Dict[str, Any]] = []

    # GANADOR
    winner_conf = int(max(68, min(89, 69 + min(abs_diff * 1.7, 18))))
    winner_conf += get_adjustment_from_stats(league, "winner")
    winner_conf -= draw_penalty

    options.append({
        "pick": f"Gana {winner}",
        "pick_type": "winner",
        "confidence": int(max(60, min(92, winner_conf))),
        "winner_team": winner,
    })

    # DOBLE OPORTUNIDAD
    if diff >= 0:
        dc_pick = f"1X {home}"
    else:
        dc_pick = f"X2 {away}"

    dc_conf = 74 + min(abs_diff * 1.1, 10)
    if draw_trap:
        dc_conf += 6
    if abs_diff < 4:
        dc_conf += 4

    options.append({
        "pick": dc_pick,
        "pick_type": "double_chance",
        "confidence": int(max(68, min(91, dc_conf))),
    })

    # OVER 2.5
    if total_xg >= 2.45:
        over_conf = 69 + max(0, (total_xg - 2.25) * 12)
        over_conf += get_adjustment_from_stats(league, "over_2_5")
        if abs_diff < 5.5:
            over_conf += 1

        options.append({
            "pick": "Más de 2.5 goles",
            "pick_type": "over_2_5",
            "confidence": int(max(64, min(89, over_conf))),
        })

    # UNDER 2.5
    if total_xg <= 2.45 or draw_trap:
        under25_conf = 70 + max(0, (2.50 - total_xg) * 14)
        if draw_trap:
            under25_conf += 4

        options.append({
            "pick": "Menos de 2.5 goles",
            "pick_type": "under_2_5",
            "confidence": int(max(66, min(89, under25_conf))),
        })

    # UNDER 3.5
    under35_conf = 73
    if total_xg <= 2.90:
        under35_conf += max(0, (3.05 - total_xg) * 9)
    if draw_trap:
        under35_conf += 5

    options.append({
        "pick": "Menos de 3.5 goles",
        "pick_type": "under_3_5",
        "confidence": int(max(68, min(90, under35_conf))),
    })

    # BTTS SI
    if home_xg >= 1.0 and away_xg >= 0.9 and abs_diff < 7.5:
        btts_yes_conf = 68 + max(0, (min(home_xg, away_xg) - 0.85) * 14) + max(0, 8 - abs_diff)

        options.append({
            "pick": "Ambos marcan",
            "pick_type": "btts_yes",
            "confidence": int(max(64, min(87, btts_yes_conf))),
        })

    # BTTS NO
    if total_xg <= 2.55 or abs_diff >= 6.5 or draw_trap:
        btts_no_conf = 70
        if total_xg <= 2.35:
            btts_no_conf += 4
        if abs_diff >= 6.5:
            btts_no_conf += 3
        if draw_trap:
            btts_no_conf += 3

        options.append({
            "pick": "Ambos no marcan",
            "pick_type": "btts_no",
            "confidence": int(max(66, min(88, btts_no_conf))),
        })

    # TARJETAS DE EQUIPO
    options.append(team_cards_market(home, away, home_strength, away_strength, league))

    # Campos auxiliares para explicación
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
# EXPLICACIONES
# =========================================================

def tipster_explanation(
    option: Dict[str, Any],
    home: str,
    away: str,
    league: str,
    bookmaker_note: str,
    market_note: str,
) -> str:
    pick_type = option["pick_type"]
    winner = option.get("winner_team", home)
    total_xg = option.get("total_xg", 2.4)
    draw_trap = option.get("draw_trap", False)
    cards_team = option.get("cards_team", "")
    cards_line = option.get("cards_line", 1.5)

    if pick_type == "winner":
        base = (
            f"Me quedo con {option['pick']}. {winner} llega con mejor escenario competitivo para sacar "
            f"el partido adelante y tiene argumentos para imponer su plan en los momentos clave."
        )
        if draw_trap:
            base += " Aun así no es un cruce completamente limpio y por eso la confianza no se dispara."
        return f"{base} {market_note} {bookmaker_note}".strip()

    if pick_type == "double_chance":
        return (
            f"Veo más sólido cubrir con {option['pick']}. El partido no parece lo bastante limpio como para "
            f"ir con un ganador puro, así que esta vía protege mejor el guion más probable del encuentro. "
            f"{market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "over_2_5":
        return (
            f"Me gusta la línea de más de 2.5 goles. El cruce apunta a ritmo, tramos abiertos y opciones "
            f"reales para que el marcador supere esa barrera. {market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "under_2_5":
        return (
            f"Prefiero el menos de 2.5 goles. No veo un partido demasiado roto y el contexto invita a pensar "
            f"en un choque más contenido de lo que suele marcar la intuición inicial. {market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "under_3_5":
        return (
            f"El menos de 3.5 goles me parece una vía muy sólida. Incluso si el partido tiene momentos de ida "
            f"y vuelta, no apunta a un intercambio tan extremo como para irse a un marcador descontrolado. "
            f"{market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "btts_yes":
        return (
            f"Veo valor en el ambos marcan. Hay argumentos ofensivos en las dos partes y el cruce puede dejar "
            f"espacios suficientes como para que ambos equipos encuentren portería. {market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "btts_no":
        return (
            f"Me cuadra más el ambos no marcan. El partido tiene rasgos de control, fases cerradas o un guion "
            f"en el que una de las dos partes puede quedarse corta en producción ofensiva. {market_note} {bookmaker_note}"
        ).strip()

    if pick_type == "team_cards":
        return (
            f"Me gusta {option['pick']}. {cards_team} es el lado que más opciones tiene de entrar en faltas tácticas, "
            f"duelos exigentes y acciones de corte, así que superar la línea de {cards_line} tarjetas tiene sentido. "
            f"{market_note} {bookmaker_note}"
        ).strip()

    return f"{option['pick']} {market_note} {bookmaker_note}".strip()


# =========================================================
# SELECCIÓN DEL MEJOR MERCADO POR PARTIDO
# =========================================================

def enrich_option_with_market(match: Dict[str, Any], option: Dict[str, Any], odds_index: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Dict[str, Any]:
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
    bookmaker_note = ""
    market_note = ""

    # Solo usamos mercado real para ganador si existe.
    if option["pick_type"] == "winner" and odds_data:
        bookmaker = odds_data.get("bookmaker")
        bookmaker_market = odds_data.get("market")

        winner_team = option.get("winner_team")
        if winner_team == home:
            bookmaker_odds = odds_data.get("home")
        elif winner_team == away:
            bookmaker_odds = odds_data.get("away")

        adjusted_conf, market_note = market_read_adjustment(bookmaker_odds, option["confidence"])
        option["confidence"] = adjusted_conf

        if bookmaker_odds:
            bookmaker_note = "La cuota disponible respalda bastante bien esta lectura del partido."

    # Para otros mercados, si no hay odds reales específicas, usamos odds internas consistentes.
    if bookmaker_odds is None:
        bookmaker_odds = safe_odds_from_confidence(option["confidence"], option["pick_type"])

    value = calculate_value(option["confidence"], bookmaker_odds)
    band = confidence_band(option["confidence"])

    explanation = tipster_explanation(
        option,
        home,
        away,
        league,
        bookmaker_note,
        market_note,
    )

    enriched = {
        "id": match["id"],
        "match": match["match"],
        "league": league,
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": option["pick"],
        "pick_type": option["pick_type"],
        "confidence": int(option["confidence"]),
        "confidence_band": band,
        "odds_estimate": round(bookmaker_odds, 2) if bookmaker_odds is not None else None,
        "pick_winner": option.get("winner_team"),
        "btts": "Sí" if option["pick_type"] == "btts_yes" else ("No" if option["pick_type"] == "btts_no" else None),
        "over_2_5": "Sí" if option["pick_type"] == "over_2_5" else ("No" if option["pick_type"] == "under_2_5" else None),
        "cards": predict_cards(
            league,
            option.get("home_strength", stable_team_rating(home)),
            option.get("away_strength", stable_team_rating(away)),
            home,
            away,
        ),
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "tipster_explanation": explanation,
        "source": match.get("source", "unknown"),
        "bookmaker": bookmaker,
        "bookmaker_market": bookmaker_market,
        "model_confidence": value["model_prob"],
        "book_confidence": value["book_prob"],
        "value_edge": value["edge"],
        "has_value": value["has_value"],
        "stake": value["stake"],
        "cards_team": option.get("cards_team"),
        "cards_line": option.get("cards_line"),
    }

    return enriched


def choose_best_option_for_match(match: Dict[str, Any], odds_index: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Dict[str, Any]:
    options = build_market_options(match)
    enriched_options = [enrich_option_with_market(match, dict(o), odds_index) for o in options]

    # prioridad: value + confianza, pero si no hay value suficiente se queda con el mejor conservador
    enriched_options.sort(
        key=lambda x: (
            1 if x.get("has_value") else 0,
            x.get("confidence", 0),
            x.get("stake", 0),
        ),
        reverse=True,
    )

    return enriched_options[0]


# =========================================================
# BUILD PICKS / SIEMPRE CON PICKS
# =========================================================

def build_picks() -> List[Dict[str, Any]]:
    matches = get_real_matches()
    odds_index = fetch_live_odds_index()

    picks = [choose_best_option_for_match(m, odds_index) for m in matches]

    # Primero intentamos picks con value real.
    premium = [p for p in picks if p["confidence"] >= MIN_CONFIDENCE and p.get("has_value")]

    # Si no llegamos al mínimo, completamos con picks conservadores.
    if len(premium) < MIN_PICKS_ALWAYS:
        backup = [p for p in picks if p not in premium]
        backup.sort(
            key=lambda x: (
                x.get("confidence", 0),
                x.get("stake", 0),
            ),
            reverse=True,
        )
        need = MIN_PICKS_ALWAYS - len(premium)
        premium.extend(backup[:need])

    premium.sort(
        key=lambda x: (
            x.get("confidence", 0),
            x.get("stake", 0),
            x.get("odds_estimate", 0) or 0,
        ),
        reverse=True,
    )

    return premium[:MAX_PICKS]


# =========================================================
# COMBINADA INTELIGENTE
# =========================================================

def combo_market_priority(pick: Dict[str, Any]) -> int:
    pick_type = pick.get("pick_type")

    # prioridad a mercados más seguros para combinada
    if pick_type == "double_chance":
        return 5
    if pick_type == "under_3_5":
        return 4
    if pick_type == "team_cards":
        return 3
    if pick_type in {"btts_no", "under_2_5"}:
        return 2
    if pick_type in {"winner", "over_2_5", "btts_yes"}:
        return 1
    return 0


def build_combo(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not picks:
        return {"size": 0, "estimated_total_odds": None, "confidence": 0, "picks": []}

    ordered = sorted(
        picks,
        key=lambda x: (
            combo_market_priority(x),
            x.get("confidence", 0),
            x.get("stake", 0),
        ),
        reverse=True,
    )

    combo: List[Dict[str, Any]] = []
    used_matches = set()

    for p in ordered:
        if p["match"] in used_matches:
            continue
        combo.append(p)
        used_matches.add(p["match"])
        if len(combo) == 3:
            break

    if len(combo) < 2:
        return {"size": len(combo), "estimated_total_odds": None, "confidence": 0, "picks": combo}

    total_odds = 1.0
    valid = True
    for p in combo:
        if p.get("odds_estimate") is None:
            valid = False
            break
        total_odds *= p["odds_estimate"]

    return {
        "size": len(combo),
        "estimated_total_odds": round(total_odds, 2) if valid else None,
        "confidence": int(sum(p["confidence"] for p in combo) / len(combo)),
        "picks": combo,
    }


# =========================================================
# GROUPS
# =========================================================

def group_picks(picks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "alta": [p for p in picks if p["confidence"] >= 80],
        "media": [p for p in picks if 72 <= p["confidence"] < 80],
        "intermedia": [p for p in picks if p["confidence"] < 72],
    }


# =========================================================
# RESULT EVALUATION
# =========================================================

def evaluate_pick_result(pick: Dict[str, Any], home_goals: int, away_goals: int) -> str:
    pick_type = pick.get("pick_type")
    selected_pick = pick.get("pick", "")

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

    if pick_type == "team_cards":
        # si no tenemos tarjetas reales por equipo todavía, dejamos pendiente
        return "pending"

    return "pending"
# =========================================================
# SCORES / RESULTS
# =========================================================

def get_finished_scores_sportsdb() -> List[Dict[str, Any]]:
    results = []
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

            for ev in events:
                home = (ev.get("strHomeTeam") or "").strip()
                away = (ev.get("strAwayTeam") or "").strip()
                status = (ev.get("strStatus") or "").lower()
                home_score = ev.get("intHomeScore")
                away_score = ev.get("intAwayScore")

                if not home or not away:
                    continue
                if home_score is None or away_score is None:
                    continue

                if status and all(x not in status for x in ["match finished", "ft", "after pen", "aet"]):
                    continue

                try:
                    dt_local = parse_sportsdb_datetime(ev.get("dateEvent"), ev.get("strTime"))
                except Exception:
                    continue

                results.append({
                    "home_team": home,
                    "away_team": away,
                    "league": league_name,
                    "kickoff_iso": dt_local.isoformat(),
                    "home_goals": int(home_score),
                    "away_goals": int(away_score),
                    "score_line": f"{home_score}-{away_score}",
                })
    except Exception:
        pass

    return results


def get_finished_scores_football_data() -> List[Dict[str, Any]]:
    results = []
    try:
        start_date = (now_local() - timedelta(days=10)).date().isoformat()
        end_date = now_local().date().isoformat()

        for code, league_name in FOOTBALL_DATA_LEAGUES.items():
            data = football_data_get(
                f"/competitions/{code}/matches",
                {"dateFrom": start_date, "dateTo": end_date},
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

                try:
                    dt_local = datetime.fromisoformat(item["utcDate"].replace("Z", "+00:00")).astimezone(TZ)
                except Exception:
                    continue

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
    finished_results = get_finished_scores_football_data() + get_finished_scores_sportsdb()

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
            if pick.get("status") in ["won", "lost"]:
                continue

            matched_result = None
            for result in finished_results:
                if same_match(pick, result):
                    matched_result = result
                    break

            if not matched_result:
                continue

            new_status = evaluate_pick_result(
                pick,
                matched_result["home_goals"],
                matched_result["away_goals"],
            )

            pick["score_line"] = matched_result["score_line"]
            pick["status"] = new_status

    return refresh_history_stats(history)


# =========================================================
# HISTORY
# =========================================================

def refresh_history_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    history.setdefault("days", {})
    for _, day_data in history["days"].items():
        picks = day_data.get("picks", [])
        day_data["stats"] = {
            "won": sum(1 for p in picks if p.get("status") == "won"),
            "lost": sum(1 for p in picks if p.get("status") == "lost"),
            "pending": sum(1 for p in picks if p.get("status") == "pending"),
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
        )
        existing_index[key] = p

    for p in picks:
        key = (
            simplify_team_name(p.get("home_team")),
            simplify_team_name(p.get("away_team")),
            normalize_text(p.get("league")),
            p.get("kickoff_iso"),
        )

        if key not in existing_index:
            existing_picks.append(p)
        else:
            old = existing_index[key]
            for field in [
                "pick", "pick_type", "confidence", "confidence_band", "odds_estimate",
                "tipster_explanation", "source", "bookmaker", "bookmaker_market",
                "model_confidence", "book_confidence", "value_edge", "has_value", "stake",
                "btts", "over_2_5", "pick_winner", "cards", "cards_team", "cards_line"
            ]:
                old[field] = p.get(field, old.get(field))

    history["days"][day] = {"picks": existing_picks}
    history = refresh_history_stats(history)
    history = trim_history(history)
    return history


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
    page_items = all_picks[start:end]

    return {
        "page": page,
        "page_size": page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "items": page_items,
    }


# =========================================================
# DASHBOARD
# =========================================================

def compute_dashboard_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    won = 0
    lost = 0
    pending = 0
    total = 0
    profit = 0.0

    for _, day in history.get("days", {}).items():
        for pick in day.get("picks", []):
            status = pick.get("status")

            if status == "pending":
                pending += 1
                total += 1
                continue

            if status not in ["won", "lost"]:
                continue

            total += 1

            if status == "won":
                won += 1
            else:
                lost += 1

            stake = float(pick.get("stake", 0) or 0)
            odds = pick.get("odds_estimate")

            if stake <= 0:
                continue

            if status == "won" and odds:
                profit += (float(odds) - 1.0) * stake
            elif status == "lost":
                profit -= stake

    resolved = won + lost
    effectiveness = round((won / resolved) * 100, 1) if resolved > 0 else 0.0

    return {
        "hits": f"{won}/{resolved}" if resolved > 0 else "0/0",
        "effectiveness": effectiveness,
        "profit": round(profit, 2),
        "total_picks": total,
        "pending": pending,
    }


# =========================================================
# MODEL STATS REFRESH
# =========================================================

def refresh_model_stats_from_history(history: Dict[str, Any]) -> None:
    stats = rebuild_model_stats_from_history(history)
    save_model_stats(stats)


# =========================================================
# PAYLOAD / CACHE
# =========================================================

def build_payload() -> Dict[str, Any]:
    try:
        picks = build_picks()
    except Exception:
        picks = []

    history = read_json(HISTORY_FILE)

    if picks:
        history = merge_today_history(history, picks)

    history = update_history_finished_matches(history)
    refresh_model_stats_from_history(history)
    dashboard_stats = compute_dashboard_stats(history)

    payload = {
        "generated_at": now_local().isoformat(),
        "cache_day": today_key(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
        "combo_of_day": build_combo(picks) if picks else {},
        "groups": group_picks(picks) if picks else {"alta": [], "media": [], "intermedia": []},
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


# =========================================================
# ROUTES
# =========================================================

@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "msg": "API funcionando con picks automáticos, combinada inteligente e historial real"
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
        allsports_matches = get_allsports_matches()
        merged = get_real_matches()
        odds_index = fetch_live_odds_index()
        state = load_api_state()

        return {
            "ok": True,
            "sportsdb_count": len(sportsdb_matches),
            "football_data_count": len(football_data_matches),
            "api_football_count": len(api_football_matches),
            "allsports_count": len(allsports_matches),
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
                for m in merged[:15]
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
            "combo_of_day": {},
            "groups": {"alta": [], "media": [], "intermedia": []},
            "dashboard_stats": {"hits": "0/0", "effectiveness": 0.0, "profit": 0.0, "total_picks": 0, "pending": 0},
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