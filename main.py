import json
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
MAX_HISTORY_DAYS = 30
API_COOLDOWN_MINUTES = 10
MIN_CONFIDENCE = 69
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
    "athletic club",
    "sevilla",
    "rayo vallecano",
    "sporting",
    "huesca",
    "burgos",
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
        c for c in unicodedata.normalize("NFD", text or "")
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
        "castellon": "castellon",
        "almeria": "almeria",
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


def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]

    digest = hashlib.md5(team_name.encode("utf-8")).hexdigest()
    h = int(digest[:8], 16) % 1000
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


def parse_iso_to_local(iso_str: str) -> datetime:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = TZ.localize(dt)
    return dt.astimezone(TZ)


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
    dt_utc = datetime.strptime(
        f"{date_str} {time_str}",
        "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=pytz.UTC)
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

                    dt_local = parse_iso_to_local(date_str)
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
                {"dateFrom": start_date, "dateTo": end_date},
            )

            items = data.get("matches") or []
            for item in items:
                try:
                    utc_date = item.get("utcDate")
                    home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                    away = ((item.get("awayTeam") or {}).get("name") or "").strip()

                    if not utc_date or not home or not away:
                        continue

                    dt_local = parse_iso_to_local(utc_date)
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


def select_best_markets_from_bookmakers(bookmakers: List[Dict[str, Any]], home: str, away: str) -> Dict[str, Any]:
    best_h2h = None
    best_totals: Dict[float, Dict[str, Any]] = {}

    for book in bookmakers or []:
        title = book.get("title") or book.get("key") or "Bookmaker"
        markets = book.get("markets") or []

        for market in markets:
            key = market.get("key")
            outcomes = market.get("outcomes") or []

            if key == "h2h":
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
                    "market": "h2h",
                    "home": home_price,
                    "draw": draw_price,
                    "away": away_price,
                    "avg": avg,
                }

                if best_h2h is None or candidate["avg"] < best_h2h["avg"]:
                    best_h2h = candidate

            elif key == "totals":
                over_price = None
                under_price = None
                point = None

                for outcome in outcomes:
                    name = normalize_text(outcome.get("name"))
                    price = outcome.get("price")
                    p = outcome.get("point")

                    if price is None or p is None:
                        continue

                    if name == "over":
                        over_price = price
                        point = p
                    elif name == "under":
                        under_price = price
                        point = p

                if point is None or (over_price is None and under_price is None):
                    continue

                point = float(point)
                candidate = {
                    "bookmaker": title,
                    "market": "totals",
                    "point": point,
                    "over": over_price,
                    "under": under_price,
                    "avg": sum([x for x in [over_price, under_price] if isinstance(x, (int, float))]) / max(
                        len([x for x in [over_price, under_price] if isinstance(x, (int, float))]), 1
                    ),
                }

                prev = best_totals.get(point)
                if prev is None or candidate["avg"] < prev["avg"]:
                    best_totals[point] = candidate

    return {"h2h": best_h2h, "totals": best_totals}


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
                        "markets": "h2h,totals",
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

                markets = select_best_markets_from_bookmakers(event.get("bookmakers") or [], home, away)
                if not markets.get("h2h") and not markets.get("totals"):
                    continue

                key = (
                    simplify_team_name(home),
                    simplify_team_name(away),
                    normalize_text(league_name),
                )
                index[key] = markets

        clear_api_cooldown("odds_api")
        return index
    except Exception as e:
        set_api_cooldown("odds_api", parse_requests_error(e))
        return {}

# =========================================================
# MODEL STATS / VALUE
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


def implied_probability(odds: float) -> Optional[float]:
    if not odds or odds <= 1:
        return None
    return round(100 / odds, 1)


def calculate_value(conf: int, odds: Optional[float]) -> Dict[str, Any]:
    if not odds:
        return {
            "model_prob": None,
            "book_prob": None,
            "edge": None,
            "has_value": False,
            "stake": 0,
        }

    book = implied_probability(odds)
    if book is None:
        return {
            "model_prob": conf,
            "book_prob": None,
            "edge": None,
            "has_value": False,
            "stake": 0,
        }

    edge = round(conf - book, 1)

    stake = 0
    if edge > 8:
        stake = 5
    elif edge > 5:
        stake = 3
    elif edge > 2:
        stake = 2

    return {
        "model_prob": conf,
        "book_prob": book,
        "edge": edge,
        "has_value": edge > 2,
        "stake": stake,
    }

# =========================================================
# MATCH MERGE
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
# PICK ENGINE
# =========================================================

def anti_draw_penalty(home: str, away: str, diff: float) -> int:
    penalty = 0

    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    if abs(diff) < 6:
        penalty += 8

    if home_s in DRAW_TRAP_TEAMS:
        penalty += 4
    if away_s in DRAW_TRAP_TEAMS:
        penalty += 4

    defensive_keywords = ["atletico", "getafe", "osasuna", "mallorca", "huesca", "burgos"]
    if any(x in home_s for x in defensive_keywords):
        penalty += 2
    if any(x in away_s for x in defensive_keywords):
        penalty += 2

    return penalty


def adjust_confidence(base_conf: float, home: str, away: str, diff: float, league: str, pick_type: str) -> int:
    conf = base_conf
    conf -= anti_draw_penalty(home, away, diff)
    conf += get_adjustment_from_stats(league, pick_type)
    return int(max(65, min(round(conf), 92)))


def build_market_pick(
    match: Dict[str, Any],
    pick_type: str,
    pick_label: str,
    confidence: int,
    odds_estimate: Optional[float],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    extra = extra or {}
    value = calculate_value(confidence, odds_estimate)

    if confidence >= 80:
        band = "alta"
    elif confidence >= 72:
        band = "media"
    else:
        band = "intermedia"

    data = {
        "id": f"{match['id']}-{pick_type}",
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": pick_label,
        "pick_type": pick_type,
        "confidence": confidence,
        "confidence_band": band,
        "odds_estimate": odds_estimate,
        "status": "pending",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "model_confidence": value["model_prob"],
        "book_confidence": value["book_prob"],
        "value_edge": value["edge"],
        "has_value": value["has_value"],
        "stake": value["stake"],
        "source": match.get("source"),
    }
    data.update(extra)
    return data


def build_real_market_picks_for_match(
    match: Dict[str, Any],
    odds_index: Dict[Tuple[str, str, str], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    key = (
        simplify_team_name(match["home_team"]),
        simplify_team_name(match["away_team"]),
        normalize_text(match["league"]),
    )

    odds_pack = odds_index.get(key)
    if not odds_pack:
        return []

    out: List[Dict[str, Any]] = []
    home = match["home_team"]
    away = match["away_team"]

    home_strength = stable_team_rating(home) + 3
    away_strength = stable_team_rating(away)
    diff = home_strength - away_strength
    winner = home if diff >= 0 else away

    h2h = odds_pack.get("h2h")
    if h2h:
        home_odds = h2h.get("home")
        away_odds = h2h.get("away")
        draw_odds = h2h.get("draw")
        winner_odds = home_odds if winner == home else away_odds

        if winner_odds:
            conf = adjust_confidence(74 + min(abs(diff) * 1.3, 12), home, away, diff, match["league"], "winner")
            out.append(build_market_pick(
                match,
                "winner",
                f"Gana {winner}",
                conf,
                winner_odds,
                {
                    "pick_winner": winner,
                    "bookmaker": h2h.get("bookmaker"),
                    "market_name": "h2h",
                    "tipster_explanation": "mercado real 1X2 del bookmaker con ventaja del favorito",
                }
            ))

        if draw_odds and abs(diff) <= 5:
            out.append(build_market_pick(
                match,
                "draw",
                "Empate",
                69,
                draw_odds,
                {
                    "bookmaker": h2h.get("bookmaker"),
                    "market_name": "h2h",
                    "tipster_explanation": "mercado real 1X2 con partido equilibrado",
                }
            ))

        if draw_odds and winner_odds:
            p_win = 1 / winner_odds if winner_odds > 1 else 0
            p_draw = 1 / draw_odds if draw_odds > 1 else 0
            p_dc = p_win + p_draw
            dc_odds = round(1 / p_dc, 2) if p_dc > 0 else None

            if dc_odds and 1.10 <= dc_odds <= 1.80:
                dc_conf = min(90, adjust_confidence(76 + min(abs(diff) * 1.1, 10), home, away, diff, match["league"], "double_chance") + 6)
                out.append(build_market_pick(
                    match,
                    "double_chance",
                    f"{winner} o empate",
                    dc_conf,
                    dc_odds,
                    {
                        "pick_winner": winner,
                        "bookmaker": h2h.get("bookmaker"),
                        "market_name": "double_chance_derived_from_h2h",
                        "tipster_explanation": "doble oportunidad derivada de cuotas reales 1X2",
                    }
                ))

    totals = odds_pack.get("totals") or {}
    for point, total_data in totals.items():
        point = float(point)

        if point == 2.5:
            if total_data.get("over"):
                out.append(build_market_pick(
                    match,
                    "over_2_5",
                    "Más de 2.5 goles",
                    adjust_confidence(71, home, away, diff, match["league"], "over_2_5"),
                    total_data.get("over"),
                    {
                        "bookmaker": total_data.get("bookmaker"),
                        "market_name": "totals",
                        "total_line": point,
                        "tipster_explanation": "mercado real de goles over 2.5",
                    }
                ))
            if total_data.get("under"):
                out.append(build_market_pick(
                    match,
                    "under_2_5",
                    "Menos de 2.5 goles",
                    adjust_confidence(71, home, away, diff, match["league"], "under_2_5"),
                    total_data.get("under"),
                    {
                        "bookmaker": total_data.get("bookmaker"),
                        "market_name": "totals",
                        "total_line": point,
                        "tipster_explanation": "mercado real de goles under 2.5",
                    }
                ))

        elif point == 3.5 and total_data.get("under"):
            out.append(build_market_pick(
                match,
                "under_3_5",
                "Menos de 3.5 goles",
                adjust_confidence(75, home, away, diff, match["league"], "under_3_5"),
                total_data.get("under"),
                {
                    "bookmaker": total_data.get("bookmaker"),
                    "market_name": "totals",
                    "total_line": point,
                    "tipster_explanation": "mercado real de goles under 3.5",
                }
            ))

    out = [p for p in out if p.get("odds_estimate")]
    out.sort(key=lambda x: (x["confidence"], -(abs((x["odds_estimate"] or 2.0) - 1.90))), reverse=True)
    return out
# =========================================================
# CREATED BET / COMBI
# =========================================================

def build_created_bet(match: Dict[str, Any], odds_index: Dict[Tuple[str, str, str], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    market_picks = build_real_market_picks_for_match(match, odds_index)
    market_picks = [p for p in market_picks if p.get("confidence", 0) >= 68]

    if not market_picks:
        return None

    TARGET_MIN_ODDS = 1.55
    TARGET_MAX_ODDS = 2.50
    TARGET_CENTER = 1.95

    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for p in market_picks:
        by_type.setdefault(p["pick_type"], []).append(p)

    for k in by_type:
        by_type[k].sort(
            key=lambda x: (
                x.get("confidence", 0),
                x.get("value_edge") or -999,
                -(abs((x.get("odds_estimate", 2.0) or 2.0) - TARGET_CENTER)),
            ),
            reverse=True,
        )

    def best_of(name: str) -> Optional[Dict[str, Any]]:
        arr = by_type.get(name) or []
        return arr[0] if arr else None

    def combo_is_valid(legs: List[Dict[str, Any]]) -> bool:
        types = {x["pick_type"] for x in legs}
        invalid_pairs = [
            {"winner", "draw"},
            {"double_chance", "draw"},
            {"over_2_5", "under_2_5"},
            {"over_2_5", "under_3_5"},
        ]
        for pair in invalid_pairs:
            if pair.issubset(types):
                return False
        return True

    def combo_score(legs: List[Dict[str, Any]]) -> Tuple[float, float, float, bool]:
        combined_odds = 1.0
        conf_sum = 0.0
        value_sum = 0.0

        for leg in legs:
            combined_odds *= float(leg.get("odds_estimate", 1.0) or 1.0)
            conf_sum += float(leg.get("confidence", 0) or 0)
            value_sum += float(leg.get("value_edge", 0) or 0)

        avg_conf = conf_sum / len(legs)
        final_conf = avg_conf - ((len(legs) - 1) * 4)

        in_range = TARGET_MIN_ODDS <= combined_odds <= TARGET_MAX_ODDS
        range_bonus = 12 if in_range else max(0, 4 - abs(combined_odds - TARGET_CENTER) * 4)

        range_penalty = 0
        if combined_odds < TARGET_MIN_ODDS:
            range_penalty = (TARGET_MIN_ODDS - combined_odds) * 18
        elif combined_odds > TARGET_MAX_ODDS:
            range_penalty = (combined_odds - TARGET_MAX_ODDS) * 14

        score = (final_conf * 0.74) + (value_sum * 0.12) + range_bonus - range_penalty
        return score, final_conf, combined_odds, in_range

    candidates: List[List[Dict[str, Any]]] = []

    winner = best_of("winner")
    double_chance = best_of("double_chance")
    draw = best_of("draw")
    over_2_5 = best_of("over_2_5")
    under_2_5 = best_of("under_2_5")
    under_3_5 = best_of("under_3_5")

    if double_chance and under_3_5:
        candidates.append([double_chance, under_3_5])

    if winner and under_3_5:
        candidates.append([winner, under_3_5])

    if winner and under_2_5:
        candidates.append([winner, under_2_5])

    if over_2_5 and draw:
        candidates.append([over_2_5, draw])

    if draw and under_3_5:
        candidates.append([draw, under_3_5])

    if winner and over_2_5:
        candidates.append([winner, over_2_5])

    evaluated = []
    for legs in candidates:
        if not combo_is_valid(legs):
            continue

        score, final_conf, combined_odds, in_range = combo_score(legs)
        if final_conf < 69:
            continue

        evaluated.append({
            "legs": legs,
            "score": score,
            "confidence": int(max(68, min(round(final_conf), 90))),
            "combined_odds": round(combined_odds, 2),
            "in_range": in_range,
        })

    in_range_candidates = [x for x in evaluated if x["in_range"]]
    if in_range_candidates:
        best = sorted(
            in_range_candidates,
            key=lambda x: (x["score"], x["confidence"], -abs(x["combined_odds"] - TARGET_CENTER)),
            reverse=True,
        )[0]
    elif evaluated:
        best = sorted(
            evaluated,
            key=lambda x: (
                -min(abs(x["combined_odds"] - TARGET_MIN_ODDS), abs(x["combined_odds"] - TARGET_MAX_ODDS)),
                x["confidence"],
                x["score"],
            ),
            reverse=True,
        )[0]
    else:
        single = sorted(
            market_picks,
            key=lambda x: (
                1 if TARGET_MIN_ODDS <= float(x.get("odds_estimate", 0) or 0) <= TARGET_MAX_ODDS else 0,
                x.get("confidence", 0),
                -abs((x.get("odds_estimate", 2.0) or 2.0) - TARGET_CENTER),
            ),
            reverse=True,
        )[0]

        best = {
            "legs": [single],
            "confidence": int(single.get("confidence", 68)),
            "combined_odds": round(float(single.get("odds_estimate", 1.0) or 1.0), 2),
        }

    leg_labels = [x["pick"] for x in best["legs"]]

    if best["confidence"] >= 80:
        band = "alta"
    elif best["confidence"] >= 72:
        band = "media"
    else:
        band = "intermedia"

    return {
        "id": f"{match['id']}-created-bet",
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": " + ".join(leg_labels),
        "pick_type": "created_bet",
        "confidence": best["confidence"],
        "confidence_band": band,
        "odds_estimate": best["combined_odds"],
        "status": "pending",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "has_value": True,
        "stake": 2 if best["confidence"] >= 76 else 1,
        "legs": best["legs"],
        "tipster_explanation": "apuesta creada con mercados reales disponibles del bookmaker y cuota objetivo 1.55-2.50",
        "source": match.get("source"),
    }


def get_cached_picks_fallback() -> List[Dict[str, Any]]:
    cache = read_json(CACHE_FILE)
    picks = cache.get("picks") or []
    if picks:
        return picks[:MAX_PICKS]

    history = ensure_history_shape(read_json(HISTORY_FILE))
    today = history.get("days", {}).get(today_key(), {})
    today_picks = today.get("picks") or []
    if today_picks:
        today_picks = sorted(
            today_picks,
            key=lambda x: (x.get("confidence", 0), x.get("odds_estimate", 0)),
            reverse=True,
        )
        return today_picks[:MAX_PICKS]

    return []


def build_picks() -> List[Dict[str, Any]]:
    matches = get_real_matches()
    odds_index = fetch_live_odds_index()

    created_bets: List[Dict[str, Any]] = []

    for match in matches:
        created_bet = build_created_bet(match, odds_index)
        if created_bet and created_bet["confidence"] >= MIN_CONFIDENCE:
            created_bets.append(created_bet)

    created_bets.sort(
        key=lambda x: (
            1 if 1.55 <= float(x.get("odds_estimate", 0) or 0) <= 2.50 else 0,
            x.get("confidence", 0),
            -abs((float(x.get("odds_estimate", 2.0) or 2.0) - 1.95)),
        ),
        reverse=True
    )

    if len(created_bets) < 3:
        fallback = get_cached_picks_fallback()
        if fallback:
            return fallback[:MAX_PICKS]

    return created_bets[:MAX_PICKS]


def build_combo(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    selected = []
    seen_matches = set()

    ordered = sorted(
        picks,
        key=lambda x: (
            1 if 1.55 <= float(x.get("odds_estimate", 0) or 0) <= 2.50 else 0,
            x.get("confidence", 0),
            -abs((float(x.get("odds_estimate", 2.0) or 2.0) - 1.95)),
        ),
        reverse=True,
    )

    for pick in ordered:
        match_key = (
            simplify_team_name(pick.get("home_team", "")),
            simplify_team_name(pick.get("away_team", "")),
            normalize_text(pick.get("league", "")),
        )
        if match_key in seen_matches:
            continue

        selected.append(pick)
        seen_matches.add(match_key)

        if len(selected) == 3:
            break

    combined_odds = 1.0
    for p in selected:
        combined_odds *= float(p.get("odds_estimate", 1.0) or 1.0)

    avg_conf = int(sum(p.get("confidence", 0) for p in selected) / len(selected)) if selected else 0
    combo_conf = max(65, min(88, avg_conf - 6)) if selected else 0

    return {
        "legs": selected,
        "combined_odds": round(combined_odds, 2),
        "confidence": combo_conf,
        "label": "Combi del día 3 partidos / 3 apuestas creadas",
    }

# =========================================================
# HISTORY / RESULTS
# =========================================================

def get_pick_key(pick: Dict[str, Any]) -> str:
    try:
        date_key = parse_iso_to_local(pick["kickoff_iso"]).strftime("%Y-%m-%d %H:%M")
    except Exception:
        date_key = pick.get("kickoff_iso", "")

    return "||".join([
        simplify_team_name(pick.get("home_team", "")),
        simplify_team_name(pick.get("away_team", "")),
        normalize_text(pick.get("league", "")),
        date_key,
        normalize_text(pick.get("pick_type", "created_bet")),
        normalize_text(pick.get("pick", "")),
    ])


def ensure_history_shape(history: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(history, dict):
        history = {}
    history.setdefault("days", {})
    history.setdefault("updated_at", now_local().isoformat())
    return history


def prune_history(history: Dict[str, Any]) -> Dict[str, Any]:
    history = ensure_history_shape(history)
    days = history.get("days", {})
    keep_after = now_local().date() - timedelta(days=MAX_HISTORY_DAYS)

    filtered = {}
    for day_key, day_data in days.items():
        try:
            d = datetime.strptime(day_key, "%Y-%m-%d").date()
        except Exception:
            continue
        if d >= keep_after:
            filtered[day_key] = day_data

    history["days"] = dict(sorted(filtered.items(), reverse=True))
    return history


def merge_today_history(history: Dict[str, Any], picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    history = ensure_history_shape(history)
    day_key = today_key()

    history["days"].setdefault(day_key, {
        "date": day_key,
        "created_at": now_local().isoformat(),
        "picks": [],
    })

    existing_picks = history["days"][day_key]["picks"]
    existing_by_key = {get_pick_key(p): p for p in existing_picks}

    merged: List[Dict[str, Any]] = []
    seen = set()

    for pick in picks:
        key = get_pick_key(pick)
        seen.add(key)

        if key in existing_by_key:
            old = existing_by_key[key]
            updated = dict(old)
            updated.update(pick)
            updated["status"] = old.get("status", "pending")
            if old.get("score_line"):
                updated["score_line"] = old["score_line"]
            if old.get("result_checked_at"):
                updated["result_checked_at"] = old["result_checked_at"]
            merged.append(updated)
        else:
            merged.append(pick)

    for old in existing_picks:
        key = get_pick_key(old)
        if key not in seen:
            merged.append(old)

    merged.sort(key=lambda x: x.get("kickoff_iso", ""))
    history["days"][day_key]["picks"] = merged
    history["updated_at"] = now_local().isoformat()
    return prune_history(history)


def refresh_history_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    stats = rebuild_model_stats_from_history(history)
    save_model_stats(stats)
    history["updated_at"] = now_local().isoformat()
    return history


def get_finished_scores_football_data() -> List[Dict[str, Any]]:
    if not FOOTBALL_DATA_API_KEY or not api_is_available("football_data"):
        return []

    start_date = (now_local() - timedelta(days=3)).date().isoformat()
    end_date = now_local().date().isoformat()
    out: List[Dict[str, Any]] = []

    try:
        for code, league_name in FOOTBALL_DATA_LEAGUES.items():
            data = football_data_get(
                f"/competitions/{code}/matches",
                {"dateFrom": start_date, "dateTo": end_date},
            )

            for item in data.get("matches") or []:
                try:
                    status = (item.get("status") or "").upper()
                    if status != "FINISHED":
                        continue

                    home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                    away = ((item.get("awayTeam") or {}).get("name") or "").strip()
                    utc_date = item.get("utcDate")

                    full_time = ((item.get("score") or {}).get("fullTime") or {})
                    home_goals = full_time.get("home")
                    away_goals = full_time.get("away")

                    if home_goals is None or away_goals is None or not home or not away or not utc_date:
                        continue

                    kickoff = parse_iso_to_local(utc_date)

                    out.append({
                        "league": league_name,
                        "home_team": home,
                        "away_team": away,
                        "home_goals": int(home_goals),
                        "away_goals": int(away_goals),
                        "kickoff_iso": kickoff.isoformat(),
                        "score_line": f"{home_goals}-{away_goals}",
                    })
                except Exception:
                    continue

        clear_api_cooldown("football_data")
        return out
    except Exception as e:
        set_api_cooldown("football_data", parse_requests_error(e))
        return []


def get_finished_scores_sportsdb() -> List[Dict[str, Any]]:
    if not api_is_available("sportsdb"):
        return []

    out: List[Dict[str, Any]] = []

    try:
        for league_id, league_name in SPORTSDB_LEAGUES.items():
            for offset in range(0, 4):
                date_key = (now_local() - timedelta(days=offset)).strftime("%Y-%m-%d")
                try:
                    data = sportsdb_get(f"/eventsday.php?d={date_key}&l={league_name}")
                    events = data.get("events") or []
                except Exception:
                    continue

                for ev in events:
                    try:
                        teams = extract_home_away_sportsdb(ev)
                        home_goals = ev.get("intHomeScore")
                        away_goals = ev.get("intAwayScore")
                        if home_goals is None or away_goals is None:
                            continue

                        dt_local = parse_sportsdb_datetime(ev.get("dateEvent"), ev.get("strTime"))

                        out.append({
                            "league": league_name,
                            "home_team": teams["home"],
                            "away_team": teams["away"],
                            "home_goals": int(home_goals),
                            "away_goals": int(away_goals),
                            "kickoff_iso": dt_local.isoformat(),
                            "score_line": f"{home_goals}-{away_goals}",
                        })
                    except Exception:
                        continue

        clear_api_cooldown("sportsdb")
        return out
    except Exception as e:
        set_api_cooldown("sportsdb", parse_requests_error(e))
        return []


def same_match_pick_result(pick: Dict[str, Any], result: Dict[str, Any]) -> bool:
    if normalize_text(pick.get("league")) != normalize_text(result.get("league")):
        return False

    if simplify_team_name(pick.get("home_team", "")) != simplify_team_name(result.get("home_team", "")):
        return False

    if simplify_team_name(pick.get("away_team", "")) != simplify_team_name(result.get("away_team", "")):
        return False

    try:
        pick_date = parse_iso_to_local(pick["kickoff_iso"]).date()
        result_date = parse_iso_to_local(result["kickoff_iso"]).date()
        return pick_date == result_date
    except Exception:
        return True


def evaluate_pick_result(pick: Dict[str, Any], home_goals: int, away_goals: int) -> str:
    pick_type = pick.get("pick_type")
    total_goals = home_goals + away_goals

    if pick_type == "winner":
        predicted = simplify_team_name(pick.get("pick_winner", ""))
        real_winner = "draw"

        if home_goals > away_goals:
            real_winner = simplify_team_name(pick.get("home_team", ""))
        elif away_goals > home_goals:
            real_winner = simplify_team_name(pick.get("away_team", ""))

        return "won" if predicted == real_winner else "lost"

    if pick_type == "double_chance":
        predicted = simplify_team_name(pick.get("pick_winner", ""))
        if home_goals == away_goals:
            return "won"

        real_winner = simplify_team_name(pick.get("home_team", "")) if home_goals > away_goals else simplify_team_name(pick.get("away_team", ""))
        return "won" if predicted == real_winner else "lost"

    if pick_type == "draw":
        return "won" if home_goals == away_goals else "lost"

    if pick_type == "over_2_5":
        return "won" if total_goals >= 3 else "lost"

    if pick_type == "under_2_5":
        return "won" if total_goals <= 2 else "lost"

    if pick_type == "under_3_5":
        return "won" if total_goals <= 3 else "lost"

    if pick_type == "created_bet":
        return "pending"

    return "pending"


def update_history_finished_matches(history: Dict[str, Any]) -> Dict[str, Any]:
    results = get_finished_scores_football_data() + get_finished_scores_sportsdb()

    if not results:
        return refresh_history_stats(history)

    for day in history.get("days", {}).values():
        for pick in day.get("picks", []):
            if pick.get("status") != "pending":
                continue

            if pick.get("pick_type") == "created_bet":
                continue

            for r in results:
                if same_match_pick_result(pick, r):
                    pick["status"] = evaluate_pick_result(pick, r["home_goals"], r["away_goals"])
                    pick["score_line"] = r["score_line"]
                    pick["result_checked_at"] = now_local().isoformat()
                    break

    return refresh_history_stats(history)


def compute_dashboard_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    won = 0
    lost = 0
    pending = 0
    total = 0
    profit = 0.0

    for _, day in history.get("days", {}).items():
        for p in day.get("picks", []):
            total += 1
            status = p.get("status")

            if status == "pending":
                pending += 1
                continue

            stake = float(p.get("stake", 0) or 0)
            odds = float(p.get("odds_estimate", 1.8) or 1.8)

            if status == "won":
                won += 1
                profit += (odds - 1.0) * stake
            elif status == "lost":
                lost += 1
                profit -= stake

    resolved = won + lost

    return {
        "hits": f"{won}/{resolved}" if resolved else "0/0",
        "effectiveness": round((won / resolved) * 100, 1) if resolved else 0.0,
        "profit": round(profit, 2),
        "total_picks": total,
        "pending": pending,
    }


def group_picks(picks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    alta = [p for p in picks if p.get("confidence", 0) >= 80]
    media = [p for p in picks if 72 <= p.get("confidence", 0) < 80]
    intermedia = [p for p in picks if p.get("confidence", 0) < 72]

    return {
        "alta_confianza": alta[:6],
        "media_confianza": media[:8],
        "intermedia": intermedia[:8],
    }


def build_payload(force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        cache = read_json(CACHE_FILE)
        if cache_is_valid(cache):
            return cache

    picks = build_picks()

    history = read_json(HISTORY_FILE)
    history = ensure_history_shape(history)
    history = merge_today_history(history, picks)
    history = update_history_finished_matches(history)
    history = prune_history(history)

    stats = compute_dashboard_stats(history)

    payload = {
        "generated_at": now_local().isoformat(),
        "count": len(picks),
        "picks": picks,
        "groups": group_picks(picks),
        "combo_of_day": build_combo(picks),
        "dashboard_stats": stats,
        "history_days": len(history.get("days", {})),
    }

    write_json(HISTORY_FILE, history)
    write_json(CACHE_FILE, payload)

    return payload


def get_history_payload(page: int = 1, page_size: int = HISTORY_PAGE_SIZE) -> Dict[str, Any]:
    history = ensure_history_shape(read_json(HISTORY_FILE))
    history = update_history_finished_matches(history)
    history = prune_history(history)
    write_json(HISTORY_FILE, history)

    items = []
    for day_key, day_data in sorted(history.get("days", {}).items(), reverse=True):
        items.append({
            "date": day_key,
            "picks": day_data.get("picks", []),
            "count": len(day_data.get("picks", [])),
        })

    total = len(items)
    start = max((page - 1) * page_size, 0)
    end = start + page_size

    return {
        "page": page,
        "page_size": page_size,
        "total_days": total,
        "items": items[start:end],
        "dashboard_stats": compute_dashboard_stats(history),
    }

# =========================================================
# ROUTES
# =========================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "app": "Top Picks Pro Premium",
        "time": now_local().isoformat(),
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_local().isoformat(),
        "api_state": load_api_state(),
    }


@app.get("/picks")
def get_picks(force_refresh: bool = Query(False)):
    try:
        return build_payload(force_refresh=force_refresh)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/history")
def get_history(page: int = Query(1, ge=1), page_size: int = Query(HISTORY_PAGE_SIZE, ge=1, le=50)):
    try:
        return get_history_payload(page=page, page_size=page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/refresh")
def refresh_now():
    try:
        payload = build_payload(force_refresh=True)
        return {
            "ok": True,
            "generated_at": payload.get("generated_at"),
            "count": payload.get("count", 0),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/history/update-results")
def refresh_results_only():
    try:
        history = ensure_history_shape(read_json(HISTORY_FILE))
        history = update_history_finished_matches(history)
        history = prune_history(history)
        write_json(HISTORY_FILE, history)

        return {
            "ok": True,
            "updated_at": now_local().isoformat(),
            "dashboard_stats": compute_dashboard_stats(history),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/odds")
def debug_odds():
    try:
        odds = fetch_live_odds_index()
        sample = []
        for k, v in list(odds.items())[:20]:
            sample.append({"key": k, "value": v})
        return {"count": len(odds), "sample": sample}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/history")
def debug_history():
    return ensure_history_shape(read_json(HISTORY_FILE))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)