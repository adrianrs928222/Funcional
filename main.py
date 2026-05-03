import json
import os
import unicodedata
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

TZ = pytz.timezone("Europe/Madrid")

SPORTSDB_API_KEY = os.getenv("SPORTSDB_API_KEY", "123").strip()
SPORTSDB_BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{SPORTSDB_API_KEY}"

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"

CACHE_FILE = "cache.json"

LOOKAHEAD_HOURS = 48
CACHE_REFRESH_MINUTES = 10

TARGET_PICKS = 10

MIN_BUILDER_SELECTIONS = 2
MAX_BUILDER_SELECTIONS = 4

MIN_BUILDER_ODDS = 2.00
MAX_BUILDER_ODDS = 2.75

MIN_PUBLIC_CONFIDENCE = 10
MAX_PUBLIC_CONFIDENCE = 80

SPORTSDB_LEAGUES = {
    "4335": "LaLiga",
    "4400": "Segunda División",
    "4480": "Champions League",
    "4328": "Premier League",
    "4429": "Mundial",
}

API_FOOTBALL_LEAGUES = {
    140: "LaLiga",
    2: "Champions League",
    39: "Premier League",
    1: "Mundial",
}

FOOTBALL_DATA_LEAGUES = {
    "PD": "LaLiga",
    "SD": "Segunda División",
    "CL": "Champions League",
    "PL": "Premier League",
}

SEASON_CANDIDATES_SPORTSDB = ["2025-2026", "2026", "2024-2025"]

TEAM_RATINGS = {
    "Real Madrid": 93,
    "Barcelona": 91,
    "Atletico Madrid": 87,
    "Atlético Madrid": 87,
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
    "Manchester City": 94,
    "Arsenal": 91,
    "Liverpool": 91,
    "Manchester United": 84,
    "Chelsea": 84,
    "Tottenham": 84,
    "Tottenham Hotspur": 84,
    "Newcastle": 83,
    "Newcastle United": 83,
    "Aston Villa": 83,
    "Brighton": 80,
    "Brighton & Hove Albion": 80,
    "West Ham": 79,
    "West Ham United": 79,
    "Crystal Palace": 78,
    "Fulham": 77,
    "Brentford": 77,
    "Everton": 77,
    "Wolves": 76,
    "Wolverhampton": 76,
    "Bournemouth": 76,
    "Nottingham Forest": 76,
    "Leicester": 75,
    "Leicester City": 75,
    "Leeds": 75,
    "Leeds United": 75,
    "Southampton": 74,
    "Burnley": 74,
    "Sunderland": 73,
    "Bayern Munich": 92,
    "Paris Saint Germain": 91,
    "Paris SG": 91,
    "Inter": 90,
    "Juventus": 86,
    "AC Milan": 86,
    "Benfica": 84,
    "FC Porto": 83,
    "PSV Eindhoven": 85,
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
    "Italy": 87,
    "Italia": 87,
    "Uruguay": 84,
    "Belgium": 84,
    "Mexico": 80,
    "México": 80,
    "USA": 80,
}
DRAW_TRAP_TEAMS = {
    "atletico madrid",
    "getafe",
    "osasuna",
    "mallorca",
    "rayo vallecano",
    "everton",
    "wolves",
    "wolverhampton",
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
    "everton",
    "wolves",
    "wolverhampton",
    "nottingham forest",
}

app = FastAPI(title="Tipster Tips Pro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_local() -> datetime:
    return datetime.now(TZ)


def read_json(path: str) -> Any:
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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


def normalize_text(v: Optional[str]) -> str:
    return (v or "").strip().lower()


def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def simplify_team_name(name: str) -> str:
    n = strip_accents(normalize_text(name))

    replacements = {
        "rcd espanyol de barcelona": "espanyol",
        "espanyol de barcelona": "espanyol",
        "levante ud": "levante",
        "club atletico de madrid": "atletico madrid",
        "atletico de madrid": "atletico madrid",
        "real sporting de gijon": "sporting",
        "sporting gijon": "sporting",
        "brighton hove albion": "brighton",
        "tottenham hotspur": "tottenham",
        "newcastle united": "newcastle",
        "west ham united": "west ham",
        "leicester city": "leicester",
        "leeds united": "leeds",
        "wolverhampton wanderers": "wolves",
        "wolverhampton": "wolves",
        "manchester united": "man united",
        "manchester city": "man city",
        "paris saint germain": "psg",
        "paris sg": "psg",
    }

    for old, new in replacements.items():
        n = n.replace(old, new)

    remove_words = {
        "fc",
        "cf",
        "cd",
        "ud",
        "sd",
        "rcd",
        "rc",
        "club",
        "de",
        "del",
        "la",
        "el",
        "football",
        "futbol",
        "balompie",
    }

    tokens = [t for t in n.split() if t not in remove_words]

    return " ".join(tokens).strip()
def league_team_sanity_check(league: str, home: str, away: str) -> bool:
    return True


def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]

    key = simplify_team_name(team_name).encode("utf-8")
    digest = hashlib.md5(key).hexdigest()
    value = int(digest[:8], 16) % 1000

    return round(68 + (value / 1000) * 14, 2)


def public_confidence(confidence: int) -> int:
    return int(max(MIN_PUBLIC_CONFIDENCE, min(MAX_PUBLIC_CONFIDENCE, confidence)))


def parse_sportsdb_datetime(date_str: Optional[str], time_str: Optional[str]) -> datetime:
    date_str = (date_str or "").strip()
    time_str = (time_str or "00:00:00").replace("Z", "").strip()

    dt_utc = datetime.strptime(
        f"{date_str} {time_str}",
        "%Y-%m-%d %H:%M:%S",
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

    raise ValueError("No teams")


def sportsdb_get(path: str) -> Dict[str, Any]:
    r = requests.get(f"{SPORTSDB_BASE_URL}{path}", timeout=12)
    r.raise_for_status()
    return r.json()


def get_sportsdb_matches() -> List[Dict[str, Any]]:
    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)

    out = []
    seen = set()

    for league_id, league_name in SPORTSDB_LEAGUES.items():
        events = []

        for season in SEASON_CANDIDATES_SPORTSDB:
            try:
                data = sportsdb_get(f"/eventsseason.php?id={league_id}&s={season}")
                evs = data.get("events") or []

                if evs:
                    events.extend(evs)
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

            key = (
                league_name,
                simplify_team_name(teams["home"]),
                simplify_team_name(teams["away"]),
                dt_local.strftime("%Y-%m-%d"),
            )

            if key in seen:
                continue

            seen.add(key)

            out.append({
                "id": ev.get("idEvent"),
                "match": f'{teams["home"]} vs {teams["away"]}',
                "league": league_name,
                "home_team": teams["home"],
                "away_team": teams["away"],
                "dt_local": dt_local,
                "source": "sportsdb",
            })

    out.sort(key=lambda x: x["dt_local"])
    return out
def api_football_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not API_FOOTBALL_KEY:
        return {}

    headers = {"x-apisports-key": API_FOOTBALL_KEY}

    r = requests.get(
        f"{API_FOOTBALL_BASE_URL}{path}",
        headers=headers,
        params=params or {},
        timeout=15,
    )

    r.raise_for_status()
    return r.json()


def current_api_football_season() -> int:
    now = now_local()
    return now.year if now.month >= 7 else now.year - 1


def get_api_football_matches() -> List[Dict[str, Any]]:
    if not API_FOOTBALL_KEY:
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    season = current_api_football_season()

    out = []

    for league_id, league_name in API_FOOTBALL_LEAGUES.items():
        try:
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
        except Exception:
            continue

        for item in data.get("response") or []:
            try:
                fixture = item.get("fixture") or {}
                teams = item.get("teams") or {}

                home = (teams.get("home") or {}).get("name")
                away = (teams.get("away") or {}).get("name")
                date_str = fixture.get("date")

                if not home or not away or not date_str:
                    continue

                dt_local = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).astimezone(TZ)

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

    out.sort(key=lambda x: x["dt_local"])
    return out


def football_data_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not FOOTBALL_DATA_API_KEY:
        return {}

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
    if not FOOTBALL_DATA_API_KEY:
        return []

    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)

    out = []

    for code, league_name in FOOTBALL_DATA_LEAGUES.items():
        try:
            data = football_data_get(
                f"/competitions/{code}/matches",
                {
                    "dateFrom": start.date().isoformat(),
                    "dateTo": end.date().isoformat(),
                },
            )
        except Exception:
            continue

        for item in data.get("matches") or []:
            try:
                utc_date = item.get("utcDate")
                home = ((item.get("homeTeam") or {}).get("name") or "").strip()
                away = ((item.get("awayTeam") or {}).get("name") or "").strip()

                if not utc_date or not home or not away:
                    continue

                dt_local = datetime.fromisoformat(
                    utc_date.replace("Z", "+00:00")
                ).astimezone(TZ)

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

    out.sort(key=lambda x: x["dt_local"])
    return out


def get_real_matches() -> List[Dict[str, Any]]:
    matches = []

    matches.extend(get_api_football_matches())
    matches.extend(get_football_data_matches())
    matches.extend(get_sportsdb_matches())

    today = now_local().date()
    tomorrow = today + timedelta(days=1)

    matches = [
        m for m in matches
        if m["dt_local"].date() in {today, tomorrow}
    ]

    dedup = {}
    final = []

    for m in matches:
        key = (
            simplify_team_name(m["home_team"]),
            simplify_team_name(m["away_team"]),
            normalize_text(m["league"]),
            m["dt_local"].strftime("%Y-%m-%d"),
        )

        if key in dedup:
            continue

        dedup[key] = True
        final.append(m)

    final.sort(key=lambda x: x["dt_local"])
    return final
def market_reliability_bonus(pick_type: str) -> int:
    if pick_type == "double_chance":
        return 10

    if pick_type == "goals_interval":
        return 10

    if pick_type == "under_3_5":
        return 9

    if pick_type == "btts_no":
        return 5

    if pick_type == "team_cards_1_5":
        return 7

    if pick_type == "both_teams_card_1_plus":
        return 7

    if pick_type == "over_2_5":
        return 3

    if pick_type == "btts_yes":
        return 2

    return 0


def safe_odds_from_confidence(confidence: int, market_type: str) -> float:
    if market_type == "double_chance":
        return round(min(max(1.22, 1.25 + (100 - confidence) * 0.005), 1.70), 2)

    if market_type == "goals_interval":
        return round(min(max(1.35, 1.45 + (100 - confidence) * 0.005), 1.90), 2)

    if market_type == "under_3_5":
        return round(min(max(1.30, 1.34 + (100 - confidence) * 0.005), 1.85), 2)

    if market_type == "over_2_5":
        return round(min(max(1.55, 1.60 + (100 - confidence) * 0.006), 2.20), 2)

    if market_type == "btts_no":
        return round(min(max(1.52, 1.58 + (100 - confidence) * 0.006), 2.15), 2)

    if market_type == "btts_yes":
        return round(min(max(1.60, 1.65 + (100 - confidence) * 0.006), 2.25), 2)

    return round(min(max(1.45, 1.55 + (100 - confidence) * 0.006), 2.50), 2)


def is_draw_trap(home: str, away: str, abs_diff: float) -> bool:
    home_s = simplify_team_name(home)
    away_s = simplify_team_name(away)

    if abs_diff < 4.5:
        return True

    if home_s in DRAW_TRAP_TEAMS or away_s in DRAW_TRAP_TEAMS:
        return True

    return False


def team_specific_cards_market(
    team: str,
    opponent: str,
    team_strength: float,
    opponent_strength: float,
    league: str,
    is_home: bool,
) -> Dict[str, Any]:
    team_s = simplify_team_name(team)
    opponent_s = simplify_team_name(opponent)

    diff = opponent_strength - team_strength
    conf = 68

    if league in {"LaLiga", "Segunda División"}:
        conf += 5
    elif league == "Premier League":
        conf += 3
    elif league in {"Champions League", "Mundial"}:
        conf -= 1

    if team_s in AGGRESSIVE_CARD_TEAMS:
        conf += 7

    if opponent_s in {
        "real madrid",
        "barcelona",
        "man city",
        "arsenal",
        "liverpool",
        "psg",
        "bayern munich",
    }:
        conf += 3

    if diff >= 4:
        conf += 4

    if not is_home:
        conf += 1

    return {
        "pick": f"{team} +1.5 tarjetas",
        "pick_type": "team_cards_1_5",
        "confidence": int(max(58, min(86, conf))),
        "trackable": False,
    }


def both_teams_cards_market(league: str, draw_trap: bool) -> List[Dict[str, Any]]:
    base = 70

    if league in {"LaLiga", "Segunda División"}:
        base += 6
    elif league == "Premier League":
        base += 3
    elif league in {"Champions League", "Mundial"}:
        base -= 2

    if draw_trap:
        base += 3

    return [
        {
            "pick": "Ambos equipos reciben 1+ tarjeta",
            "pick_type": "both_teams_card_1_plus",
            "confidence": int(max(62, min(88, base))),
            "trackable": False,
        },
        {
            "pick": "Ambos equipos reciben 2+ tarjetas",
            "pick_type": "both_teams_card_2_plus",
            "confidence": int(max(54, min(82, base - 8))),
            "trackable": False,
        },
    ]
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

    if is_world_cup:
        home_xg *= 0.92
        away_xg *= 0.92

    total_xg = home_xg + away_xg

    if total_xg <= 2.15:
        goals_interval_pick = "0-2 goles"
        goals_interval_conf = 76
    elif total_xg <= 2.65:
        goals_interval_pick = "1-3 goles"
        goals_interval_conf = 78
    elif total_xg <= 3.10:
        goals_interval_pick = "1-4 goles"
        goals_interval_conf = 77
    elif total_xg <= 3.55:
        goals_interval_pick = "2-4 goles"
        goals_interval_conf = 75
    else:
        goals_interval_pick = "2-5 goles"
        goals_interval_conf = 74

    winner = home if home_strength >= away_strength else away
    draw_trap = is_draw_trap(home, away, abs_diff)

    options: List[Dict[str, Any]] = []

    options.append({
        "pick": goals_interval_pick,
        "pick_type": "goals_interval",
        "confidence": int(max(68, min(86, goals_interval_conf))),
        "trackable": True,
    })

    winner_conf = 66 + min(abs_diff * 1.5, 16)

    if draw_trap:
        winner_conf -= 8

    options.append({
        "pick": f"Gana {winner}",
        "pick_type": "winner",
        "confidence": int(max(58, min(84, winner_conf))),
        "winner_team": winner,
        "trackable": True,
    })

    dc_pick = f"1X {home}" if diff >= 0 else f"X2 {away}"
    dc_conf = 74 + min(abs_diff * 1.1, 10)

    if draw_trap:
        dc_conf += 6

    if abs_diff < 4:
        dc_conf += 4

    options.append({
        "pick": dc_pick,
        "pick_type": "double_chance",
        "confidence": int(max(68, min(90, dc_conf))),
        "trackable": True,
    })

    over_conf = 63

    if total_xg >= 2.45:
        over_conf += max(0, (total_xg - 2.25) * 13)

    if home_xg >= 1.2 and away_xg >= 0.9:
        over_conf += 5

    if abs_diff < 6:
        over_conf += 2

    options.append({
        "pick": "Más de 2.5 goles",
        "pick_type": "over_2_5",
        "confidence": int(max(56, min(86, over_conf))),
        "trackable": True,
    })

    under35_conf = 73

    if total_xg <= 2.90:
        under35_conf += max(0, (3.05 - total_xg) * 9)

    if draw_trap:
        under35_conf += 5

    if is_world_cup:
        under35_conf += 3

    options.append({
        "pick": "Menos de 3.5 goles",
        "pick_type": "under_3_5",
        "confidence": int(max(68, min(90, under35_conf))),
        "trackable": True,
    })

    btts_yes_conf = 63

    if home_xg >= 1.0:
        btts_yes_conf += 4

    if away_xg >= 0.9:
        btts_yes_conf += 5

    if abs_diff < 7:
        btts_yes_conf += 3

    if is_world_cup:
        btts_yes_conf -= 2

    options.append({
        "pick": "Ambos marcan: Sí",
        "pick_type": "btts_yes",
        "confidence": int(max(56, min(84, btts_yes_conf))),
        "trackable": True,
    })

    btts_no_conf = 70

    if total_xg <= 2.35:
        btts_no_conf += 4

    if abs_diff >= 6:
        btts_no_conf += 3

    if draw_trap:
        btts_no_conf += 3

    if is_world_cup:
        btts_no_conf += 2

    options.append({
        "pick": "Ambos marcan: No",
        "pick_type": "btts_no",
        "confidence": int(max(60, min(88, btts_no_conf))),
        "trackable": True,
    })

    options.append(
        team_specific_cards_market(
            team=home,
            opponent=away,
            team_strength=home_strength,
            opponent_strength=away_strength,
            league=league,
            is_home=True,
        )
    )

    options.append(
        team_specific_cards_market(
            team=away,
            opponent=home,
            team_strength=away_strength,
            opponent_strength=home_strength,
            league=league,
            is_home=False,
        )
    )

    options.extend(
        both_teams_cards_market(
            league=league,
            draw_trap=draw_trap,
        )
    )

    for option in options:
        option["score"] = option.get("confidence", 0) + market_reliability_bonus(option.get("pick_type", ""))

    return options
def enrich_option(match: Dict[str, Any], option: Dict[str, Any]) -> Dict[str, Any]:
    odds = safe_odds_from_confidence(option["confidence"], option["pick_type"])
    confidence = public_confidence(option["confidence"])

    return {
        "id": match["id"],
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": option["pick"],
        "pick_type": option["pick_type"],
        "confidence": confidence,
        "confidence_band": "alta",
        "tier": "premium",
        "score": option.get("score", confidence),
        "odds_estimate": odds,
        "odds_source": "synthetic",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "status": "pending",
        "tipster_explanation": "",
        "trackable": bool(option.get("trackable", False)),
    }


def compatible_with_builder(existing: List[Dict[str, Any]], candidate: Dict[str, Any]) -> bool:
    existing_types = {x.get("pick_type") for x in existing}
    ctype = candidate.get("pick_type")

    incompatible = [
        ("btts_yes", "btts_no"),
        ("over_2_5", "under_3_5"),
        ("winner", "double_chance"),
    ]

    for a, b in incompatible:
        if ctype == a and b in existing_types:
            return False
        if ctype == b and a in existing_types:
            return False

    if ctype in existing_types:
        return False

    return True


def builder_total_odds(builder: List[Dict[str, Any]]) -> float:
    total = 1.0

    for leg in builder:
        total *= float(leg.get("odds_estimate") or 1)

    return round(total, 4)


def build_bet_builder_for_match(match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    options = build_market_options(match)
    enriched = [enrich_option(match, dict(option)) for option in options]

    enriched.sort(
        key=lambda x: (
            x.get("confidence", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    base_candidates = [
        x for x in enriched
        if x.get("pick_type") in {"double_chance", "winner"}
        and x.get("confidence", 0) >= 68
    ]

    base_candidates.sort(
        key=lambda x: (
            1 if x.get("pick_type") == "double_chance" else 0,
            x.get("confidence", 0),
            x.get("score", 0),
        ),
        reverse=True,
    )

    if not base_candidates:
        return None

    builder = [base_candidates[0]]

    interval_candidates = [
        x for x in enriched
        if x.get("pick_type") == "goals_interval"
        and compatible_with_builder(builder, x)
    ]

    if interval_candidates:
        builder.append(interval_candidates[0])

    preferred_order = ["btts_no", "btts_yes", "best_cards_market"]

    for wanted_type in preferred_order:
        if len(builder) >= MAX_BUILDER_SELECTIONS:
            break

        if wanted_type == "best_cards_market":
            card_candidates = [
                x for x in enriched
                if x.get("pick_type") in {"team_cards_1_5", "both_teams_card_1_plus"}
                and compatible_with_builder(builder, x)
                and x.get("confidence", 0) >= 76
            ]

            card_candidates.sort(
                key=lambda x: (
                    x.get("confidence", 0),
                    x.get("score", 0),
                ),
                reverse=True,
            )

            if card_candidates:
                test_builder = builder + [card_candidates[0]]

                if builder_total_odds(test_builder) <= MAX_BUILDER_ODDS:
                    builder.append(card_candidates[0])

            continue

        candidates = [
            x for x in enriched
            if x.get("pick_type") == wanted_type
            and compatible_with_builder(builder, x)
            and x.get("confidence", 0) >= 72
        ]

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

        if builder_total_odds(test_builder) <= MAX_BUILDER_ODDS:
            builder.append(candidates[0])

    if len(builder) < MIN_BUILDER_SELECTIONS:
        return None

    total_odds = builder_total_odds(builder)

    if total_odds < MIN_BUILDER_ODDS or total_odds > MAX_BUILDER_ODDS:
        return None

    confidence = int(sum(x.get("confidence", 0) for x in builder) / len(builder))
    confidence = public_confidence(confidence)

    selections = [x.get("pick", "--") for x in builder]

    return {
        "id": match["id"],
        "match": match["match"],
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick_type": "bet_builder",
        "pick": "Crear apuesta: " + " + ".join(selections),
        "selections": selections,
        "legs": builder,
        "confidence": confidence,
        "confidence_band": "alta",
        "tier": "premium",
        "score": sum(x.get("score", 0) for x in builder),
        "odds_estimate": round(total_odds, 2),
        "odds_source": "synthetic",
        "home_team": match["home_team"],
        "away_team": match["away_team"],
        "status": "pending",
        "tipster_explanation": "",
        "trackable": False,
    }


def build_picks() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    matches = get_real_matches()
    candidates = []

    for match in matches:
        builder = build_bet_builder_for_match(match)

        if not builder:
            continue

        odds = float(builder.get("odds_estimate") or 999)

        if MIN_BUILDER_ODDS <= odds <= MAX_BUILDER_ODDS and builder.get("confidence", 0) >= 68:
            candidates.append(builder)

    candidates.sort(
        key=lambda x: (
            x.get("confidence", 0),
            -abs(float(x.get("odds_estimate", 0) or 0) - 2.25),
            x.get("score", 0),
        ),
        reverse=True,
    )

    return candidates[:TARGET_PICKS], []


def build_combo(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not picks:
        return {
            "size": 0,
            "estimated_total_odds": None,
            "confidence": 0,
            "picks": [],
        }

    today = now_local().date()

    sorted_picks = sorted(
        picks,
        key=lambda p: (
            0 if datetime.fromisoformat(p["kickoff_iso"]).astimezone(TZ).date() == today else 1,
            -p.get("confidence", 0),
            abs(float(p.get("odds_estimate", 0) or 0) - 2.10),
        ),
    )

    combo = []
    used_matches = set()

    for pick in sorted_picks:
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

    for pick in combo:
        total_odds *= float(pick.get("odds_estimate") or 1)

    confidence = int(sum(p.get("confidence", 0) for p in combo) / len(combo))

    return {
        "size": 3,
        "estimated_total_odds": round(total_odds, 2),
        "confidence": public_confidence(confidence),
        "picks": combo,
    }


def get_premium_single_pick(picks: List[Dict[str, Any]], combo: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
            -abs(float(x.get("odds_estimate", 0) or 0) - 2.25),
            x.get("score", 0),
        ),
        reverse=True,
    )

    return candidates[0]


def group_picks(picks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "premium": picks,
        "strong": [],
        "medium": [],
        "risky": [],
    }


def build_payload() -> Dict[str, Any]:
    try:
        picks, match_catalog = build_picks()
    except Exception:
        picks, match_catalog = [], []

    combo = build_combo(picks)
    premium_single = get_premium_single_pick(picks, combo)

    payload = {
        "generated_at": now_local().isoformat(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
        "combo_of_day": combo,
        "premium_single": premium_single,
        "groups": group_picks(picks),
        "match_catalog": [],
        "dashboard_stats": {
            "hits": "0/0",
            "effectiveness": 0,
            "profit": 0,
            "pending": len(picks),
            "total_picks": len(picks),
        },
    }

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
        "name": "Tipster Tips Pro",
        "mode": "premium-predictive",
    }


@app.get("/test")
def test() -> Dict[str, Any]:
    return {
        "ok": True,
        "status": "backend online",
    }


@app.get("/test-api")
def test_api() -> Dict[str, Any]:
    matches = get_real_matches()

    return {
        "ok": True,
        "count": len(matches),
        "matches": [
            {
                "match": m["match"],
                "league": m["league"],
                "time_local": m["dt_local"].strftime("%d/%m %H:%M"),
                "source": m["source"],
            }
            for m in matches[:30]
        ],
    }


@app.get("/api/picks")
def api_picks(force_refresh: bool = Query(False)) -> Dict[str, Any]:
    return get_cached_or_refresh(force_refresh=force_refresh)


@app.get("/api/odds")
def api_odds() -> Dict[str, Any]:
    return {
        "count": 0,
        "items": [],
        "mode": "synthetic-premium",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=10000,
        reload=True,
    )