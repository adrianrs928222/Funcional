import json
import os
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

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"
API_STATE_FILE = "api_state.json"

LOOKAHEAD_HOURS = 96
CACHE_REFRESH_MINUTES = 15
MAX_PICKS = 12
MAX_HISTORY_DAYS = 10

# cooldown si una API falla o devuelve rate limit
API_COOLDOWN_MINUTES = 10

# prioridad de uso
API_PRIORITY = ["sportsdb", "football_data", "api_football"]

# TheSportsDB
SPORTSDB_LEAGUES = {
    "4328": "LaLiga",
    "4480": "Champions League",
}

# API-Football
API_FOOTBALL_LEAGUES = {
    140: "LaLiga",
    2: "Champions League",
}

# Football-Data.org
FOOTBALL_DATA_LEAGUES = {
    "PD": "LaLiga",
    "CL": "Champions League",
}

SEASON_CANDIDATES_SPORTSDB = ["2025-2026", "2024-2025"]

TEAM_RATINGS = {
    "Real Madrid": 93,
    "Real Madrid CF": 93,
    "Barcelona": 91,
    "FC Barcelona": 91,
    "Atletico Madrid": 87,
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
    "Deportivo Alavés": 73,

    "Manchester City": 94,
    "Manchester City FC": 94,
    "Arsenal": 91,
    "Arsenal FC": 91,
    "Liverpool": 91,
    "Liverpool FC": 91,
    "Bayern Munich": 92,
    "FC Bayern München": 92,
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
}

app = FastAPI(title="Top Picks Pro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# FILE UTILS
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

# =========================================================
# API STATE / COOLDOWN
# =========================================================

def load_api_state() -> Dict[str, Any]:
    state = read_json(API_STATE_FILE)
    state.setdefault("sportsdb", {})
    state.setdefault("football_data", {})
    state.setdefault("api_football", {})
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

def parse_requests_error(e: Exception) -> str:
    text = str(e)
    if "429" in text:
        return "rate_limit"
    return text[:300]

# =========================================================
# THESPORTSDB
# =========================================================

def sportsdb_get(path: str) -> Dict[str, Any]:
    url = f"{SPORTSDB_BASE_URL}{path}"
    r = requests.get(url, timeout=12)
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
# FOOTBALL-DATA.ORG
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
# MERGE / PRIORITY / DEDUP
# =========================================================

def source_priority(source: str) -> int:
    try:
        return API_PRIORITY.index(source)
    except ValueError:
        return 999

def get_real_matches() -> List[Dict[str, Any]]:
    # consulta todas, pero con cooldown automático
    matches_by_source = {
        "sportsdb": get_sportsdb_matches(),
        "football_data": get_football_data_matches(),
        "api_football": get_api_football_matches(),
    }

    combined: List[Dict[str, Any]] = []
    for src in API_PRIORITY:
        combined.extend(matches_by_source.get(src, []))

    # deduplicación priorizando por fuente
    dedup: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    for m in combined:
        date_key = m["dt_local"].strftime("%Y-%m-%d %H:%M")
        key = (
            normalize_text(m["home_team"]),
            normalize_text(m["away_team"]),
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
# PICK MODEL
# =========================================================

def predict_cards(league: str, home_strength: float, away_strength: float, home: str, away: str) -> Dict[str, int]:
    base_cards = {
        "LaLiga": 5,
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

def estimate_odds_from_confidence(confidence: int, pick_type: str) -> float:
    if pick_type == "winner":
        base = 2.30 - (confidence - 60) * 0.024
    elif pick_type == "over_2_5":
        base = 2.42 - (confidence - 60) * 0.022
    else:
        base = 2.48 - (confidence - 60) * 0.021
    return round(max(1.42, min(base, 2.60)), 2)

def odds_band(odds: float) -> str:
    if odds <= 1.70:
        return "normal"
    if odds <= 2.05:
        return "media"
    return "alta"

def build_pick(match: Dict[str, Any]) -> Dict[str, Any]:
    home = match["home_team"]
    away = match["away_team"]
    league = match["league"]

    home_strength = stable_team_rating(home) + 3.2
    away_strength = stable_team_rating(away)

    diff = home_strength - away_strength
    abs_diff = abs(diff)

    home_xg = max(0.55, min(1.20 + diff * 0.035, 2.80))
    away_xg = max(0.40, min(1.00 - diff * 0.022, 2.30))
    total_xg = home_xg + away_xg

    winner = home if home_strength >= away_strength else away
    btts = "Sí" if home_xg >= 1.0 and away_xg >= 0.9 and abs_diff < 7.5 else "No"
    over = "Sí" if total_xg >= 2.60 else "No"

    options = []

    winner_conf = int(max(68, min(89, 69 + min(abs_diff * 1.7, 18))))
    options.append({
        "pick": f"Gana {winner}",
        "pick_type": "winner",
        "confidence": winner_conf,
    })

    if btts == "Sí":
        btts_conf = int(max(70, min(87, 68 + max(0, (min(home_xg, away_xg) - 0.85) * 14) + max(0, 8 - abs_diff))))
        options.append({
            "pick": "Ambos marcan",
            "pick_type": "btts_yes",
            "confidence": btts_conf,
        })

    if over == "Sí":
        over_conf = int(max(71, min(88, 69 + max(0, (total_xg - 2.35) * 13))))
        options.append({
            "pick": "Más de 2.5 goles",
            "pick_type": "over_2_5",
            "confidence": over_conf,
        })

    options.sort(key=lambda x: x["confidence"], reverse=True)
    best = options[0]

    odds = estimate_odds_from_confidence(best["confidence"], best["pick_type"])
    band = odds_band(odds)
    cards = predict_cards(league, home_strength, away_strength, home, away)

    explanation = (
        f"{league}: {home} vs {away}. "
        f"Ganador estimado: {winner}. "
        f"Proyección ofensiva aproximada: {home_xg:.2f} - {away_xg:.2f} xG. "
        f"BTTS: {btts}. Over 2.5: {over}. "
        f"Tarjetas previstas: {home} {cards[home]} / {away} {cards[away]}. "
        f"Pick principal: {best['pick']}."
    )

    return {
        "id": match["id"],
        "match": match["match"],
        "league": league,
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
        "kickoff_iso": match["dt_local"].isoformat(),
        "pick": best["pick"],
        "pick_type": best["pick_type"],
        "confidence": best["confidence"],
        "odds_estimate": odds,
        "odds_band": band,
        "pick_winner": winner,
        "btts": btts,
        "over_2_5": over,
        "cards": cards,
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "tipster_explanation": explanation,
        "source": match.get("source", "unknown"),
    }

def build_picks() -> List[Dict[str, Any]]:
    matches = get_real_matches()
    picks = [build_pick(m) for m in matches]
    picks = [p for p in picks if p["confidence"] >= 72]
    picks.sort(key=lambda x: (x["confidence"], x["odds_estimate"]), reverse=True)
    return picks[:MAX_PICKS]

def build_combo(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    eligible = [p for p in picks if p["confidence"] >= 80]
    combo = []
    used = set()

    for p in eligible:
        if p["match"] in used:
            continue
        combo.append(p)
        used.add(p["match"])
        if len(combo) == 3:
            break

    if len(combo) < 2:
        for p in picks:
            if p["match"] in used:
                continue
            combo.append(p)
            used.add(p["match"])
            if len(combo) == 2:
                break

    total_odds = 1.0
    for p in combo:
        total_odds *= p["odds_estimate"]

    return {
        "size": len(combo),
        "estimated_total_odds": round(total_odds, 2) if combo else 0,
        "confidence": int(sum(p["confidence"] for p in combo) / len(combo)) if combo else 0,
        "picks": combo,
    }

def group_picks(picks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "normal": [p for p in picks if p["odds_band"] == "normal"],
        "media": [p for p in picks if p["odds_band"] == "media"],
        "alta": [p for p in picks if p["odds_band"] == "alta"],
    }

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
    history["days"][today_key()] = {"picks": picks}
    history = refresh_history_stats(history)
    history = trim_history(history)
    return history

def history_to_frontend(history: Dict[str, Any]) -> Dict[str, Any]:
    days_obj = history.get("days", {})
    days_list = []
    for day, data in sorted(days_obj.items(), reverse=True):
        days_list.append({
            "date": day,
            "stats": data.get("stats", {"won": 0, "lost": 0, "pending": 0}),
            "picks": data.get("picks", []),
        })
    return {"days": days_list}

# =========================================================
# PAYLOAD / CACHE
# =========================================================

def build_payload() -> Dict[str, Any]:
    picks = build_picks()

    payload = {
        "generated_at": now_local().isoformat(),
        "cache_day": today_key(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
        "combo_of_day": build_combo(picks),
        "groups": group_picks(picks),
    }

    history = read_json(HISTORY_FILE)
    history = merge_today_history(history, picks)
    write_json(HISTORY_FILE, history)
    write_json(CACHE_FILE, payload)
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
def root():
    return {"ok": True, "msg": "API funcionando con 3 APIs reales, cooldown automático y sin fallback"}

@app.get("/test")
def test():
    return {"ok": True}

@app.get("/test-api")
def test_api():
    try:
        sportsdb_matches = get_sportsdb_matches()
        football_data_matches = get_football_data_matches()
        api_football_matches = get_api_football_matches()
        merged = get_real_matches()
        state = load_api_state()

        return {
            "ok": True,
            "sportsdb_count": len(sportsdb_matches),
            "football_data_count": len(football_data_matches),
            "api_football_count": len(api_football_matches),
            "final_count": len(merged),
            "api_state": state,
            "matches": [
                {
                    "match": m["match"],
                    "league": m["league"],
                    "time_local": m["dt_local"].strftime("%d/%m %H:%M"),
                    "source": m["source"],
                }
                for m in merged[:10]
            ],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/picks")
def picks(force_refresh: bool = Query(False)):
    try:
        return get_cached_or_refresh(force_refresh=force_refresh)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": True,
            "message": str(e),
            "count": 0,
            "picks": [],
            "combo_of_day": {},
            "groups": {"normal": [], "media": [], "alta": []},
        }

@app.get("/api/history")
def history():
    try:
        raw = read_json(HISTORY_FILE)
        raw = refresh_history_stats(raw)
        raw = trim_history(raw)
        write_json(HISTORY_FILE, raw)
        return history_to_frontend(raw)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)