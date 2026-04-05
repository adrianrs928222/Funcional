import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
ODDS_API_BASE_URL = os.getenv("ODDS_API_BASE_URL", "https://api.the-odds-api.com").rstrip("/")
TZ_NAME = os.getenv("TZ", "Europe/Madrid")

if not ODDS_API_KEY:
    raise RuntimeError("Falta ODDS_API_KEY en variables de entorno")

app = FastAPI(title="Top Picks Backend", version="24.0.0")

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
MAX_PICKS = 8


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
    except Exception as e:
        log("save_cache error:", str(e))


def clear_cache() -> None:
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception as e:
        log("clear_cache error:", str(e))


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
    except Exception as e:
        log("save_history error:", str(e))


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
                for event in data:
                    event["_resolved_sport_key"] = sport_key
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


def get_nearby_fixtures() -> List[Dict[str, Any]]:
    now = madrid_now()
    min_time = now - timedelta(hours=LOOKBACK_HOURS)
    max_time = now + timedelta(hours=LOOKAHEAD_HOURS)

    fixtures: List[Dict[str, Any]] = []

    for sport_key in TARGET_SPORTS.keys():
        events = fetch_events_for_sport(sport_key)

        for event in events:
            commence_time = event.get("commence_time")
            if not commence_time:
                continue

            try:
                event_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(
                    pytz.timezone(TZ_NAME)
                )
            except Exception:
                continue

            if min_time <= event_dt <= max_time:
                fixtures.append(event)

    fixtures.sort(
        key=lambda e: (
            -sport_priority(e.get("_resolved_sport_key", e.get("sport_key", ""))),
            e.get("commence_time", ""),
        )
    )
    return fixtures


def choose_best_bookmaker(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmakers = event.get("bookmakers") or []
    if not bookmakers:
        return None

    ranked = sorted(
        bookmakers,
        key=lambda b: bookmaker_rank(b.get("key") or b.get("title") or ""),
    )
    return ranked[0] if ranked else None


def get_h2h_market(bookmaker: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for market in bookmaker.get("markets", []):
        if market.get("key") == "h2h":
            return market
    return None


def parse_h2h_outcomes(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = choose_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_h2h_market(bookmaker)
    if not market:
        return None

    outcomes = market.get("outcomes") or []
    if not outcomes:
        return None

    home_team = event.get("home_team")
    away_team = None
    teams = event.get("teams") or []
    if teams and home_team:
        away_candidates = [t for t in teams if t != home_team]
        away_team = away_candidates[0] if away_candidates else None

    home_odds = None
    away_odds = None
    draw_odds = None

    for outcome in outcomes:
        name = outcome.get("name")
        price = safe_float(outcome.get("price"))
        if not name or not price:
            continue

        if home_team and name == home_team:
            home_odds = price
        elif away_team and name == away_team:
            away_odds = price
        elif normalize_text(name) in ["draw", "tie", "empate"]:
            draw_odds = price

    if not home_team or not away_team or not home_odds or not away_odds:
        return None

    return {
        "bookmaker": bookmaker.get("title") or bookmaker.get("key") or "Bookmaker",
        "home_team": home_team,
        "away_team": away_team,
        "home_odds": home_odds,
        "away_odds": away_odds,
        "draw_odds": draw_odds,
    }


def build_pick_from_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    parsed = parse_h2h_outcomes(event)
    if not parsed:
        return None

    home_team = parsed["home_team"]
    away_team = parsed["away_team"]
    home_odds = parsed["home_odds"]
    away_odds = parsed["away_odds"]
    draw_odds = parsed["draw_odds"]

    home_prob = implied_probability(home_odds)
    away_prob = implied_probability(away_odds)
    draw_prob = implied_probability(draw_odds) if draw_odds else 0.0

    total_prob = home_prob + away_prob + draw_prob
    if total_prob > 0:
        home_prob /= total_prob
        away_prob /= total_prob
        if draw_odds:
            draw_prob /= total_prob

    favorite_side = "home" if home_prob >= away_prob else "away"
    favorite_team = home_team if favorite_side == "home" else away_team
    favorite_odds = home_odds if favorite_side == "home" else away_odds
    underdog_team = away_team if favorite_side == "home" else home_team
    underdog_odds = away_odds if favorite_side == "home" else home_odds
    favorite_prob = max(home_prob, away_prob)
    underdog_prob = min(home_prob, away_prob)

    sport_key = event.get("_resolved_sport_key", event.get("sport_key", ""))
    commence_time = event.get("commence_time", "")

    local_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(
        pytz.timezone(TZ_NAME)
    )

    # Reglas simples para crear picks "reales" basados en cuotas
    pick_type = None
    pick_name = None
    pick_odds = None
    confidence = 50.0

    # 1) Favorito ganador
    if 1.28 <= favorite_odds <= 1.75 and favorite_prob >= 0.50:
        pick_type = "winner"
        pick_name = f"Gana {favorite_team}"
        pick_odds = favorite_odds
        confidence = 62 + ((favorite_prob - 0.50) * 100)

    # 2) Doble oportunidad virtual si hay favorito claro pero cuota no tan baja
    elif favorite_prob >= 0.57:
        pick_type = "double_chance"
        pick_name = f"1X {home_team}" if favorite_side == "home" else f"X2 {away_team}"
        synthetic_prob = clamp(favorite_prob + draw_prob, 0.58, 0.90)
        pick_odds = round(clamp(1 / synthetic_prob, 1.18, 1.60), 2)
        confidence = 66 + ((synthetic_prob - 0.58) * 100)

    # 3) Mixto / conservador
    elif 1.80 <= favorite_odds <= 2.35 and favorite_prob >= 0.44:
        pick_type = "draw_no_bet"
        pick_name = f"Empate no apuesta {favorite_team}"
        synthetic_prob = clamp(favorite_prob / max(0.0001, (1 - draw_prob * 0.65)), 0.48, 0.78)
        pick_odds = round(clamp(1 / synthetic_prob, 1.35, 1.95), 2)
        confidence = 58 + ((synthetic_prob - 0.48) * 100)

    # 4) Fallback si no entra en nada
    else:
        pick_type = "winner"
        pick_name = f"Gana {favorite_team}"
        pick_odds = favorite_odds
        confidence = 54 + ((favorite_prob - underdog_prob) * 100)

    confidence = round(clamp(confidence, 55, 92), 1)

    event_id = event.get("id") or f"{sport_key}-{home_team}-{away_team}-{commence_time}"

    return {
        "id": event_id,
        "sport_key": sport_key,
        "league": TARGET_SPORTS.get(sport_key, {}).get("title", sport_key),
        "priority": sport_priority(sport_key),
        "home_team": home_team,
        "away_team": away_team,
        "match": f"{home_team} vs {away_team}",
        "commence_time": commence_time,
        "time_local": local_dt.strftime("%H:%M"),
        "date_local": local_dt.strftime("%Y-%m-%d"),
        "bookmaker": parsed["bookmaker"],
        "pick_type": pick_type,
        "pick": pick_name,
        "odds": round(float(pick_odds), 2),
        "confidence": confidence,
        "favorite_team": favorite_team,
        "favorite_odds": round(float(favorite_odds), 2),
        "underdog_team": underdog_team,
        "underdog_odds": round(float(underdog_odds), 2),
        "prob_home": round(home_prob * 100, 1),
        "prob_away": round(away_prob * 100, 1),
        "prob_draw": round(draw_prob * 100, 1) if draw_odds else None,
    }


def dedupe_picks(picks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    result = []

    for pick in picks:
        key = (
            pick.get("home_team"),
            pick.get("away_team"),
            pick.get("pick"),
            pick.get("date_local"),
            pick.get("time_local"),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(pick)

    return result


def generate_top_picks() -> List[Dict[str, Any]]:
    fixtures = get_nearby_fixtures()
    picks: List[Dict[str, Any]] = []

    for event in fixtures:
        pick = build_pick_from_event(event)
        if pick:
            picks.append(pick)

    picks = dedupe_picks(picks)

    picks.sort(
        key=lambda p: (
            -float(p.get("confidence", 0)),
            -int(p.get("priority", 0)),
            p.get("commence_time", ""),
        )
    )

    return picks[:MAX_PICKS]


def store_today_history(picks: List[Dict[str, Any]]) -> None:
    history = load_history()
    today = madrid_now().strftime("%Y-%m-%d")

    history["days"][today] = {
        "saved_at": madrid_now().isoformat(),
        "count": len(picks),
        "picks": picks,
    }

    save_history(history)


def get_or_generate_daily_picks(force_refresh: bool = False) -> Dict[str, Any]:
    today = madrid_now().strftime("%Y-%m-%d")

    if not force_refresh:
        cached = load_cache()
        if cached:
            return cached

    picks = generate_top_picks()

    payload = {
        "cache_day": today,
        "generated_at": madrid_now().isoformat(),
        "cached_until": daily_cache_deadline().isoformat(),
        "count": len(picks),
        "picks": picks,
    }

    save_cache(payload)
    store_today_history(picks)
    return payload


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "name": "Top Picks Backend",
        "version": "24.0.0",
        "timezone": TZ_NAME,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "time": madrid_now().isoformat(),
        "has_api_key": bool(ODDS_API_KEY),
    }


@app.get("/api/picks")
def api_picks(force_refresh: bool = Query(False)) -> Dict[str, Any]:
    try:
        return get_or_generate_daily_picks(force_refresh=force_refresh)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando Odds API: {str(e)}")
    except Exception as e:
        log("api_picks error:", str(e))
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get("/api/history")
def api_history() -> Dict[str, Any]:
    try:
        return load_history()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo histórico: {str(e)}")


@app.post("/api/cache/clear")
def api_clear_cache() -> Dict[str, Any]:
    clear_cache()
    return {"ok": True, "message": "Cache eliminada"}


@app.get("/api/matches")
def api_matches() -> Dict[str, Any]:
    try:
        fixtures = get_nearby_fixtures()
        result = []

        for event in fixtures:
            parsed = parse_h2h_outcomes(event)
            if not parsed:
                continue

            sport_key = event.get("_resolved_sport_key", event.get("sport_key", ""))
            result.append(
                {
                    "id": event.get("id"),
                    "league": TARGET_SPORTS.get(sport_key, {}).get("title", sport_key),
                    "sport_key": sport_key,
                    "home_team": parsed["home_team"],
                    "away_team": parsed["away_team"],
                    "match": f'{parsed["home_team"]} vs {parsed["away_team"]}',
                    "time_local": iso_to_local_hhmm(event["commence_time"]),
                    "commence_time": event["commence_time"],
                    "bookmaker": parsed["bookmaker"],
                    "home_odds": parsed["home_odds"],
                    "draw_odds": parsed["draw_odds"],
                    "away_odds": parsed["away_odds"],
                }
            )

        return {
            "count": len(result),
            "matches": result,
        }
    except Exception as e:
        log("api_matches error:", str(e))
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")