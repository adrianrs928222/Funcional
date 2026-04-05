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

app = FastAPI(title="Top Picks Backend", version="25.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "daily_cache.json"
HISTORY_FILE = "history_picks.json"

TARGET_SPORTS: Dict[str, Dict[str, Any]] = {
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

SPORT_KEY_ALIASES: Dict[str, List[str]] = {
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
SCORES_DAYS_FROM = 5


def log(*args: Any) -> None:
    print("[TOP-PICKS]", *args, flush=True)


def madrid_tz():
    return pytz.timezone(TZ_NAME)


def madrid_now() -> datetime:
    return datetime.now(madrid_tz())


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def normalize_text(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def implied_probability(odds: Optional[float]) -> float:
    if odds is None or odds <= 0:
        return 0.0
    return 1.0 / odds


def sport_priority(sport_key: str) -> int:
    return int(TARGET_SPORTS.get(sport_key, {}).get("priority", 10))


def bookmaker_rank(key_or_title: str) -> int:
    norm = normalize_text(key_or_title)
    for idx, name in enumerate(BOOKMAKER_PRIORITY):
        if normalize_text(name) == norm:
            return idx
    return 999


def parse_iso_dt(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def to_local_dt(iso_str: str) -> datetime:
    return parse_iso_dt(iso_str).astimezone(madrid_tz())


def iso_to_local_hhmm(iso_str: str) -> str:
    return to_local_dt(iso_str).strftime("%H:%M")


def iso_to_local_date(iso_str: str) -> str:
    return to_local_dt(iso_str).strftime("%Y-%m-%d")


def format_display_dt(value: str) -> str:
    try:
        return parse_iso_dt(value).astimezone(madrid_tz()).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return value


def daily_cache_deadline() -> datetime:
    now = madrid_now()
    tomorrow = (now + timedelta(days=1)).date()
    midnight = datetime.combine(tomorrow, datetime.min.time())
    return madrid_tz().localize(midnight) + timedelta(minutes=5)


def odds_api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{ODDS_API_BASE_URL}{path}"
    q = dict(params or {})
    q["apiKey"] = ODDS_API_KEY
    resp = requests.get(url, params=q, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log("load_json_file error", path, str(e))
        return default


def save_json_file(path: str, data: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log("save_json_file error", path, str(e))


def clear_file(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as e:
        log("clear_file error", path, str(e))


def load_cache() -> Optional[Dict[str, Any]]:
    data = load_json_file(CACHE_FILE, None)
    if not isinstance(data, dict):
        return None

    cached_until = data.get("cached_until")
    cache_day = data.get("cache_day")
    if not cached_until or not cache_day:
        return None

    try:
        until_dt = parse_iso_dt(cached_until)
    except Exception:
        return None

    now = madrid_now()
    if now.strftime("%Y-%m-%d") != cache_day:
        return None
    if now >= until_dt.astimezone(madrid_tz()):
        return None
    return data


def save_cache(data: Dict[str, Any]) -> None:
    save_json_file(CACHE_FILE, data)


def clear_cache() -> None:
    clear_file(CACHE_FILE)


def load_history() -> Dict[str, Any]:
    data = load_json_file(HISTORY_FILE, {"days": {}})
    if not isinstance(data, dict):
        return {"days": {}}
    if "days" not in data or not isinstance(data["days"], dict):
        return {"days": {}}
    return data


def save_history(data: Dict[str, Any]) -> None:
    save_json_file(HISTORY_FILE, data)


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

    return []


def fetch_scores_for_sport(sport_key: str, days_from: int = SCORES_DAYS_FROM) -> List[Dict[str, Any]]:
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
                for row in data:
                    row["_resolved_sport_key"] = sport_key
                return data
        except Exception as e:
            log("Error scores", alias, str(e))

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
                event_dt = to_local_dt(commence_time)
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
    teams = event.get("teams") or []
    if not home_team or len(teams) < 2:
        return None

    away_candidates = [t for t in teams if t != home_team]
    away_team = away_candidates[0] if away_candidates else None
    if not away_team:
        return None

    home_odds = None
    away_odds = None
    draw_odds = None

    for outcome in outcomes:
        name = outcome.get("name")
        price = safe_float(outcome.get("price"))
        if not name or price is None:
            continue

        if name == home_team:
            home_odds = price
        elif name == away_team:
            away_odds = price
        elif normalize_text(name) in {"draw", "tie", "empate"}:
            draw_odds = price

    if home_odds is None or away_odds is None:
        return None

    return {
        "bookmaker": bookmaker.get("title") or bookmaker.get("key") or "Bookmaker",
        "home_team": home_team,
        "away_team": away_team,
        "home_odds": home_odds,
        "away_odds": away_odds,
        "draw_odds": draw_odds,
    }


def make_confidence_label(conf: float) -> str:
    if conf >= 75:
        return "verde"
    if conf >= 65:
        return "amarillo"
    return "rojo"


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

    pick_type: str
    pick_name: str
    pick_odds: float
    confidence = 50.0
    source_type = "real_odds"
    market_name = ""
    market_group = ""

    if 1.28 <= favorite_odds <= 1.75 and favorite_prob >= 0.50:
        pick_type = "winner"
        pick_name = f"Gana {favorite_team}"
        pick_odds = favorite_odds
        confidence = 62 + ((favorite_prob - 0.50) * 100)
        market_name = "Ganador del partido"
        market_group = "winner"

    elif favorite_prob >= 0.57:
        pick_type = "double_chance"
        pick_name = f"1X {home_team}" if favorite_side == "home" else f"X2 {away_team}"
        synthetic_prob = clamp(favorite_prob + draw_prob, 0.58, 0.90)
        pick_odds = round(clamp(1 / synthetic_prob, 1.18, 1.60), 2)
        confidence = 66 + ((synthetic_prob - 0.58) * 100)
        market_name = "Doble oportunidad"
        market_group = "double_chance"
        source_type = "model_odds"

    elif 1.80 <= favorite_odds <= 2.35 and favorite_prob >= 0.44:
        pick_type = "draw_no_bet"
        pick_name = f"Empate no apuesta {favorite_team}"
        synthetic_prob = clamp(favorite_prob / max(0.0001, (1 - draw_prob * 0.65)), 0.48, 0.78)
        pick_odds = round(clamp(1 / synthetic_prob, 1.35, 1.95), 2)
        confidence = 58 + ((synthetic_prob - 0.48) * 100)
        market_name = "Empate no apuesta"
        market_group = "draw_no_bet"
        source_type = "model_odds"

    else:
        pick_type = "winner"
        pick_name = f"Gana {favorite_team}"
        pick_odds = favorite_odds
        confidence = 54 + ((favorite_prob - underdog_prob) * 100)
        market_name = "Ganador del partido"
        market_group = "winner"

    confidence = round(clamp(confidence, 55, 92), 1)

    sport_key = event.get("_resolved_sport_key", event.get("sport_key", ""))
    commence_time = event.get("commence_time", "")
    local_dt = to_local_dt(commence_time)

    tipster_explanation = (
        f"{favorite_team} parte con mejores probabilidades implícitas. "
        f"Local {round(home_prob * 100, 1)}%, empate {round(draw_prob * 100, 1) if draw_odds else 0}%, "
        f"visitante {round(away_prob * 100, 1)}%."
    )

    event_id = event.get("id") or f"{sport_key}-{home_team}-{away_team}-{commence_time}"

    return {
        "id": event_id,
        "sport_key": sport_key,
        "league": TARGET_SPORTS.get(sport_key, {}).get("title", sport_key),
        "competition": TARGET_SPORTS.get(sport_key, {}).get("title", sport_key),
        "priority": sport_priority(sport_key),
        "home_team": home_team,
        "away_team": away_team,
        "match": f"{home_team} vs {away_team}",
        "commence_time": commence_time,
        "time_local": local_dt.strftime("%H:%M"),
        "starts_at": local_dt.strftime("%H:%M"),
        "date_local": local_dt.strftime("%Y-%m-%d"),
        "bookmaker": parsed["bookmaker"],
        "pick_type": pick_type,
        "type": pick_type,
        "pick": pick_name,
        "odds": round(float(pick_odds), 2),
        "confidence": confidence,
        "confidence_label": make_confidence_label(confidence),
        "favorite_team": favorite_team,
        "favorite_odds": round(float(favorite_odds), 2),
        "underdog_team": underdog_team,
        "underdog_odds": round(float(underdog_odds), 2),
        "prob_home": round(home_prob * 100, 1),
        "prob_away": round(away_prob * 100, 1),
        "prob_draw": round(draw_prob * 100, 1) if draw_odds else 0.0,
        "market_name": market_name,
        "market_group": market_group,
        "source_type": source_type,
        "value_edge": round(clamp((confidence - 60) * 0.8, -10, 18), 1),
        "tipster_explanation": tipster_explanation,
        "status": "pending",
        "result_label": "Pendiente",
        "home_score": None,
        "away_score": None,
        "score_line": None,
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


def upsert_today_history(picks: List[Dict[str, Any]]) -> None:
    history = load_history()
    today = madrid_now().strftime("%Y-%m-%d")

    history["days"][today] = {
        "saved_at": madrid_now().isoformat(),
        "count": len(picks),
        "picks": picks,
    }

    save_history(history)


def extract_score_map() -> Dict[str, Dict[str, Any]]:
    score_map: Dict[str, Dict[str, Any]] = {}

    for sport_key in TARGET_SPORTS.keys():
        rows = fetch_scores_for_sport(sport_key)

        for row in rows:
            row_id = row.get("id")
            if row_id:
                score_map[row_id] = row

            teams = row.get("scores") or []
            if len(teams) == 2:
                t1 = teams[0].get("name")
                t2 = teams[1].get("name")
                if t1 and t2:
                    key = f"{normalize_text(t1)}__{normalize_text(t2)}__{row.get('commence_time','')}"
                    score_map[key] = row
                    reverse_key = f"{normalize_text(t2)}__{normalize_text(t1)}__{row.get('commence_time','')}"
                    score_map[reverse_key] = row

    return score_map


def find_score_for_pick(pick: Dict[str, Any], score_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    pick_id = pick.get("id")
    if pick_id and pick_id in score_map:
        return score_map[pick_id]

    key = (
        f"{normalize_text(pick.get('home_team',''))}__"
        f"{normalize_text(pick.get('away_team',''))}__"
        f"{pick.get('commence_time','')}"
    )
    return score_map.get(key)


def parse_score_row(score_row: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    scores = score_row.get("scores") or []
    if len(scores) != 2:
        return None

    score_values = []
    for item in scores:
        raw = item.get("score")
        if raw is None:
            return None
        try:
            score_values.append(int(raw))
        except Exception:
            return None

    if len(score_values) != 2:
        return None
    return score_values[0], score_values[1]


def score_row_completed(score_row: Dict[str, Any]) -> bool:
    completed = score_row.get("completed")
    if isinstance(completed, bool):
        return completed

    # fallback
    parsed = parse_score_row(score_row)
    return parsed is not None


def resolve_pick_status(pick: Dict[str, Any], score_row: Dict[str, Any]) -> Dict[str, Any]:
    parsed = parse_score_row(score_row)
    if not parsed:
        pick["status"] = "pending"
        pick["result_label"] = "Pendiente"
        return pick

    home_score, away_score = parsed
    home_team = pick.get("home_team", "")
    away_team = pick.get("away_team", "")

    pick["home_score"] = home_score
    pick["away_score"] = away_score
    pick["score_line"] = f"{home_score}-{away_score}"

    home_win = home_score > away_score
    away_win = away_score > home_score
    is_draw = home_score == away_score

    pick_type = str(pick.get("pick_type", "")).lower()
    pick_text = str(pick.get("pick", ""))

    status = "pending"

    if pick_type == "winner":
        if pick_text == f"Gana {home_team}":
            status = "won" if home_win else "lost"
        elif pick_text == f"Gana {away_team}":
            status = "won" if away_win else "lost"
        else:
            status = "pending"

    elif pick_type == "double_chance":
        if pick_text == f"1X {home_team}":
            status = "won" if (home_win or is_draw) else "lost"
        elif pick_text == f"X2 {away_team}":
            status = "won" if (away_win or is_draw) else "lost"
        else:
            status = "pending"

    elif pick_type == "draw_no_bet":
        team = ""
        prefix = "Empate no apuesta "
        if pick_text.startswith(prefix):
            team = pick_text[len(prefix):]

        if is_draw:
            status = "pending"
        elif team == home_team:
            status = "won" if home_win else "lost"
        elif team == away_team:
            status = "won" if away_win else "lost"
        else:
            status = "pending"

    if not score_row_completed(score_row) and status != "pending":
        status = "pending"

    pick["status"] = status
    pick["result_label"] = (
        "Acertada" if status == "won" else
        "Perdida" if status == "lost" else
        "Pendiente"
    )
    return pick


def update_history_results() -> Dict[str, Any]:
    history = load_history()
    score_map = extract_score_map()

    changed = False
    for day_key, day_data in history.get("days", {}).items():
        picks = day_data.get("picks", [])
        if not isinstance(picks, list):
            continue

        for i, pick in enumerate(picks):
            if not isinstance(pick, dict):
                continue

            score_row = find_score_for_pick(pick, score_map)
            if not score_row:
                continue

            before = (
                pick.get("status"),
                pick.get("result_label"),
                pick.get("home_score"),
                pick.get("away_score"),
                pick.get("score_line"),
            )

            picks[i] = resolve_pick_status(pick, score_row)

            after = (
                picks[i].get("status"),
                picks[i].get("result_label"),
                picks[i].get("home_score"),
                picks[i].get("away_score"),
                picks[i].get("score_line"),
            )

            if before != after:
                changed = True

        day_data["count"] = len(picks)

    if changed:
        save_history(history)

    return history


def compute_history_summary(history: Dict[str, Any]) -> Dict[str, Any]:
    total_picks = 0
    won = 0
    lost = 0
    pending = 0

    normalized_days = []

    for date_key, day in sorted(history.get("days", {}).items(), reverse=True):
        picks = day.get("picks", [])
        day_won = 0
        day_lost = 0
        day_pending = 0

        for pick in picks:
            total_picks += 1
            status = pick.get("status", "pending")
            if status == "won":
                won += 1
                day_won += 1
            elif status == "lost":
                lost += 1
                day_lost += 1
            else:
                pending += 1
                day_pending += 1

        normalized_days.append(
            {
                "date": date_key,
                "generated_at": format_display_dt(day.get("saved_at", "-")) if day.get("saved_at") else "-",
                "stats": {
                    "won": day_won,
                    "lost": day_lost,
                    "pending": day_pending,
                },
                "picks": picks,
            }
        )

    decided = won + lost
    hit_rate = round((won / decided) * 100, 1) if decided > 0 else 0.0

    return {
        "summary": {
            "total_picks": total_picks,
            "won": won,
            "lost": lost,
            "pending": pending,
            "hit_rate": hit_rate,
        },
        "days": normalized_days,
    }


def get_or_generate_daily_picks(force_refresh: bool = False) -> Dict[str, Any]:
    today = madrid_now().strftime("%Y-%m-%d")

    history = update_history_results()

    if not force_refresh:
        cached = load_cache()
        if cached:
            return cached

    picks = generate_top_picks()

    payload = {
        "cache_day": today,
        "date": today,
        "generated_at": madrid_now().isoformat(),
        "cached_until": daily_cache_deadline().isoformat(),
        "count": len(picks),
        "source": "odds_api",
        "picks": picks,
    }

    save_cache(payload)
    upsert_today_history(picks)
    update_history_results()
    return payload


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "name": "Top Picks Backend",
        "version": "25.0.0",
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
        history = update_history_results()
        return compute_history_summary(history)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando scores: {str(e)}")
    except Exception as e:
        log("api_history error:", str(e))
        raise HTTPException(status_code=500, detail=f"Error leyendo histórico: {str(e)}")


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
                    "match": f"{parsed['home_team']} vs {parsed['away_team']}",
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


@app.post("/api/cache/clear")
def api_clear_cache() -> Dict[str, Any]:
    clear_cache()
    return {"ok": True, "message": "Cache eliminada"}


@app.post("/api/history/update")
def api_history_update() -> Dict[str, Any]:
    try:
        history = update_history_results()
        return {
            "ok": True,
            "days": len(history.get("days", {})),
        }
    except Exception as e:
        log("api_history_update error:", str(e))
        raise HTTPException(status_code=500, detail=f"Error actualizando historial: {str(e)}")