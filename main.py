import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
BASE_URL = "https://api.football-data.org/v4"
TZ = pytz.timezone("Europe/Madrid")

if not API_KEY:
    raise RuntimeError("Falta FOOTBALL_DATA_API_KEY")

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"

LOOKAHEAD_HOURS = 36
CACHE_REFRESH_HOURS = 6
MAX_PICKS = 12
MAX_HISTORY_DAYS = 10
MAX_MATCHES_PER_COMP = 6
SCORE_REFRESH_DAYS_BACK = 3

COMPETITIONS: Dict[str, Dict[str, Any]] = {
    "PD": {"name": "LaLiga", "priority": 92},
    "SD": {"name": "Segunda División", "priority": 84},
    "CL": {"name": "Champions League", "priority": 100},
}

TEAM_RATINGS: Dict[str, float] = {
    # LaLiga
    "Real Madrid CF": 93,
    "FC Barcelona": 91,
    "Club Atlético de Madrid": 87,
    "Athletic Club": 84,
    "Real Sociedad de Fútbol": 82,
    "Villarreal CF": 81,
    "Real Betis Balompié": 80,
    "Girona FC": 80,
    "Valencia CF": 77,
    "Sevilla FC": 78,

    # Segunda
    "RCD Espanyol de Barcelona": 77,
    "Levante UD": 75,
    "Real Zaragoza": 73,
    "Real Sporting de Gijón": 73,
    "Real Oviedo": 74,
    "Elche CF": 75,
    "CD Tenerife": 71,
    "Cádiz CF": 75,
    "SD Eibar": 74,

    # Champions / Europa top
    "Manchester City FC": 94,
    "Arsenal FC": 91,
    "Liverpool FC": 91,
    "Chelsea FC": 84,
    "Tottenham Hotspur FC": 84,
    "FC Bayern München": 92,
    "Borussia Dortmund": 86,
    "Paris Saint-Germain FC": 91,
    "FC Internazionale Milano": 90,
    "Juventus FC": 86,
    "AC Milan": 86,
    "SSC Napoli": 84,
    "SL Benfica": 84,
    "FC Porto": 83,
    "PSV": 85,
    "AFC Ajax": 80,
    "Feyenoord Rotterdam": 84,
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
# UTILS
# =========================================================

def now_local() -> datetime:
    return datetime.now(TZ)

def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")

def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()

def parse_iso_to_local(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ)

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

def competition_priority(league_name: str) -> int:
    for meta in COMPETITIONS.values():
        if meta["name"] == league_name:
            return int(meta["priority"])
    return 0

def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    cache_day = cache.get("cache_day")
    generated_at = cache.get("generated_at")
    picks = cache.get("picks")

    if not cache_day or not generated_at or not isinstance(picks, list) or not picks:
        return False

    if cache_day != today_key():
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    return (now_local() - dt.astimezone(TZ)) < timedelta(hours=CACHE_REFRESH_HOURS)

def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    headers = {"X-Auth-Token": API_KEY}
    r = requests.get(
        f"{BASE_URL}{path}",
        headers=headers,
        params=params or {},
        timeout=10,
    )
    if not r.ok:
        raise requests.HTTPError(f"{r.status_code} {r.text[:400]}")
    return r.json()

def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]
    h = abs(hash(team_name)) % 1000
    return 68 + (h / 1000) * 14

# =========================================================
# ROOT / TEST
# =========================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "msg": "API funcionando",
        "endpoints": ["/api/picks", "/api/history", "/test", "/test-api"],
    }

@app.get("/test")
def test():
    return {"ok": True}

@app.get("/test-api")
def test_api():
    try:
        data = api_get(
            "/competitions/PD/matches",
            {
                "dateFrom": now_local().date().isoformat(),
                "dateTo": (now_local() + timedelta(days=1)).date().isoformat(),
            },
        )
        return {"ok": True, "matches": len(data.get("matches", []))}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# =========================================================
# FIXTURES REALES
# =========================================================

def fetch_matches_for_competition(code: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    data = api_get(
        f"/competitions/{code}/matches",
        {
            "dateFrom": start_date.date().isoformat(),
            "dateTo": end_date.date().isoformat(),
        },
    )
    return data.get("matches", []) or []

def build_event_from_match(match: Dict[str, Any], league_name: str, priority: int) -> Optional[Dict[str, Any]]:
    utc_date = match.get("utcDate")
    home_team = (match.get("homeTeam") or {}).get("name")
    away_team = (match.get("awayTeam") or {}).get("name")

    if not utc_date or not home_team or not away_team:
        return None

    try:
        dt_local = parse_iso_to_local(utc_date)
    except Exception:
        return None

    return {
        "id": match.get("id"),
        "league": league_name,
        "priority": priority,
        "dt_local": dt_local,
        "status": match.get("status"),
        "home_team": home_team,
        "away_team": away_team,
        "score": match.get("score") or {},
    }

def get_real_events_window() -> List[Dict[str, Any]]:
    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)

    events: List[Dict[str, Any]] = []

    for code, meta in COMPETITIONS.items():
        try:
            matches = fetch_matches_for_competition(code, start, end)
            matches = matches[:MAX_MATCHES_PER_COMP]
        except Exception as e:
            print(f"ERROR competición {code}: {e}")
            continue

        for match in matches:
            event = build_event_from_match(match, meta["name"], int(meta["priority"]))
            if not event:
                continue

            if start <= event["dt_local"] <= end:
                events.append(event)

    return events

def get_recent_events_for_scoring() -> Dict[int, Dict[str, Any]]:
    start = now_local() - timedelta(days=SCORE_REFRESH_DAYS_BACK)
    end = now_local() + timedelta(days=1)

    event_map: Dict[int, Dict[str, Any]] = {}

    for code, meta in COMPETITIONS.items():
        try:
            matches = fetch_matches_for_competition(code, start, end)
            matches = matches[:MAX_MATCHES_PER_COMP]
        except Exception:
            continue

        for match in matches:
            event = build_event_from_match(match, meta["name"], int(meta["priority"]))
            if not event or event.get("id") is None:
                continue
            event_map[int(event["id"])] = event

    return event_map

# =========================================================
# MODELO INTELIGENTE
# =========================================================

def compute_strengths(home: str, away: str, league_priority: int) -> Tuple[float, float]:
    home_rating = stable_team_rating(home)
    away_rating = stable_team_rating(away)

    home_strength = home_rating + 3.2
    away_strength = away_rating
    league_adj = max(0, league_priority - 80) * 0.02

    return home_strength + league_adj, away_strength + league_adj

def expected_goals(home_strength: float, away_strength: float) -> Tuple[float, float]:
    diff = home_strength - away_strength

    home_xg = 1.20 + (diff * 0.035)
    away_xg = 1.00 - (diff * 0.022)

    home_xg = max(0.55, min(home_xg, 2.80))
    away_xg = max(0.40, min(away_xg, 2.30))

    return round(home_xg, 2), round(away_xg, 2)

def predict_cards(league: str, home_strength: float, away_strength: float, home: str, away: str) -> Dict[str, int]:
    base_cards = {
        "LaLiga": 5,
        "Segunda División": 6,
        "Champions League": 4,
    }

    total = base_cards.get(league, 5)
    diff = home_strength - away_strength

    if diff > 0:
        away_cards = min(total - 1, max(2, round(total * 0.58)))
        home_cards = total - away_cards
    elif diff < 0:
        home_cards = min(total - 1, max(2, round(total * 0.58)))
        away_cards = total - home_cards
    else:
        home_cards = total // 2
        away_cards = total - home_cards

    return {home: int(home_cards), away: int(away_cards)}

def estimate_market_confidence(pick_type: str, hs: float, aws: float, home_xg: float, away_xg: float) -> int:
    diff = abs(hs - aws)
    total_xg = home_xg + away_xg

    if pick_type == "winner":
        conf = 69 + min(diff * 1.7, 18)
    elif pick_type == "btts_yes":
        conf = 67 + max(0, (min(home_xg, away_xg) - 0.85) * 14) + max(0, 8 - diff)
    elif pick_type == "over_2_5":
        conf = 68 + max(0, (total_xg - 2.35) * 13)
    else:
        conf = 65

    return max(60, min(int(round(conf)), 89))

def estimate_odds_from_confidence(confidence: int, pick_type: str) -> float:
    if pick_type == "winner":
        base = 2.30 - (confidence - 60) * 0.024
    elif pick_type == "over_2_5":
        base = 2.42 - (confidence - 60) * 0.022
    elif pick_type == "btts_yes":
        base = 2.48 - (confidence - 60) * 0.021
    else:
        base = 2.40 - (confidence - 60) * 0.020

    return round(max(1.42, min(base, 2.60)), 2)

def odds_band(odds: float) -> str:
    if odds <= 1.70:
        return "normal"
    if odds <= 2.05:
        return "media"
    return "alta"

def choose_main_market(home: str, away: str, hs: float, aws: float, home_xg: float, away_xg: float) -> Tuple[Dict[str, Any], str, str, str]:
    diff = abs(hs - aws)
    total_xg = home_xg + away_xg

    winner = home if hs >= aws else away
    btts = "Sí" if home_xg >= 1.05 and away_xg >= 0.92 and diff < 7.5 else "No"
    over = "Sí" if total_xg >= 2.60 else "No"

    options: List[Dict[str, Any]] = [
        {
            "pick": f"Gana {winner}",
            "pick_type": "winner",
            "confidence": estimate_market_confidence("winner", hs, aws, home_xg, away_xg),
        }
    ]

    if btts == "Sí":
        options.append({
            "pick": "Ambos marcan",
            "pick_type": "btts_yes",
            "confidence": estimate_market_confidence("btts_yes", hs, aws, home_xg, away_xg),
        })

    if over == "Sí":
        options.append({
            "pick": "Más de 2.5 goles",
            "pick_type": "over_2_5",
            "confidence": estimate_market_confidence("over_2_5", hs, aws, home_xg, away_xg),
        })

    options.sort(key=lambda x: x["confidence"], reverse=True)
    return options[0], winner, btts, over

def build_pick_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    league = event["league"]
    priority = int(event["priority"])
    home = event["home_team"]
    away = event["away_team"]
    dt_local = event["dt_local"]

    hs, aws = compute_strengths(home, away, priority)
    home_xg, away_xg = expected_goals(hs, aws)

    main_pick, winner, btts, over25 = choose_main_market(home, away, hs, aws, home_xg, away_xg)
    cards = predict_cards(league, hs, aws, home, away)

    odds_est = estimate_odds_from_confidence(main_pick["confidence"], main_pick["pick_type"])
    band = odds_band(odds_est)

    explanation = (
        f"{league}: {home} vs {away}. "
        f"Ganador estimado: {winner}. "
        f"Proyección ofensiva aproximada: {home_xg:.2f} - {away_xg:.2f} xG. "
        f"BTTS: {btts}. Over 2.5: {over25}. "
        f"Tarjetas previstas: {home} {cards[home]} / {away} {cards[away]}. "
        f"Pick principal: {main_pick['pick']}."
    )

    return {
        "id": event["id"],
        "match": f"{home} vs {away}",
        "league": league,
        "time_local": dt_local.strftime("%d/%m %H:%M"),
        "kickoff_iso": dt_local.isoformat(),
        "pick": main_pick["pick"],
        "pick_type": main_pick["pick_type"],
        "confidence": main_pick["confidence"],
        "odds_estimate": odds_est,
        "odds_band": band,
        "pick_winner": winner,
        "btts": btts,
        "over_2_5": over25,
        "cards": cards,
        "status": "pending",
        "score_line": "",
        "home_team": home,
        "away_team": away,
        "tipster_explanation": explanation,
        "prediction_source": "internal_model_real_fixtures",
    }

# =========================================================
# HISTORY / RESULTS
# =========================================================

def resolve_pick_status(pick: Dict[str, Any], score_obj: Dict[str, Any]) -> Tuple[str, str]:
    full_time = (score_obj or {}).get("fullTime") or {}
    home_goals = full_time.get("home")
    away_goals = full_time.get("away")

    if home_goals is None or away_goals is None:
        return "pending", ""

    score_line = f"{home_goals}-{away_goals}"

    if pick["pick_type"] == "winner":
        target = pick["pick"].replace("Gana ", "").strip()
        if home_goals > away_goals:
            return ("won" if target == pick["home_team"] else "lost"), score_line
        if away_goals > home_goals:
            return ("won" if target == pick["away_team"] else "lost"), score_line
        return "lost", score_line

    if pick["pick_type"] == "btts_yes":
        return ("won" if home_goals > 0 and away_goals > 0 else "lost"), score_line

    if pick["pick_type"] == "over_2_5":
        return ("won" if (home_goals + away_goals) >= 3 else "lost"), score_line

    return "pending", score_line

def refresh_scores_for_history(history: Dict[str, Any]) -> Dict[str, Any]:
    history.setdefault("days", {})
    event_map = get_recent_events_for_scoring()

    for _, day_data in history["days"].items():
        for pick in day_data.get("picks", []):
            event = event_map.get(pick.get("id"))
            if not event:
                continue

            status, score_line = resolve_pick_status(pick, event.get("score") or {})
            pick["status"] = status
            pick["score_line"] = score_line

    return history

def rebuild_history_stats(history: Dict[str, Any]) -> Dict[str, Any]:
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
    history = refresh_scores_for_history(history)
    history = rebuild_history_stats(history)
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
# PICKS / COMBO / GROUPS
# =========================================================

def select_top_picks(picks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    picks = [p for p in picks if p["confidence"] >= 72]
    picks.sort(
        key=lambda x: (
            competition_priority(x["league"]),
            x["confidence"],
            1 if x["odds_band"] == "media" else 0,
            1 if x["odds_band"] == "alta" else 0,
        ),
        reverse=True,
    )
    return picks[:MAX_PICKS]

def build_combo_of_day(picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    eligible = [p for p in picks if p["confidence"] >= 80]

    combo: List[Dict[str, Any]] = []
    used_matches = set()

    for p in eligible:
        if p["match"] in used_matches:
            continue
        combo.append(p)
        used_matches.add(p["match"])
        if len(combo) == 3:
            break

    if len(combo) < 2:
        for p in picks:
            if p["match"] in used_matches:
                continue
            combo.append(p)
            used_matches.add(p["match"])
            if len(combo) == 2:
                break

    total_odds = 1.0
    for p in combo:
        total_odds *= p["odds_estimate"]

    return {
        "size": len(combo),
        "estimated_total_odds": round(total_odds, 2) if combo else 0.0,
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
# PAYLOAD
# =========================================================

def build_payload(league: Optional[str] = None) -> Dict[str, Any]:
    events = get_real_events_window()
    picks = [build_pick_from_event(e) for e in events]

    if league:
        lf = normalize_text(league)
        picks = [p for p in picks if lf in normalize_text(p.get("league"))]

    picks = select_top_picks(picks)
    combo = build_combo_of_day(picks)
    groups = group_picks(picks)

    history = read_json(HISTORY_FILE)
    history = merge_today_history(history, picks)
    write_json(HISTORY_FILE, history)

    payload = {
        "generated_at": now_local().isoformat(),
        "cache_day": today_key(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
        "combo_of_day": combo,
        "groups": groups,
    }

    if not league:
        write_json(CACHE_FILE, payload)

    return payload

def get_cached_or_refresh(force_refresh: bool = False, league: Optional[str] = None) -> Dict[str, Any]:
    if league:
        return build_payload(league=league)

    cache = read_json(CACHE_FILE)
    if not force_refresh and cache_is_valid(cache):
        return cache

    return build_payload()

# =========================================================
# ROUTES
# =========================================================

@app.get("/api/picks")
def get_picks(
    force_refresh: bool = Query(False),
    league: Optional[str] = Query(None),
):
    try:
        payload = get_cached_or_refresh(force_refresh=force_refresh, league=league)

        picks = payload.get("picks", [])
        if league:
            lf = normalize_text(league)
            picks = [p for p in picks if lf in normalize_text(p.get("league"))]

        return {
            "generated_at": payload.get("generated_at"),
            "cache_day": payload.get("cache_day"),
            "lookahead_hours": payload.get("lookahead_hours", LOOKAHEAD_HOURS),
            "count": len(picks),
            "picks": picks,
            "combo_of_day": build_combo_of_day(picks),
            "groups": group_picks(picks),
        }

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
def get_history():
    try:
        raw = read_json(HISTORY_FILE)
        raw = refresh_scores_for_history(raw)
        raw = rebuild_history_stats(raw)
        raw = trim_history(raw)
        write_json(HISTORY_FILE, raw)
        return history_to_frontend(raw)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)