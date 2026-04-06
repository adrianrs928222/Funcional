import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"

LOOKAHEAD_HOURS = 96
CACHE_REFRESH_HOURS = 6
MAX_PICKS = 12
MAX_HISTORY_DAYS = 10
MIN_REAL_MATCHES_BEFORE_FALLBACK = 4

LEAGUES = {
    "4328": "LaLiga",
    "4480": "Champions League",
}

SEASON_CANDIDATES = ["2025-2026", "2024-2025"]

TEAM_RATINGS = {
    # LaLiga
    "Real Madrid": 93,
    "Barcelona": 91,
    "Atletico Madrid": 87,
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

    # Champions / Europa top
    "Manchester City": 94,
    "Arsenal": 91,
    "Liverpool": 91,
    "Bayern Munich": 92,
    "Borussia Dortmund": 86,
    "Paris SG": 91,
    "Inter": 90,
    "Juventus": 86,
    "AC Milan": 86,
    "Napoli": 84,
    "Benfica": 84,
    "FC Porto": 83,
    "PSV Eindhoven": 85,
    "RB Leipzig": 84,
    "Barcelona": 91,
    "Real Madrid": 93,
    "Atletico Madrid": 87,
}

FALLBACK_MATCHES = [
    {"id": 700001, "league": "LaLiga", "home_team": "Girona", "away_team": "Villarreal", "hour_offset": 18},
    {"id": 700002, "league": "LaLiga", "home_team": "Valencia", "away_team": "Sevilla", "hour_offset": 22},
    {"id": 700003, "league": "LaLiga", "home_team": "Real Betis", "away_team": "Osasuna", "hour_offset": 28},
    {"id": 700004, "league": "LaLiga", "home_team": "Athletic Club", "away_team": "Getafe", "hour_offset": 34},

    {"id": 720001, "league": "Champions League", "home_team": "Manchester City", "away_team": "Bayern Munich", "hour_offset": 20},
    {"id": 720002, "league": "Champions League", "home_team": "Inter", "away_team": "Arsenal", "hour_offset": 26},
    {"id": 720003, "league": "Champions League", "home_team": "Barcelona", "away_team": "Benfica", "hour_offset": 30},
    {"id": 720004, "league": "Champions League", "home_team": "Juventus", "away_team": "PSV Eindhoven", "hour_offset": 38},
]

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

def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    cache_day = cache.get("cache_day")
    generated_at = cache.get("generated_at")

    if cache_day != today_key():
        return False

    if not generated_at:
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    age = now_local() - dt.astimezone(TZ)
    return age < timedelta(hours=CACHE_REFRESH_HOURS)

def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]
    h = abs(hash(team_name)) % 1000
    return 68 + (h / 1000) * 14

# =========================================================
# THESPORTSDB V1
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

def extract_home_away(event: Dict[str, Any]) -> Dict[str, str]:
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

def get_season_candidates(league_id: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    for season in SEASON_CANDIDATES:
        try:
            data = sportsdb_get(f"/eventsseason.php?id={league_id}&s={season}")
            season_events = data.get("events") or []
            if season_events:
                events.extend(season_events)
                break
        except Exception as e:
            print(f"ERROR season {league_id} {season}: {e}")

    return events

def get_real_matches() -> List[Dict[str, Any]]:
    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    out: List[Dict[str, Any]] = []

    for league_id, league_name in LEAGUES.items():
        try:
            events = get_season_candidates(league_id)
        except Exception as e:
            print(f"ERROR LEAGUE {league_id}: {e}")
            events = []

        for ev in events:
            try:
                teams = extract_home_away(ev)
                dt_local = parse_sportsdb_datetime(ev.get("dateEvent"), ev.get("strTime"))
            except Exception:
                continue

            if not (start <= dt_local <= end):
                continue

            out.append({
                "id": ev.get("idEvent") or f"tsdb-{league_id}-{teams['home']}-{teams['away']}",
                "match": f"{teams['home']} vs {teams['away']}",
                "league": league_name,
                "home_team": teams["home"],
                "away_team": teams["away"],
                "dt_local": dt_local,
                "source": "api_real",
            })

    out.sort(key=lambda x: x["dt_local"])
    return out[:30]

# =========================================================
# FALLBACK
# =========================================================

def get_fallback_matches() -> List[Dict[str, Any]]:
    base = now_local()
    out = []

    for item in FALLBACK_MATCHES:
        out.append({
            "id": item["id"],
            "match": f"{item['home_team']} vs {item['away_team']}",
            "league": item["league"],
            "home_team": item["home_team"],
            "away_team": item["away_team"],
            "dt_local": base + timedelta(hours=item["hour_offset"]),
            "source": "fallback_local",
        })

    out.sort(key=lambda x: x["dt_local"])
    return out

def merge_matches(real_matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(real_matches) >= MIN_REAL_MATCHES_BEFORE_FALLBACK:
        return real_matches[:MAX_PICKS]

    seen = set()
    merged = []

    for m in real_matches + get_fallback_matches():
        key = (m["league"], m["home_team"], m["away_team"])
        if key in seen:
            continue
        seen.add(key)
        merged.append(m)

    merged.sort(key=lambda x: x["dt_local"])
    return merged[:MAX_PICKS]

# =========================================================
# MODELO DE PICKS
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
        "confidence": winner_conf
    })

    if btts == "Sí":
        btts_conf = int(max(70, min(87, 68 + max(0, (min(home_xg, away_xg) - 0.85) * 14) + max(0, 8 - abs_diff))))
        options.append({
            "pick": "Ambos marcan",
            "pick_type": "btts_yes",
            "confidence": btts_conf
        })

    if over == "Sí":
        over_conf = int(max(71, min(88, 69 + max(0, (total_xg - 2.35) * 13))))
        options.append({
            "pick": "Más de 2.5 goles",
            "pick_type": "over_2_5",
            "confidence": over_conf
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
    real_matches = get_real_matches()
    matches = merge_matches(real_matches)

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
    combo = build_combo(picks)
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
    return {"ok": True, "msg": "API funcionando con TheSportsDB v1 season + fallback"}

@app.get("/test")
def test():
    return {"ok": True}

@app.get("/test-api")
def test_api():
    try:
        real_matches = get_real_matches()
        merged = merge_matches(real_matches)
        return {
            "ok": True,
            "real_count": len(real_matches),
            "final_count": len(merged),
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