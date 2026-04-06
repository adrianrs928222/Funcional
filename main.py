import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytz
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TZ = pytz.timezone("Europe/Madrid")

# TheSportsDB free public key
SPORTSDB_API_KEY = os.getenv("SPORTSDB_API_KEY", "3").strip()
SPORTSDB_BASE_URL = f"https://www.thesportsdb.com/api/v2/json/{SPORTSDB_API_KEY}"

LOOKAHEAD_HOURS = 96
MAX_EVENTS_PER_LEAGUE = 10
MAX_PICKS = 12

LEAGUES = {
    "4328": "LaLiga",
    "4480": "Champions League",
}

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
}

FALLBACK_MATCHES = [
    {"id": 700001, "league": "LaLiga", "home_team": "Girona", "away_team": "Villarreal", "hour_offset": 18},
    {"id": 700002, "league": "LaLiga", "home_team": "Valencia", "away_team": "Sevilla", "hour_offset": 22},
    {"id": 700003, "league": "LaLiga", "home_team": "Real Betis", "away_team": "Osasuna", "hour_offset": 28},
    {"id": 700004, "league": "LaLiga", "home_team": "Athletic Club", "away_team": "Getafe", "hour_offset": 34},

    {"id": 710001, "league": "Champions League", "home_team": "Manchester City", "away_team": "Bayern Munich", "hour_offset": 20},
    {"id": 710002, "league": "Champions League", "home_team": "Inter", "away_team": "Arsenal", "hour_offset": 26},
    {"id": 710003, "league": "Champions League", "home_team": "Barcelona", "away_team": "Benfica", "hour_offset": 30},
    {"id": 710004, "league": "Champions League", "home_team": "Juventus", "away_team": "PSV Eindhoven", "hour_offset": 38},
]

def now_local() -> datetime:
    return datetime.now(TZ)

def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]
    h = abs(hash(team_name)) % 1000
    return 68 + (h / 1000) * 14

def sportsdb_get(path: str) -> Dict[str, Any]:
    url = f"{SPORTSDB_BASE_URL}{path}"
    r = requests.get(url, timeout=12)
    r.raise_for_status()
    return r.json()

def parse_sportsdb_datetime(date_str: str, time_str: str) -> datetime:
    date_str = (date_str or "").strip()
    time_str = (time_str or "00:00:00").strip()

    if not date_str:
        raise ValueError("Missing dateEvent")

    time_str = time_str.replace("Z", "")
    fmt = "%Y-%m-%d %H:%M:%S"
    dt_utc = datetime.strptime(f"{date_str} {time_str}", fmt).replace(tzinfo=pytz.UTC)
    return dt_utc.astimezone(TZ)

def extract_home_away(event: Dict[str, Any]) -> Dict[str, str]:
    home = event.get("strHomeTeam")
    away = event.get("strAwayTeam")

    if home and away:
        return {"home": home.strip(), "away": away.strip()}

    event_name = (event.get("strEvent") or "").strip()

    if " vs " in event_name:
        parts = event_name.split(" vs ", 1)
        return {"home": parts[0].strip(), "away": parts[1].strip()}

    if " - " in event_name:
        parts = event_name.split(" - ", 1)
        return {"home": parts[0].strip(), "away": parts[1].strip()}

    raise ValueError("No se pudo extraer home/away")

def get_real_matches() -> List[Dict[str, Any]]:
    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    out: List[Dict[str, Any]] = []

    for league_id, league_name in LEAGUES.items():
        try:
            data = sportsdb_get(f"/schedule/next/league/{league_id}")
            events = (data.get("events") or [])[:MAX_EVENTS_PER_LEAGUE]
        except Exception as e:
            print(f"ERROR LEAGUE {league_id}: {e}")
            continue

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
    return out

def get_fallback_matches() -> List[Dict[str, Any]]:
    base = now_local()
    out = []

    for item in FALLBACK_MATCHES:
        dt_local = base + timedelta(hours=item["hour_offset"])
        out.append({
            "id": item["id"],
            "match": f"{item['home_team']} vs {item['away_team']}",
            "league": item["league"],
            "home_team": item["home_team"],
            "away_team": item["away_team"],
            "dt_local": dt_local,
            "source": "fallback_local",
        })

    out.sort(key=lambda x: x["dt_local"])
    return out

def merge_matches(real_matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(real_matches) >= 4:
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

    return {
        home: int(home_cards),
        away: int(away_cards)
    }

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
        "league": match["league"],
        "time_local": match["dt_local"].strftime("%d/%m %H:%M"),
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

@app.get("/")
def root():
    return {"ok": True, "msg": "API funcionando con TheSportsDB + fallback"}

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
            "matches": merged[:10],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/picks")
def picks(force_refresh: bool = False):
    try:
        picks = build_picks()
        return {
            "generated_at": now_local().isoformat(),
            "cache_day": now_local().strftime("%Y-%m-%d"),
            "lookahead_hours": LOOKAHEAD_HOURS,
            "count": len(picks),
            "picks": picks,
            "combo_of_day": build_combo(picks),
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
def history():
    return {"days": []}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)