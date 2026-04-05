import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytz
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
BASE_URL = "https://api.football-data.org/v4"
TZ = pytz.timezone("Europe/Madrid")

if not API_KEY:
    raise RuntimeError("Falta FOOTBALL_DATA_API_KEY")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LOOKAHEAD_HOURS = 48
MAX_MATCHES_PER_COMP = 6
MAX_PICKS = 10

COMPETITIONS = {
    "PD": "LaLiga",
    "SD": "Segunda División",
    "CL": "Champions League",
}

TEAM_RATINGS = {
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

    "RCD Espanyol de Barcelona": 77,
    "Levante UD": 75,
    "Real Zaragoza": 73,
    "Real Sporting de Gijón": 73,
    "Real Oviedo": 74,
    "Elche CF": 75,
    "CD Tenerife": 71,
    "Cádiz CF": 75,
    "SD Eibar": 74,

    "Manchester City FC": 94,
    "Arsenal FC": 91,
    "Liverpool FC": 91,
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
}

def now_local() -> datetime:
    return datetime.now(TZ)

def parse_iso_to_local(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ)

def api_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"X-Auth-Token": API_KEY}
    r = requests.get(f"{BASE_URL}{path}", headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def stable_team_rating(team_name: str) -> float:
    if team_name in TEAM_RATINGS:
        return TEAM_RATINGS[team_name]
    h = abs(hash(team_name)) % 1000
    return 68 + (h / 1000) * 14

def get_matches() -> List[Dict[str, Any]]:
    start = now_local()
    end = now_local() + timedelta(hours=LOOKAHEAD_HOURS)
    out: List[Dict[str, Any]] = []

    for code, league_name in COMPETITIONS.items():
        try:
            data = api_get(
                f"/competitions/{code}/matches",
                {
                    "dateFrom": start.date().isoformat(),
                    "dateTo": end.date().isoformat(),
                },
            )
            matches = (data.get("matches") or [])[:MAX_MATCHES_PER_COMP]
        except Exception as e:
            print(f"ERROR {code}: {e}")
            continue

        for m in matches:
            try:
                utc_date = m["utcDate"]
                home = m["homeTeam"]["name"]
                away = m["awayTeam"]["name"]
                dt_local = parse_iso_to_local(utc_date)
            except Exception:
                continue

            if not (start <= dt_local <= end):
                continue

            out.append({
                "id": m.get("id"),
                "league": league_name,
                "home_team": home,
                "away_team": away,
                "dt_local": dt_local,
            })

    return out

def predict_cards(league: str, hs: float, aws: float, home: str, away: str) -> Dict[str, int]:
    base_cards = {
        "LaLiga": 5,
        "Segunda División": 6,
        "Champions League": 4,
    }
    total = base_cards.get(league, 5)

    if hs > aws:
        away_cards = min(total - 1, max(2, round(total * 0.58)))
        home_cards = total - away_cards
    elif aws > hs:
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

    hs = stable_team_rating(home) + 3.2
    aws = stable_team_rating(away)

    diff = hs - aws
    abs_diff = abs(diff)

    home_xg = max(0.55, min(1.20 + diff * 0.035, 2.80))
    away_xg = max(0.40, min(1.00 - diff * 0.022, 2.30))
    total_xg = home_xg + away_xg

    winner = home if hs >= aws else away
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
    cards = predict_cards(league, hs, aws, home, away)

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
        "match": f"{home} vs {away}",
        "league": league,
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
    }

def build_picks() -> List[Dict[str, Any]]:
    matches = get_matches()
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
    return {"ok": True, "msg": "API funcionando"}

@app.get("/test")
def test():
    return {"ok": True}

@app.get("/test-api")
def test_api():
    try:
        matches = get_matches()
        return {
            "ok": True,
            "count": len(matches),
            "matches": matches[:5],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/picks")
def picks(force_refresh: bool = False):
    try:
        picks = build_picks()
        return {
            "generated_at": now_local().isoformat(),
            "cache_day": today_key(),
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