import math
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("API_KEY", "").strip()
BASE_URL = os.getenv("APISPORTS_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
TZ_NAME = os.getenv("TZ", "Europe/Madrid")

if not API_KEY:
    raise RuntimeError("Falta API_KEY en variables de entorno")

HEADERS = {
    "x-apisports-key": API_KEY,
}

app = FastAPI(title="Top Picks Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DAILY_CACHE: Dict[str, Dict[str, Any]] = {}

# Ligas prioritarias. Puedes ampliar esta lista.
TARGET_LEAGUES = {
    # España
    140,  # LaLiga
    141,  # Segunda
    # UEFA
    2,    # Champions
    3,    # Europa League
    848,  # Conference League
    # Inglaterra
    39,   # Premier
    40,   # Championship
    # Italia
    135,  # Serie A
    136,  # Serie B
    # Alemania
    78,   # Bundesliga
    79,   # Bundesliga 2
    # Francia
    61,   # Ligue 1
    62,   # Ligue 2
    # Mundiales / selecciones
    1,    # World Cup
    4,    # Euro Championship
}

def madrid_now() -> datetime:
    tz = pytz.timezone(TZ_NAME)
    return datetime.now(tz)

def day_key() -> str:
    return madrid_now().strftime("%Y-%m-%d")

def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, headers=HEADERS, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None

def implied_probability(odds: float) -> float:
    return 1.0 / odds if odds > 0 else 0.0

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def iso_to_local_hhmm(iso_str: str) -> str:
    tz = pytz.timezone(TZ_NAME)
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(tz)
    return dt.strftime("%H:%M")

def season_for_today() -> int:
    now = madrid_now()
    # Temporada simple estilo europeo
    return now.year if now.month >= 7 else now.year - 1

def next_7_days() -> Tuple[str, str]:
    now = madrid_now()
    end = now + timedelta(days=7)
    return now.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def get_upcoming_fixtures() -> List[Dict[str, Any]]:
    date_from, date_to = next_7_days()
    fixtures: List[Dict[str, Any]] = []

    # Pedimos por fecha, luego filtramos ligas objetivo
    current = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")

    while current <= end:
        payload = api_get("/fixtures", {"date": current.strftime("%Y-%m-%d")})
        for item in payload.get("response", []):
            league_id = item.get("league", {}).get("id")
            status_short = item.get("fixture", {}).get("status", {}).get("short")
            if league_id in TARGET_LEAGUES and status_short in {"NS", "TBD"}:
                fixtures.append(item)
        current += timedelta(days=1)

    return fixtures

def get_prediction(fixture_id: int) -> Optional[Dict[str, Any]]:
    try:
        payload = api_get("/predictions", {"fixture": fixture_id})
        items = payload.get("response", [])
        return items[0] if items else None
    except Exception:
        return None

def pick_best_h2h_from_odds_response(odds_item: Dict[str, Any]) -> Optional[Tuple[float, float, str]]:
    """
    Devuelve (home_odds, away_odds, bookmaker_name)
    Intenta adaptarse a distintas formas de respuesta del endpoint odds.
    """
    bookmakers = odds_item.get("bookmakers", [])
    if not bookmakers:
        return None

    # Preferimos bookmaker conocido si existe
    preferred = ["Bet365", "1xBet", "William Hill", "Bwin", "Marathonbet", "Unibet"]

    def bookmaker_rank(name: str) -> int:
        try:
            return preferred.index(name)
        except ValueError:
            return 999

    bookmakers_sorted = sorted(
        bookmakers,
        key=lambda b: bookmaker_rank(str(b.get("name", "")))
    )

    for bookmaker in bookmakers_sorted:
        name = str(bookmaker.get("name", "Bookmaker"))
        bets = bookmaker.get("bets", [])
        for bet in bets:
            # 1X2 / Match Winner / Winner
            bet_name = str(bet.get("name", "")).lower()
            if bet_name not in {"match winner", "winner", "1x2"}:
                continue

            values = bet.get("values", [])
            home_odds = None
            away_odds = None

            for v in values:
                label = str(v.get("value", "")).strip().lower()
                odd = safe_float(v.get("odd"))
                if odd is None:
                    continue

                # Home / Away o 1 / 2
                if label in {"home", "1"}:
                    home_odds = odd
                elif label in {"away", "2"}:
                    away_odds = odd

            if home_odds and away_odds:
                return home_odds, away_odds, name

    return None

def get_match_odds(fixture_id: int) -> Optional[Dict[str, Any]]:
    try:
        payload = api_get("/odds", {"fixture": fixture_id})
        items = payload.get("response", [])
        if not items:
            return None

        parsed = pick_best_h2h_from_odds_response(items[0])
        if not parsed:
            return None

        home_odds, away_odds, bookmaker = parsed
        return {
            "home_odds": home_odds,
            "away_odds": away_odds,
            "bookmaker": bookmaker,
        }
    except Exception:
        return None

def recent_form_points(form_str: Optional[str]) -> float:
    if not form_str:
        return 0.5
    score = 0.0
    mapping = {"W": 1.0, "D": 0.5, "L": 0.0}
    vals = [mapping.get(ch.upper(), 0.5) for ch in form_str[-5:]]
    if not vals:
        return 0.5
    return sum(vals) / len(vals)

def get_last5_team_stats(team_id: int, is_home_context: bool) -> Dict[str, float]:
    try:
        payload = api_get("/fixtures", {
            "team": team_id,
            "last": 5,
            "status": "FT"
        })
        items = payload.get("response", [])

        if not items:
            return {
                "points_form": 0.5,
                "goals_for": 1.0,
                "goals_against": 1.0,
                "win_rate": 0.4,
                "clean_sheet_rate": 0.2,
                "context_rate": 0.5,
            }

        points = 0.0
        gf = 0.0
        ga = 0.0
        wins = 0
        clean = 0
        context_good = 0

        for m in items:
            home_id = m["teams"]["home"]["id"]
            away_id = m["teams"]["away"]["id"]
            home_goals = m["goals"]["home"] or 0
            away_goals = m["goals"]["away"] or 0

            is_home = team_id == home_id
            team_goals = home_goals if is_home else away_goals
            opp_goals = away_goals if is_home else home_goals

            gf += team_goals
            ga += opp_goals

            if team_goals > opp_goals:
                points += 3
                wins += 1
            elif team_goals == opp_goals:
                points += 1

            if opp_goals == 0:
                clean += 1

            if is_home_context == is_home:
                if team_goals >= opp_goals:
                    context_good += 1

        n = len(items)
        return {
            "points_form": points / (n * 3),
            "goals_for": gf / n,
            "goals_against": ga / n,
            "win_rate": wins / n,
            "clean_sheet_rate": clean / n,
            "context_rate": context_good / n,
        }
    except Exception:
        return {
            "points_form": 0.5,
            "goals_for": 1.0,
            "goals_against": 1.0,
            "win_rate": 0.4,
            "clean_sheet_rate": 0.2,
            "context_rate": 0.5,
        }

def normalize_goal_attack(x: float) -> float:
    return clamp(x / 2.5, 0.0, 1.0)

def normalize_goal_defense(x: float) -> float:
    # Menos goles encajados = mejor
    return clamp(1.0 - (x / 2.5), 0.0, 1.0)

def model_probabilities(
    fixture_item: Dict[str, Any],
    prediction: Optional[Dict[str, Any]],
) -> Tuple[float, float, List[str], List[str]]:
    home_team = fixture_item["teams"]["home"]
    away_team = fixture_item["teams"]["away"]

    home_stats = get_last5_team_stats(home_team["id"], is_home_context=True)
    away_stats = get_last5_team_stats(away_team["id"], is_home_context=False)

    # Señal extra del endpoint predictions si viene
    pred_percent_home = 0.0
    pred_percent_away = 0.0
    if prediction:
        compare = prediction.get("comparison", {})
        try:
            pred_percent_home = safe_float(str(compare.get("form", {}).get("home", "0")).replace("%", "")) or 0.0
            pred_percent_away = safe_float(str(compare.get("form", {}).get("away", "0")).replace("%", "")) or 0.0
            pred_percent_home /= 100.0
            pred_percent_away /= 100.0
        except Exception:
            pred_percent_home = 0.0
            pred_percent_away = 0.0

    home_strength = (
        0.24 * home_stats["points_form"] +
        0.18 * home_stats["context_rate"] +
        0.17 * normalize_goal_attack(home_stats["goals_for"]) +
        0.16 * normalize_goal_defense(home_stats["goals_against"]) +
        0.10 * home_stats["win_rate"] +
        0.08 * home_stats["clean_sheet_rate"] +
        0.07 * pred_percent_home
    )

    away_strength = (
        0.24 * away_stats["points_form"] +
        0.18 * away_stats["context_rate"] +
        0.17 * normalize_goal_attack(away_stats["goals_for"]) +
        0.16 * normalize_goal_defense(away_stats["goals_against"]) +
        0.10 * away_stats["win_rate"] +
        0.08 * away_stats["clean_sheet_rate"] +
        0.07 * pred_percent_away
    )

    # Pequeña ventaja casa
    home_strength += 0.03

    total = max(home_strength + away_strength, 1e-6)
    p_home = clamp(home_strength / total, 0.05, 0.90)
    p_away = clamp(away_strength / total, 0.05, 0.90)

    home_reasons = []
    away_reasons = []

    if home_stats["points_form"] > away_stats["points_form"]:
        home_reasons.append("mejor forma reciente")
    if away_stats["points_form"] > home_stats["points_form"]:
        away_reasons.append("mejor forma reciente")

    if home_stats["context_rate"] > away_stats["context_rate"]:
        home_reasons.append("rinde mejor en casa")
    if away_stats["context_rate"] > home_stats["context_rate"]:
        away_reasons.append("rinde mejor fuera de casa")

    if home_stats["goals_for"] > away_stats["goals_for"]:
        home_reasons.append("más producción ofensiva")
    if away_stats["goals_for"] > home_stats["goals_for"]:
        away_reasons.append("más producción ofensiva")

    if home_stats["goals_against"] < away_stats["goals_against"]:
        home_reasons.append("más solidez defensiva")
    if away_stats["goals_against"] < home_stats["goals_against"]:
        away_reasons.append("más solidez defensiva")

    if not home_reasons:
        home_reasons = ["mejor encaje general del modelo", "más estabilidad", "contexto favorable"]
    if not away_reasons:
        away_reasons = ["mejor encaje general del modelo", "más estabilidad", "contexto favorable"]

    return p_home, p_away, home_reasons[:3], away_reasons[:3]

def confidence_from_edge(edge: float, model_prob: float) -> str:
    if edge >= 0.10 and model_prob >= 0.55:
        return "verde"
    if edge >= 0.06:
        return "amarillo"
    return "rojo"

def side_label(side: str, home_name: str, away_name: str) -> str:
    return f"Ganador {home_name}" if side == "home" else f"Ganador {away_name}"

def build_tipster_explanation(
    side: str,
    reasons: List[str],
    model_prob: float,
    implied_prob: float
) -> str:
    subject = "El local" if side == "home" else "El visitante"
    diff = round((model_prob - implied_prob) * 100, 1)
    joined = ", ".join(reasons[:3])
    return (
        f"{subject} entra por {joined}. "
        f"El modelo le da un {round(model_prob * 100, 1)}% de probabilidad, "
        f"por encima del {round(implied_prob * 100, 1)}% implícito de la cuota. "
        f"Value estimado: +{diff}%."
    )

def score_pick(edge: float, model_prob: float, confidence: str) -> float:
    conf_bonus = {"verde": 0.12, "amarillo": 0.06, "rojo": 0.0}.get(confidence, 0.0)
    return edge * 0.65 + model_prob * 0.25 + conf_bonus

def generate_real_picks() -> Dict[str, Any]:
    fixtures = get_upcoming_fixtures()
    picks: List[Dict[str, Any]] = []

    for item in fixtures:
        fixture = item.get("fixture", {})
        league = item.get("league", {})
        teams = item.get("teams", {})

        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        odds = get_match_odds(fixture_id)
        if not odds:
            continue

        home_odds = odds["home_odds"]
        away_odds = odds["away_odds"]

        # Solo cuotas entre 1.60 y 6.00
        valid_home = 1.60 <= home_odds <= 6.00
        valid_away = 1.60 <= away_odds <= 6.00
        if not valid_home and not valid_away:
            continue

        prediction = get_prediction(fixture_id)
        p_home, p_away, home_reasons, away_reasons = model_probabilities(item, prediction)

        implied_home = implied_probability(home_odds)
        implied_away = implied_probability(away_odds)

        edge_home = p_home - implied_home if valid_home else -999.0
        edge_away = p_away - implied_away if valid_away else -999.0

        # Umbral mínimo de value
        if edge_home < 0.06 and edge_away < 0.06:
            continue

        if edge_home >= edge_away:
            side = "home"
            chosen_odds = home_odds
            model_prob = p_home
            implied_prob = implied_home
            edge = edge_home
            reasons = home_reasons
        else:
            side = "away"
            chosen_odds = away_odds
            model_prob = p_away
            implied_prob = implied_away
            edge = edge_away
            reasons = away_reasons

        confidence = confidence_from_edge(edge, model_prob)
        if confidence == "rojo":
            continue

        home_name = teams["home"]["name"]
        away_name = teams["away"]["name"]

        pick = {
            "fixture_id": fixture_id,
            "competition": league.get("name"),
            "country": league.get("country"),
            "match": f"{home_name} vs {away_name}",
            "starts_at": iso_to_local_hhmm(fixture["date"]),
            "pick": side_label(side, home_name, away_name),
            "side": side,
            "odds": round(chosen_odds, 2),
            "model_probability": round(model_prob * 100, 1),
            "implied_probability": round(implied_prob * 100, 1),
            "value_edge": round(edge * 100, 1),
            "confidence": confidence,
            "bookmaker": odds.get("bookmaker"),
            "tipster_explanation": build_tipster_explanation(
                side=side,
                reasons=reasons,
                model_prob=model_prob,
                implied_prob=implied_prob,
            ),
            "league_id": league.get("id"),
            "score": round(score_pick(edge, model_prob, confidence), 6),
        }
        picks.append(pick)

    # Orden por score, luego mejor value, luego mejor cuota
    picks.sort(key=lambda x: (x["score"], x["value_edge"], x["odds"]), reverse=True)

    # Máximo 3 picks
    final_picks = picks[:3]

    return {
        "date": day_key(),
        "generated_at": madrid_now().strftime("%H:%M"),
        "source": "API-FOOTBALL real fixtures + odds + predictions",
        "picks": final_picks,
    }

@app.get("/")
def root():
    return {"status": "ok", "service": "top-picks-backend"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/top-picks-today")
def top_picks_today():
    key = day_key()

    if key in DAILY_CACHE:
        return DAILY_CACHE[key]

    try:
        data = generate_real_picks()
        DAILY_CACHE.clear()
        DAILY_CACHE[key] = data
        return data
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error API-Football: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
