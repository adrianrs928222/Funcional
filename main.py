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

app = FastAPI(title="Top Picks Backend", version="5.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "daily_cache.json"

TARGET_LEAGUES = {
    # Ligas top
    140,  # LaLiga
    39,   # Premier
    135,  # Serie A
    78,   # Bundesliga
    61,   # Ligue 1

    # Segundas
    141,  # LaLiga 2
    40,   # Championship
    136,  # Serie B
    79,   # Bundesliga 2
    62,   # Ligue 2

    # Europa
    2,    # Champions
    3,    # Europa League
    848,  # Conference League

    # Internacional
    1,    # World Cup
    4     # Euro
}

def league_priority(league_id: int) -> int:
    priority_map = {
        2: 100,
        1: 98,
        4: 97,
        3: 95,
        848: 90,

        39: 85,
        140: 84,
        135: 83,
        78: 82,
        61: 81,

        40: 70,
        141: 69,
        79: 68,
        136: 67,
        62: 66,
    }
    return priority_map.get(league_id, 10)

def madrid_now() -> datetime:
    return datetime.now(pytz.timezone(TZ_NAME))

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

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def implied_probability(odds: float) -> float:
    return 1.0 / odds if odds > 0 else 0.0

def iso_to_local_hhmm(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(pytz.timezone(TZ_NAME)).strftime("%H:%M")

def load_cache() -> Optional[Dict[str, Any]]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
        import json
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_until = data.get("cached_until")
        if not cached_until:
            return None
        until_dt = datetime.fromisoformat(cached_until)
        if madrid_now() < until_dt.astimezone(pytz.timezone(TZ_NAME)):
            return data
        return None
    except Exception:
        return None

def save_cache(data: Dict[str, Any]) -> None:
    try:
        import json
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def get_upcoming_fixtures(days_ahead: int = 7) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    now = madrid_now()

    for i in range(days_ahead):
        target_date = (now + timedelta(days=i)).strftime("%Y-%m-%d")
        payload = api_get("/fixtures", {"date": target_date})

        for item in payload.get("response", []):
            league_id = item.get("league", {}).get("id")
            status_short = item.get("fixture", {}).get("status", {}).get("short")
            fixture_date_str = item.get("fixture", {}).get("date")

            if league_id not in TARGET_LEAGUES:
                continue
            if status_short not in {"NS", "TBD"}:
                continue

            try:
                fixture_dt = datetime.fromisoformat(fixture_date_str.replace("Z", "+00:00")).astimezone(
                    pytz.timezone(TZ_NAME)
                )
            except Exception:
                continue

            if fixture_dt <= now:
                continue

            fixtures.append(item)

    fixtures.sort(
        key=lambda x: (
            -league_priority(x.get("league", {}).get("id", 0)),
            x.get("fixture", {}).get("date", "")
        )
    )
    return fixtures

def get_prediction(fixture_id: int) -> Optional[Dict[str, Any]]:
    try:
        payload = api_get("/predictions", {"fixture": fixture_id})
        items = payload.get("response", [])
        return items[0] if items else None
    except Exception:
        return None

def pick_best_odds(odds_item: Dict[str, Any]) -> Optional[Tuple[float, float, str]]:
    bookmakers = odds_item.get("bookmakers", [])
    if not bookmakers:
        return None

    preferred = ["Bet365", "1xBet", "William Hill", "Bwin", "Unibet", "Marathonbet"]

    def rank_name(name: str) -> int:
        try:
            return preferred.index(name)
        except ValueError:
            return 999

    bookmakers = sorted(bookmakers, key=lambda b: rank_name(str(b.get("name", ""))))

    for bookmaker in bookmakers:
        bookmaker_name = str(bookmaker.get("name", "Bookmaker"))
        bets = bookmaker.get("bets", [])

        for bet in bets:
            bet_name = str(bet.get("name", "")).lower()
            if bet_name not in {"match winner", "winner", "1x2"}:
                continue

            home_odds = None
            away_odds = None

            for value in bet.get("values", []):
                label = str(value.get("value", "")).strip().lower()
                odd = safe_float(value.get("odd"))
                if odd is None:
                    continue

                if label in {"home", "1"}:
                    home_odds = odd
                elif label in {"away", "2"}:
                    away_odds = odd

            if home_odds and away_odds:
                return home_odds, away_odds, bookmaker_name

    return None

def get_match_odds(fixture_id: int) -> Optional[Dict[str, Any]]:
    try:
        payload = api_get("/odds", {"fixture": fixture_id})
        items = payload.get("response", [])
        if not items:
            return None

        parsed = pick_best_odds(items[0])
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

def get_last5_team_stats(team_id: int, home_context: bool) -> Dict[str, float]:
    try:
        payload = api_get("/fixtures", {
            "team": team_id,
            "last": 5,
            "status": "FT"
        })
        items = payload.get("response", [])

        if not items:
            return {
                "points_form": 0.50,
                "goals_for": 1.00,
                "goals_against": 1.00,
                "win_rate": 0.40,
                "clean_sheet_rate": 0.20,
                "context_rate": 0.50,
                "consistency": 0.50,
            }

        points = 0.0
        gf = 0.0
        ga = 0.0
        wins = 0
        clean = 0
        context_good = 0
        goal_diffs = []

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
            goal_diffs.append(abs(team_goals - opp_goals))

            if team_goals > opp_goals:
                points += 3
                wins += 1
            elif team_goals == opp_goals:
                points += 1

            if opp_goals == 0:
                clean += 1

            if is_home == home_context and team_goals >= opp_goals:
                context_good += 1

        n = len(items)
        avg_diff = sum(goal_diffs) / n if n else 1.0
        consistency = clamp(1.0 - (avg_diff / 3.0), 0.0, 1.0)

        return {
            "points_form": points / (n * 3),
            "goals_for": gf / n,
            "goals_against": ga / n,
            "win_rate": wins / n,
            "clean_sheet_rate": clean / n,
            "context_rate": context_good / n,
            "consistency": consistency,
        }
    except Exception:
        return {
            "points_form": 0.50,
            "goals_for": 1.00,
            "goals_against": 1.00,
            "win_rate": 0.40,
            "clean_sheet_rate": 0.20,
            "context_rate": 0.50,
            "consistency": 0.50,
        }

def normalize_attack(x: float) -> float:
    return clamp(x / 2.5, 0.0, 1.0)

def normalize_defense(x: float) -> float:
    return clamp(1.0 - (x / 2.5), 0.0, 1.0)

def model_probabilities(
    fixture_item: Dict[str, Any],
    prediction: Optional[Dict[str, Any]],
) -> Tuple[float, float, List[str], List[str]]:
    home_team = fixture_item["teams"]["home"]
    away_team = fixture_item["teams"]["away"]

    home_stats = get_last5_team_stats(home_team["id"], home_context=True)
    away_stats = get_last5_team_stats(away_team["id"], home_context=False)

    pred_home = 0.0
    pred_away = 0.0
    if prediction:
        try:
            winner = prediction.get("predictions", {}).get("winner", {})
            winner_name = str(winner.get("name", "")).strip().lower()

            if winner_name == str(home_team["name"]).strip().lower():
                pred_home = 0.08
            elif winner_name == str(away_team["name"]).strip().lower():
                pred_away = 0.08
        except Exception:
            pass

    home_strength = (
        0.22 * home_stats["points_form"] +
        0.18 * home_stats["context_rate"] +
        0.15 * normalize_attack(home_stats["goals_for"]) +
        0.15 * normalize_defense(home_stats["goals_against"]) +
        0.08 * home_stats["win_rate"] +
        0.07 * home_stats["clean_sheet_rate"] +
        0.08 * home_stats["consistency"] +
        0.07 * pred_home
    )

    away_strength = (
        0.22 * away_stats["points_form"] +
        0.18 * away_stats["context_rate"] +
        0.15 * normalize_attack(away_stats["goals_for"]) +
        0.15 * normalize_defense(away_stats["goals_against"]) +
        0.08 * away_stats["win_rate"] +
        0.07 * away_stats["clean_sheet_rate"] +
        0.08 * away_stats["consistency"] +
        0.07 * pred_away
    )

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
        away_reasons.append("rinde mejor fuera")

    if home_stats["goals_for"] > away_stats["goals_for"]:
        home_reasons.append("más gol reciente")
    if away_stats["goals_for"] > home_stats["goals_for"]:
        away_reasons.append("más gol reciente")

    if home_stats["goals_against"] < away_stats["goals_against"]:
        home_reasons.append("más solidez defensiva")
    if away_stats["goals_against"] < home_stats["goals_against"]:
        away_reasons.append("más solidez defensiva")

    if home_stats["consistency"] > away_stats["consistency"]:
        home_reasons.append("más consistencia")
    if away_stats["consistency"] > home_stats["consistency"]:
        away_reasons.append("más consistencia")

    if not home_reasons:
        home_reasons = ["mejor encaje general", "más estabilidad", "mejor contexto"]
    if not away_reasons:
        away_reasons = ["mejor encaje general", "más estabilidad", "mejor contexto"]

    return p_home, p_away, home_reasons[:3], away_reasons[:3]

def classify_pick_type(odds: float) -> str:
    if 1.60 <= odds <= 2.20:
        return "solido"
    if 2.00 <= odds <= 3.20:
        return "medio"
    return "agresivo"

def confidence_from_edge(edge: float, model_prob: float) -> str:
    if edge >= 0.10 and model_prob >= 0.55:
        return "verde"
    if edge >= 0.03:
        return "amarillo"
    return "rojo"

def side_label(side: str, home_name: str, away_name: str) -> str:
    return f"Gana {home_name}" if side == "home" else f"Gana {away_name}"

def build_tipster_explanation(
    side: str,
    reasons: List[str],
    model_prob: float,
    implied_prob: float,
    odds: float
) -> str:
    subject = "El local" if side == "home" else "El visitante"
    edge = round((model_prob - implied_prob) * 100, 1)
    joined = ", ".join(reasons[:3])
    return (
        f"{subject} entra por {joined}. "
        f"La cuota {round(odds, 2)} implica un {round(implied_prob * 100, 1)}%, "
        f"pero el modelo lo estima en {round(model_prob * 100, 1)}%. "
        f"Value estimado: +{edge}%."
    )

def score_pick(edge: float, model_prob: float, pick_type: str, consistency_boost: float) -> float:
    type_bonus = {"solido": 0.10, "medio": 0.08, "agresivo": 0.06}.get(pick_type, 0.0)
    return edge * 0.55 + model_prob * 0.25 + consistency_boost * 0.10 + type_bonus

# MÁS FLEXIBLE PARA QUE SALGAN PICKS REALES CON MÁS FACILIDAD
def valid_by_type(pick_type: str, edge: float, model_prob: float) -> bool:
    if pick_type == "solido":
        return edge >= 0.00 and model_prob >= 0.48
    if pick_type == "medio":
        return edge >= -0.01 and model_prob >= 0.35
    if pick_type == "agresivo":
        return edge >= -0.02 and model_prob >= 0.22
    return False

def build_candidate(
    fixture_id: int,
    competition: str,
    country: str,
    league_id: int,
    match: str,
    starts_at: str,
    pick: str,
    side: str,
    odds: float,
    model_prob: float,
    implied_prob: float,
    confidence: str,
    pick_type: str,
    bookmaker: str,
    explanation: str,
    score: float
) -> Dict[str, Any]:
    return {
        "fixture_id": fixture_id,
        "competition": competition,
        "country": country,
        "league_id": league_id,
        "league_priority": league_priority(league_id),
        "match": match,
        "starts_at": starts_at,
        "pick": pick,
        "side": side,
        "odds": round(odds, 2),
        "model_probability": round(model_prob * 100, 1),
        "implied_probability": round(implied_prob * 100, 1),
        "value_edge": round((model_prob - implied_prob) * 100, 1),
        "confidence": confidence,
        "type": pick_type,
        "bookmaker": bookmaker,
        "tipster_explanation": explanation,
        "score": round(score, 6),
    }

def get_candidates() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fixtures = get_upcoming_fixtures()
    strong_candidates: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []

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

        valid_home = 1.60 <= home_odds <= 6.00
        valid_away = 1.60 <= away_odds <= 6.00
        if not valid_home and not valid_away:
            continue

        prediction = get_prediction(fixture_id)
        p_home, p_away, home_reasons, away_reasons = model_probabilities(item, prediction)

        implied_home = implied_probability(home_odds)
        implied_away = implied_probability(away_odds)

        home_name = teams["home"]["name"]
        away_name = teams["away"]["name"]
        match = f"{home_name} vs {away_name}"
        starts_at = iso_to_local_hhmm(fixture["date"])
        competition = league.get("name")
        country = league.get("country")
        league_id = league.get("id", 0)
        bookmaker = odds.get("bookmaker", "N/D")

        if valid_home:
            pick_type = classify_pick_type(home_odds)
            edge = p_home - implied_home
            conf = confidence_from_edge(edge, p_home)
            candidate = build_candidate(
                fixture_id, competition, country, league_id, match, starts_at,
                side_label("home", home_name, away_name), "home",
                home_odds, p_home, implied_home, conf, pick_type, bookmaker,
                build_tipster_explanation("home", home_reasons, p_home, implied_home, home_odds),
                score_pick(edge, p_home, pick_type, 0.5)
            )
            fallback_candidates.append(candidate)
            if valid_by_type(pick_type, edge, p_home):
                strong_candidates.append(candidate)

        if valid_away:
            pick_type = classify_pick_type(away_odds)
            edge = p_away - implied_away
            conf = confidence_from_edge(edge, p_away)
            candidate = build_candidate(
                fixture_id, competition, country, league_id, match, starts_at,
                side_label("away", home_name, away_name), "away",
                away_odds, p_away, implied_away, conf, pick_type, bookmaker,
                build_tipster_explanation("away", away_reasons, p_away, implied_away, away_odds),
                score_pick(edge, p_away, pick_type, 0.5)
            )
            fallback_candidates.append(candidate)
            if valid_by_type(pick_type, edge, p_away):
                strong_candidates.append(candidate)

    strong_candidates.sort(
        key=lambda x: (
            x["league_priority"],
            x["score"],
            x["value_edge"],
            x["model_probability"]
        ),
        reverse=True
    )

    fallback_candidates.sort(
        key=lambda x: (
            x["league_priority"],
            x["score"],
            x["model_probability"],
            x["odds"]
        ),
        reverse=True
    )

    return strong_candidates, fallback_candidates

def select_daily_picks(strong_candidates: List[Dict[str, Any]], fallback_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    used_fixtures = set()

    solid = [c for c in strong_candidates if c["type"] == "solido"]
    medium = [c for c in strong_candidates if c["type"] == "medio"]
    aggressive = [c for c in strong_candidates if c["type"] == "agresivo"]

    def take_best(group: List[Dict[str, Any]]):
        for item in group:
            if item["fixture_id"] not in used_fixtures:
                selected.append(item)
                used_fixtures.add(item["fixture_id"])
                return

    take_best(solid)
    take_best(medium)
    take_best(aggressive)

    for item in strong_candidates:
        if len(selected) >= 5:
            break
        if item["fixture_id"] in used_fixtures:
            continue
        selected.append(item)
        used_fixtures.add(item["fixture_id"])

    for item in fallback_candidates:
        if len(selected) >= 5:
            break
        if item["fixture_id"] in used_fixtures:
            continue
        selected.append(item)
        used_fixtures.add(item["fixture_id"])

    return selected[:5]

def generate_real_picks() -> Dict[str, Any]:
    strong_candidates, fallback_candidates = get_candidates()
    picks = select_daily_picks(strong_candidates, fallback_candidates)

    now = madrid_now()
    cached_until = now + timedelta(hours=24)

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "generated_at": now.strftime("%H:%M"),
        "cached_until": cached_until.isoformat(),
        "source": "API-FOOTBALL real fixtures + odds + predictions",
        "count": len(picks),
        "picks": picks
    }
    save_cache(data)
    return data

@app.get("/")
def root():
    return {"status": "ok", "service": "top-picks-backend-v5.1"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/top-picks-today")
def top_picks_today():
    cached = load_cache()
    if cached:
        return cached

    try:
        return generate_real_picks()
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error API-Football: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")