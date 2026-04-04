import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("API_KEY", "").strip()
BASE_URL = os.getenv("APISPORTS_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
TZ_NAME = os.getenv("TZ", "Europe/Madrid")

if not API_KEY:
    raise RuntimeError("Falta API_KEY en variables de entorno")

HEADERS = {
    "x-apisports-key": API_KEY,
}

app = FastAPI(title="Top Picks Backend", version="6.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "daily_cache.json"

TARGET_LEAGUES = {
    140, 39, 135, 78, 61,      # top
    141, 40, 136, 79, 62,      # second divisions
    2, 3, 848,                 # Europe
    1, 4                       # World Cup / Euro
}

def league_priority(league_id: int) -> int:
    priority_map = {
        2: 100, 1: 98, 4: 97, 3: 95, 848: 90,
        39: 85, 140: 84, 135: 83, 78: 82, 61: 81,
        40: 70, 141: 69, 79: 68, 136: 67, 62: 66,
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

def clear_cache() -> None:
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
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

def normalize_text(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("-", " ")
        .replace("_", " ")
    )

def parse_selection_label(label: str) -> Optional[str]:
    value = normalize_text(label)

    if value in {"home", "1", "local", "team1", "equipo local"}:
        return "home"
    if value in {"away", "2", "visitante", "team2", "equipo visitante"}:
        return "away"
    if value in {"draw", "x", "empate"}:
        return "draw"

    # over/under
    if "over" in value and "2.5" in value:
        return "over_2_5"
    if "under" in value and "2.5" in value:
        return "under_2_5"

    # btts
    if value in {"yes", "si", "sí"}:
        return "yes"
    if value in {"no"}:
        return "no"

    return None

def market_type_from_name(name: str) -> Optional[str]:
    n = normalize_text(name)

    # winner
    winner_markets = {
        "match winner", "winner", "1x2", "fulltime result",
        "full time result", "match result", "final result",
        "resultado final", "ganador del partido", "tiempo reglamentario",
    }
    if n in winner_markets or any(k in n for k in ["match winner", "fulltime result", "match result", "final result", "ganador"]):
        return "winner"

    # over under 2.5
    if (
        "over/under" in n
        or "goals over/under" in n
        or "total goals" in n
        or "totals" in n
        or "más/menos" in n
        or "mas/menos" in n
    ):
        if "2.5" in n:
            return "over_under_2_5"

    # btts
    if n in {
        "both teams to score",
        "btts",
        "gg/ng",
        "ambos equipos marcan",
        "both teams score"
    } or "both teams" in n or "ambos" in n:
        return "btts"

    return None

def extract_markets_from_odds_item(odds_item: Dict[str, Any]) -> List[Dict[str, Any]]:
    bookmakers = odds_item.get("bookmakers", [])
    if not bookmakers:
        return []

    preferred_bookmakers = [
        "Bet365", "1xBet", "William Hill", "Bwin", "Unibet", "Marathonbet", "Pinnacle"
    ]

    def bookmaker_rank(name: str) -> int:
        normalized = normalize_text(name)
        for idx, preferred in enumerate(preferred_bookmakers):
            if normalize_text(preferred) == normalized:
                return idx
        return 999

    found = []

    sorted_bookmakers = sorted(bookmakers, key=lambda b: bookmaker_rank(str(b.get("name", ""))))

    for bookmaker in sorted_bookmakers:
        bookmaker_name = str(bookmaker.get("name", "Bookmaker"))
        bets = bookmaker.get("bets", [])

        for bet in bets:
            raw_market_name = str(bet.get("name", ""))
            market_type = market_type_from_name(raw_market_name)
            if not market_type:
                continue

            values_map: Dict[str, float] = {}
            for value in bet.get("values", []):
                label = parse_selection_label(value.get("value", ""))
                odd = safe_float(value.get("odd"))
                if label is None or odd is None:
                    continue
                values_map[label] = odd

            if market_type == "winner":
                if "home" in values_map and "away" in values_map:
                    found.append({
                        "market_type": "winner",
                        "bookmaker": bookmaker_name,
                        "market_name": raw_market_name,
                        "values": {
                            "home": values_map["home"],
                            "away": values_map["away"],
                            "draw": values_map.get("draw")
                        },
                        "bookmaker_rank": bookmaker_rank(bookmaker_name),
                    })

            elif market_type == "over_under_2_5":
                if "over_2_5" in values_map:
                    found.append({
                        "market_type": "over_2_5",
                        "bookmaker": bookmaker_name,
                        "market_name": raw_market_name,
                        "values": {
                            "over_2_5": values_map["over_2_5"],
                            "under_2_5": values_map.get("under_2_5"),
                        },
                        "bookmaker_rank": bookmaker_rank(bookmaker_name),
                    })

            elif market_type == "btts":
                if "yes" in values_map:
                    found.append({
                        "market_type": "btts_yes",
                        "bookmaker": bookmaker_name,
                        "market_name": raw_market_name,
                        "values": {
                            "yes": values_map["yes"],
                            "no": values_map.get("no"),
                        },
                        "bookmaker_rank": bookmaker_rank(bookmaker_name),
                    })

    # priorizar mejor bookmaker
    found.sort(key=lambda x: x["bookmaker_rank"])
    return found

def get_match_markets(fixture_id: int) -> List[Dict[str, Any]]:
    try:
        payload = api_get("/odds", {"fixture": fixture_id})
        items = payload.get("response", [])
        if not items:
            return []
        return extract_markets_from_odds_item(items[0])
    except Exception:
        return []

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

def build_match_model(
    fixture_item: Dict[str, Any],
    prediction: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
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

    total_strength = max(home_strength + away_strength, 1e-6)
    p_home = clamp(home_strength / total_strength, 0.05, 0.90)
    p_away = clamp(away_strength / total_strength, 0.05, 0.90)

    avg_goals = (home_stats["goals_for"] + away_stats["goals_for"] + home_stats["goals_against"] + away_stats["goals_against"]) / 2.0
    over25_prob = clamp((avg_goals - 1.8) / 2.0, 0.18, 0.82)

    home_scoring = clamp(home_stats["goals_for"] / 2.2, 0.15, 0.90)
    away_scoring = clamp(away_stats["goals_for"] / 2.2, 0.15, 0.90)
    btts_prob = clamp((home_scoring * away_scoring) + 0.20, 0.18, 0.82)

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

    over_reasons = []
    if avg_goals >= 2.5:
        over_reasons.append("promedio goleador alto")
    if home_stats["goals_for"] >= 1.4:
        over_reasons.append("local con buena producción ofensiva")
    if away_stats["goals_for"] >= 1.2:
        over_reasons.append("visitante también genera ocasiones")
    if home_stats["goals_against"] >= 1.0 or away_stats["goals_against"] >= 1.0:
        over_reasons.append("defensas con margen para conceder")

    btts_reasons = []
    if home_stats["goals_for"] >= 1.1:
        btts_reasons.append("el local suele marcar")
    if away_stats["goals_for"] >= 1.1:
        btts_reasons.append("el visitante suele marcar")
    if home_stats["goals_against"] >= 0.8:
        btts_reasons.append("el local también concede")
    if away_stats["goals_against"] >= 0.8:
        btts_reasons.append("el visitante también concede")

    if not home_reasons:
        home_reasons = ["mejor encaje general", "más estabilidad", "mejor contexto"]
    if not away_reasons:
        away_reasons = ["mejor encaje general", "más estabilidad", "mejor contexto"]
    if not over_reasons:
        over_reasons = ["ritmo ofensivo razonable", "partido abierto", "contexto favorable al gol"]
    if not btts_reasons:
        btts_reasons = ["dos ataques con capacidad de marcar", "defensas mejorables", "partido propenso a intercambio de goles"]

    return {
        "p_home": p_home,
        "p_away": p_away,
        "p_over25": over25_prob,
        "p_btts_yes": btts_prob,
        "home_reasons": home_reasons[:3],
        "away_reasons": away_reasons[:3],
        "over_reasons": over_reasons[:3],
        "btts_reasons": btts_reasons[:3],
    }

def classify_pick_type(odds: float) -> str:
    if 1.45 <= odds <= 2.10:
        return "solido"
    if 2.10 < odds <= 3.50:
        return "medio"
    return "agresivo"

def confidence_from_edge(edge: float, model_prob: float) -> str:
    if edge >= 0.10 and model_prob >= 0.55:
        return "verde"
    if edge >= 0.03:
        return "amarillo"
    return "rojo"

def score_pick(edge: float, model_prob: float, pick_type: str) -> float:
    type_bonus = {"solido": 0.10, "medio": 0.08, "agresivo": 0.06}.get(pick_type, 0.0)
    return edge * 0.55 + model_prob * 0.25 + type_bonus

def valid_by_type(pick_type: str, edge: float, model_prob: float) -> bool:
    if pick_type == "solido":
        return edge >= -0.02 and model_prob >= 0.44
    if pick_type == "medio":
        return edge >= -0.03 and model_prob >= 0.32
    if pick_type == "agresivo":
        return edge >= -0.05 and model_prob >= 0.20
    return False

def build_tipster_explanation(label: str, reasons: List[str], model_prob: float, implied_prob: float, odds: float) -> str:
    edge = round((model_prob - implied_prob) * 100, 1)
    joined = ", ".join(reasons[:3])
    return (
        f"{label} entra por {joined}. "
        f"La cuota {round(odds, 2)} implica un {round(implied_prob * 100, 1)}%, "
        f"pero el modelo lo estima en {round(model_prob * 100, 1)}%. "
        f"Value estimado: {edge:+.1f}%."
    )

def build_candidate(
    fixture_id: int,
    competition: str,
    country: str,
    league_id: int,
    match: str,
    starts_at: str,
    pick: str,
    market_group: str,
    odds: float,
    model_prob: float,
    implied_prob: float,
    confidence: str,
    pick_type: str,
    bookmaker: str,
    market_name: str,
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
        "market_group": market_group,
        "odds": round(odds, 2),
        "model_probability": round(model_prob * 100, 1),
        "implied_probability": round(implied_prob * 100, 1),
        "value_edge": round((model_prob - implied_prob) * 100, 1),
        "confidence": confidence,
        "type": pick_type,
        "bookmaker": bookmaker,
        "market_name": market_name,
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

        markets = get_match_markets(fixture_id)
        if not markets:
            continue

        prediction = get_prediction(fixture_id)
        model = build_match_model(item, prediction)

        home_name = teams["home"]["name"]
        away_name = teams["away"]["name"]
        match = f"{home_name} vs {away_name}"
        starts_at = iso_to_local_hhmm(fixture["date"])
        competition = league.get("name")
        country = league.get("country")
        league_id = league.get("id", 0)

        for market in markets:
            bookmaker = market["bookmaker"]
            market_name = market["market_name"]

            if market["market_type"] == "winner":
                home_odds = market["values"]["home"]
                away_odds = market["values"]["away"]

                if 1.45 <= home_odds <= 8.00:
                    p = model["p_home"]
                    imp = implied_probability(home_odds)
                    edge = p - imp
                    pick_type = classify_pick_type(home_odds)
                    conf = confidence_from_edge(edge, p)
                    candidate = build_candidate(
                        fixture_id, competition, country, league_id, match, starts_at,
                        f"Gana {home_name}", "winner",
                        home_odds, p, imp, conf, pick_type, bookmaker, market_name,
                        build_tipster_explanation(f"Gana {home_name}", model["home_reasons"], p, imp, home_odds),
                        score_pick(edge, p, pick_type)
                    )
                    fallback_candidates.append(candidate)
                    if valid_by_type(pick_type, edge, p):
                        strong_candidates.append(candidate)

                if 1.45 <= away_odds <= 8.00:
                    p = model["p_away"]
                    imp = implied_probability(away_odds)
                    edge = p - imp
                    pick_type = classify_pick_type(away_odds)
                    conf = confidence_from_edge(edge, p)
                    candidate = build_candidate(
                        fixture_id, competition, country, league_id, match, starts_at,
                        f"Gana {away_name}", "winner",
                        away_odds, p, imp, conf, pick_type, bookmaker, market_name,
                        build_tipster_explanation(f"Gana {away_name}", model["away_reasons"], p, imp, away_odds),
                        score_pick(edge, p, pick_type)
                    )
                    fallback_candidates.append(candidate)
                    if valid_by_type(pick_type, edge, p):
                        strong_candidates.append(candidate)

            elif market["market_type"] == "over_2_5":
                over_odds = market["values"]["over_2_5"]
                if over_odds and 1.45 <= over_odds <= 8.00:
                    p = model["p_over25"]
                    imp = implied_probability(over_odds)
                    edge = p - imp
                    pick_type = classify_pick_type(over_odds)
                    conf = confidence_from_edge(edge, p)
                    candidate = build_candidate(
                        fixture_id, competition, country, league_id, match, starts_at,
                        "Más de 2.5 goles", "over_2_5",
                        over_odds, p, imp, conf, pick_type, bookmaker, market_name,
                        build_tipster_explanation("Más de 2.5 goles", model["over_reasons"], p, imp, over_odds),
                        score_pick(edge, p, pick_type)
                    )
                    fallback_candidates.append(candidate)
                    if valid_by_type(pick_type, edge, p):
                        strong_candidates.append(candidate)

            elif market["market_type"] == "btts_yes":
                yes_odds = market["values"]["yes"]
                if yes_odds and 1.45 <= yes_odds <= 8.00:
                    p = model["p_btts_yes"]
                    imp = implied_probability(yes_odds)
                    edge = p - imp
                    pick_type = classify_pick_type(yes_odds)
                    conf = confidence_from_edge(edge, p)
                    candidate = build_candidate(
                        fixture_id, competition, country, league_id, match, starts_at,
                        "Ambos marcan: Sí", "btts_yes",
                        yes_odds, p, imp, conf, pick_type, bookmaker, market_name,
                        build_tipster_explanation("Ambos marcan: Sí", model["btts_reasons"], p, imp, yes_odds),
                        score_pick(edge, p, pick_type)
                    )
                    fallback_candidates.append(candidate)
                    if valid_by_type(pick_type, edge, p):
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
    used_fixture_market = set()

    def add_candidate(item: Dict[str, Any]) -> bool:
        key = (item["fixture_id"], item["market_group"])
        if key in used_fixture_market:
            return False
        selected.append(item)
        used_fixture_market.add(key)
        return True

    solid = [c for c in strong_candidates if c["type"] == "solido"]
    medium = [c for c in strong_candidates if c["type"] == "medio"]
    aggressive = [c for c in strong_candidates if c["type"] == "agresivo"]

    for group in (solid, medium, aggressive):
        for item in group:
            if add_candidate(item):
                break

    for item in strong_candidates:
        if len(selected) >= 5:
            break
        add_candidate(item)

    for item in fallback_candidates:
        if len(selected) >= 5:
            break
        add_candidate(item)

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
    return {"status": "ok", "service": "top-picks-backend-v6"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/top-picks-today")
def top_picks_today(refresh: int = Query(default=0)):
    if refresh == 1:
        clear_cache()
    else:
        cached = load_cache()
        if cached:
            return cached

    try:
        return generate_real_picks()
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error API-Football: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")