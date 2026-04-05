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

app = FastAPI(title="Top Picks Backend", version="11.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "daily_cache.json"

# Sport keys reales de The Odds API
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

# Algunos sport keys pueden no estar activos según la temporada/cobertura
SPORT_KEY_ALIASES = {
    "soccer_france_ligue_one": ["soccer_france_ligue_one", "soccer_france_ligue_1"],
    "soccer_france_ligue_two": ["soccer_france_ligue_two", "soccer_france_ligue_2"],
    "soccer_efl_champ": ["soccer_efl_champ", "soccer_england_efl_championship"],
    "soccer_germany_bundesliga2": ["soccer_germany_bundesliga2", "soccer_germany_bundesliga_2"],
    "soccer_fifa_world_cup": ["soccer_fifa_world_cup"],
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
    return 1.0 / odds if odds > 0 else 0.0


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


def load_cache() -> Optional[Dict[str, Any]]:
    if not os.path.exists(CACHE_FILE):
        return None
    try:
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


def odds_api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{ODDS_API_BASE_URL}{path}"
    params = params or {}
    params["apiKey"] = ODDS_API_KEY
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def bookmaker_rank(key_or_title: str) -> int:
    norm = normalize_text(key_or_title)
    for idx, name in enumerate(BOOKMAKER_PRIORITY):
        if normalize_text(name) == norm:
            return idx
    return 999


def sport_priority(sport_key: str) -> int:
    return TARGET_SPORTS.get(sport_key, {}).get("priority", 10)


def fetch_events_for_sport(sport_key: str) -> List[Dict[str, Any]]:
    aliases = SPORT_KEY_ALIASES.get(sport_key, [sport_key])

    for alias in aliases:
        try:
            data = odds_api_get(
                f"/v4/sports/{alias}/odds",
                {
                    "regions": REGIONS,
                    "markets": "h2h,totals",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                },
            )
            if isinstance(data, list):
                return data
        except Exception as e:
            log("Error sport", alias, str(e))
            continue

    return []


def get_upcoming_fixtures() -> List[Dict[str, Any]]:
    now = madrid_now()
    events: List[Dict[str, Any]] = []
    seen = set()

    for sport_key in TARGET_SPORTS.keys():
        items = fetch_events_for_sport(sport_key)
        for item in items:
            event_id = item.get("id")
            if not event_id or event_id in seen:
                continue

            commence_time = item.get("commence_time")
            if not commence_time:
                continue

            try:
                dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(
                    pytz.timezone(TZ_NAME)
                )
            except Exception:
                continue

            # Solo prepartido cercano
            if dt < now - timedelta(hours=2):
                continue

            item["_priority"] = sport_priority(item.get("sport_key", ""))
            events.append(item)
            seen.add(event_id)

    events.sort(key=lambda x: (-x.get("_priority", 10), x.get("commence_time", "")))
    log("Fixtures encontrados:", len(events))
    return events


def get_best_h2h_market(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    best = None
    best_rank = 999

    for bookmaker in event.get("bookmakers", []):
        rank = bookmaker_rank(bookmaker.get("key") or bookmaker.get("title", ""))
        for market in bookmaker.get("markets", []):
            if market.get("key") != "h2h":
                continue
            outcomes = market.get("outcomes", [])
            if len(outcomes) < 2:
                continue
            candidate = {
                "bookmaker": bookmaker.get("title", "Bookmaker"),
                "bookmaker_key": bookmaker.get("key", ""),
                "last_update": bookmaker.get("last_update"),
                "outcomes": outcomes,
            }
            if rank < best_rank:
                best = candidate
                best_rank = rank

    return best


def get_best_totals_market(event: Dict[str, Any], target_point: float = 2.5) -> Optional[Dict[str, Any]]:
    best = None
    best_rank = 999

    for bookmaker in event.get("bookmakers", []):
        rank = bookmaker_rank(bookmaker.get("key") or bookmaker.get("title", ""))
        for market in bookmaker.get("markets", []):
            if market.get("key") != "totals":
                continue
            outcomes = market.get("outcomes", [])
            if not outcomes:
                continue

            # buscamos línea 2.5
            over = None
            under = None
            for outcome in outcomes:
                point = safe_float(outcome.get("point"))
                if point != target_point:
                    continue
                name = str(outcome.get("name", "")).strip().lower()
                if name == "over":
                    over = outcome
                elif name == "under":
                    under = outcome

            if over is not None:
                candidate = {
                    "bookmaker": bookmaker.get("title", "Bookmaker"),
                    "bookmaker_key": bookmaker.get("key", ""),
                    "over": over,
                    "under": under,
                }
                if rank < best_rank:
                    best = candidate
                    best_rank = rank

    return best


def build_market_consensus(event: Dict[str, Any]) -> Dict[str, float]:
    home_team = event.get("home_team", "")
    away_team = event.get("away_team", "")

    home_prices: List[float] = []
    away_prices: List[float] = []
    draw_prices: List[float] = []
    over25_prices: List[float] = []

    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market.get("key") == "h2h":
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name")
                    price = safe_float(outcome.get("price"))
                    if price is None:
                        continue
                    if name == home_team:
                        home_prices.append(price)
                    elif name == away_team:
                        away_prices.append(price)
                    elif str(name).strip().lower() in {"draw", "empate"}:
                        draw_prices.append(price)

            elif market.get("key") == "totals":
                for outcome in market.get("outcomes", []):
                    point = safe_float(outcome.get("point"))
                    name = str(outcome.get("name", "")).strip().lower()
                    price = safe_float(outcome.get("price"))
                    if point == 2.5 and name == "over" and price is not None:
                        over25_prices.append(price)

    def avg(xs: List[float], default: float) -> float:
        return sum(xs) / len(xs) if xs else default

    avg_home = avg(home_prices, 2.20)
    avg_away = avg(away_prices, 2.20)
    avg_draw = avg(draw_prices, 3.20)
    avg_over25 = avg(over25_prices, 1.95)

    home_imp = implied_probability(avg_home)
    away_imp = implied_probability(avg_away)
    draw_imp = implied_probability(avg_draw)
    total = max(home_imp + away_imp + draw_imp, 1e-9)

    p_home = clamp(home_imp / total, 0.10, 0.80)
    p_away = clamp(away_imp / total, 0.10, 0.80)
    p_draw = clamp(draw_imp / total, 0.08, 0.50)

    # Over 2.5 si no hay totals, estimación conservadora por equilibrio del partido
    balance = 1.0 - abs(p_home - p_away)
    p_over25 = clamp(0.42 + (balance * 0.18), 0.30, 0.68)
    if over25_prices:
        p_over25 = clamp(implied_probability(avg_over25) + 0.03, 0.30, 0.72)

    p_btts = clamp(0.40 + (balance * 0.16), 0.28, 0.68)

    return {
        "p_home": p_home,
        "p_away": p_away,
        "p_draw": p_draw,
        "p_over25": p_over25,
        "p_btts_yes": p_btts,
        "avg_home_odds": avg_home,
        "avg_away_odds": avg_away,
        "avg_draw_odds": avg_draw,
        "avg_over25_odds": avg_over25,
    }


def build_reasons(consensus: Dict[str, float], side: str) -> List[str]:
    p_home = consensus["p_home"]
    p_away = consensus["p_away"]
    reasons: List[str] = []

    if side == "home":
        if p_home > p_away:
            reasons.append("mejor consenso de mercado")
        if abs(p_home - p_away) > 0.08:
            reasons.append("ventaja clara en probabilidad implícita")
        reasons.append("mejor encaje prepartido")
    elif side == "away":
        if p_away > p_home:
            reasons.append("mejor consenso de mercado")
        if abs(p_away - p_home) > 0.08:
            reasons.append("ventaja clara en probabilidad implícita")
        reasons.append("perfil competitivo favorable")
    elif side == "over25":
        reasons.extend([
            "línea de goles favorable",
            "partido con perfil abierto",
            "probabilidad de over por encima del umbral",
        ])
    elif side == "btts":
        reasons.extend([
            "partido equilibrado",
            "intercambio de goles plausible",
            "modelo interno favorable",
        ])

    return reasons[:3]


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


def build_tipster_explanation(label: str, reasons: List[str], model_prob: float, implied_prob: float, odds: float, source_type: str) -> str:
    edge = round((model_prob - implied_prob) * 100, 1)
    joined = ", ".join(reasons[:3])

    if source_type == "real_odds":
        return (
            f"{label} entra por {joined}. "
            f"La cuota {round(odds, 2)} implica un {round(implied_prob * 100, 1)}%, "
            f"y el modelo la estima en {round(model_prob * 100, 1)}%. "
            f"Value estimado: {edge:+.1f}%."
        )

    return (
        f"{label} entra por {joined}. "
        f"No había mercado utilizable para este pick, así que se usa el modelo interno. "
        f"Probabilidad estimada: {round(model_prob * 100, 1)}%."
    )


def build_candidate(
    event: Dict[str, Any],
    competition: str,
    country: str,
    match: str,
    starts_at: str,
    pick: str,
    market_group: str,
    odds: float,
    model_prob: float,
    implied_prob_: float,
    confidence: str,
    pick_type: str,
    bookmaker: str,
    market_name: str,
    explanation: str,
    score: float,
    source_type: str,
) -> Dict[str, Any]:
    return {
        "fixture_id": event.get("id"),
        "competition": competition,
        "country": country,
        "league_id": event.get("sport_key"),
        "league_priority": sport_priority(event.get("sport_key", "")),
        "match": match,
        "starts_at": starts_at,
        "pick": pick,
        "market_group": market_group,
        "odds": round(odds, 2),
        "model_probability": round(model_prob * 100, 1),
        "implied_probability": round(implied_prob_ * 100, 1),
        "value_edge": round((model_prob - implied_prob_) * 100, 1),
        "confidence": confidence,
        "type": pick_type,
        "bookmaker": bookmaker,
        "market_name": market_name,
        "source_type": source_type,
        "tipster_explanation": explanation,
        "score": round(score, 6),
    }


def get_candidates() -> Dict[str, List[Dict[str, Any]]]:
    events = get_upcoming_fixtures()
    strong_candidates: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []

    for event in events:
        competition = event.get("sport_title", TARGET_SPORTS.get(event.get("sport_key", ""), {}).get("title", "Competition"))
        country = "N/D"
        home_name = event.get("home_team", "Local")
        away_name = event.get("away_team", "Visitante")
        match = f"{home_name} vs {away_name}"
        starts_at = iso_to_local_hhmm(event.get("commence_time"))

        consensus = build_market_consensus(event)

        h2h_market = get_best_h2h_market(event)
        totals_market = get_best_totals_market(event, target_point=2.5)

        if h2h_market:
            outcomes = h2h_market["outcomes"]

            home_odds = None
            away_odds = None
            for outcome in outcomes:
                price = safe_float(outcome.get("price"))
                if price is None:
                    continue
                if outcome.get("name") == home_name:
                    home_odds = price
                elif outcome.get("name") == away_name:
                    away_odds = price

            if home_odds and 1.45 <= home_odds <= 8.00:
                p = consensus["p_home"]
                imp = implied_probability(home_odds)
                edge = p - imp
                pick_type = classify_pick_type(home_odds)
                conf = confidence_from_edge(edge, p)
                candidate = build_candidate(
                    event, competition, country, match, starts_at,
                    f"Gana {home_name}", "winner",
                    home_odds, p, imp, conf, pick_type,
                    h2h_market["bookmaker"], "h2h",
                    build_tipster_explanation(
                        f"Gana {home_name}",
                        build_reasons(consensus, "home"),
                        p, imp, home_odds, "real_odds"
                    ),
                    score_pick(edge, p, pick_type),
                    "real_odds",
                )
                fallback_candidates.append(candidate)
                if valid_by_type(pick_type, edge, p):
                    strong_candidates.append(candidate)

            if away_odds and 1.45 <= away_odds <= 8.00:
                p = consensus["p_away"]
                imp = implied_probability(away_odds)
                edge = p - imp
                pick_type = classify_pick_type(away_odds)
                conf = confidence_from_edge(edge, p)
                candidate = build_candidate(
                    event, competition, country, match, starts_at,
                    f"Gana {away_name}", "winner",
                    away_odds, p, imp, conf, pick_type,
                    h2h_market["bookmaker"], "h2h",
                    build_tipster_explanation(
                        f"Gana {away_name}",
                        build_reasons(consensus, "away"),
                        p, imp, away_odds, "real_odds"
                    ),
                    score_pick(edge, p, pick_type),
                    "real_odds",
                )
                fallback_candidates.append(candidate)
                if valid_by_type(pick_type, edge, p):
                    strong_candidates.append(candidate)

        if totals_market:
            over = totals_market.get("over")
            over_odds = safe_float(over.get("price")) if over else None
            if over_odds and 1.45 <= over_odds <= 8.00:
                p = consensus["p_over25"]
                imp = implied_probability(over_odds)
                edge = p - imp
                pick_type = classify_pick_type(over_odds)
                conf = confidence_from_edge(edge, p)
                candidate = build_candidate(
                    event, competition, country, match, starts_at,
                    "Más de 2.5 goles", "over_2_5",
                    over_odds, p, imp, conf, pick_type,
                    totals_market["bookmaker"], "totals_2.5",
                    build_tipster_explanation(
                        "Más de 2.5 goles",
                        build_reasons(consensus, "over25"),
                        p, imp, over_odds, "real_odds"
                    ),
                    score_pick(edge, p, pick_type),
                    "real_odds",
                )
                fallback_candidates.append(candidate)
                if valid_by_type(pick_type, edge, p):
                    strong_candidates.append(candidate)

        # Fallback por evento si no hubo mercado utilizable
        event_has_candidate = any(c["fixture_id"] == event.get("id") for c in fallback_candidates)
        if not event_has_candidate:
            # Elegimos el mejor ángulo del modelo
            options = [
                ("winner", f"Gana {home_name}", consensus["p_home"], build_reasons(consensus, "home")),
                ("winner", f"Gana {away_name}", consensus["p_away"], build_reasons(consensus, "away")),
                ("over_2_5", "Más de 2.5 goles", consensus["p_over25"], build_reasons(consensus, "over25")),
                ("btts_yes", "Ambos marcan: Sí", consensus["p_btts_yes"], build_reasons(consensus, "btts")),
            ]
            market_group, pick, p, reasons = max(options, key=lambda x: x[2])
            estimated_odds = clamp(round(1 / max(p, 0.18), 2), 1.45, 4.50)
            imp = implied_probability(estimated_odds)
            edge = p - imp
            pick_type = classify_pick_type(estimated_odds)
            conf = confidence_from_edge(edge, p)

            candidate = build_candidate(
                event, competition, country, match, starts_at,
                pick, market_group,
                estimated_odds, p, imp, conf, pick_type,
                "MODEL", "IA Pick",
                build_tipster_explanation(pick, reasons, p, imp, estimated_odds, "model_fallback"),
                score_pick(edge, p, pick_type),
                "model_fallback",
            )
            fallback_candidates.append(candidate)
            strong_candidates.append(candidate)

    strong_candidates.sort(
        key=lambda x: (
            x["league_priority"],
            x["score"],
            x["value_edge"],
            x["model_probability"],
        ),
        reverse=True,
    )

    fallback_candidates.sort(
        key=lambda x: (
            x["league_priority"],
            x["score"],
            x["model_probability"],
            x["odds"],
        ),
        reverse=True,
    )

    log("Strong candidates:", len(strong_candidates))
    log("Fallback candidates:", len(fallback_candidates))

    return {
        "strong": strong_candidates,
        "fallback": fallback_candidates,
    }


def select_daily_picks(strong_candidates: List[Dict[str, Any]], fallback_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    used_keys = set()

    def add_candidate(item: Dict[str, Any]) -> bool:
        key = (item["fixture_id"], item["market_group"])
        if key in used_keys:
            return False
        selected.append(item)
        used_keys.add(key)
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
    candidates = get_candidates()
    picks = select_daily_picks(candidates["strong"], candidates["fallback"])

    now = madrid_now()
    cached_until = now + timedelta(hours=24)

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "generated_at": now.strftime("%H:%M"),
        "cached_until": cached_until.isoformat(),
        "source": "The Odds API + IA fallback",
        "count": len(picks),
        "picks": picks,
    }

    save_cache(data)
    return data


@app.get("/")
def root():
    return {"status": "ok", "service": "top-picks-backend-v11-odds-api"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug-raw-odds")
def debug_raw_odds():
    try:
        samples = {}
        for sport_key in list(TARGET_SPORTS.keys())[:5]:
            data = fetch_events_for_sport(sport_key)
            samples[sport_key] = {
                "count": len(data),
                "sample": data[:1],
            }

        return {
            "base_url": ODDS_API_BASE_URL,
            "regions": REGIONS,
            "samples": samples,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug raw odds error: {str(e)}")


@app.get("/debug-top-picks")
def debug_top_picks():
    try:
        fixtures = get_upcoming_fixtures()
        candidates = get_candidates()
        return {
            "fixtures_found": len(fixtures),
            "strong_candidates": len(candidates["strong"]),
            "fallback_candidates": len(candidates["fallback"]),
            "preview": candidates["fallback"][:5],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug error: {str(e)}")


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
        raise HTTPException(status_code=502, detail=f"Error The Odds API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")