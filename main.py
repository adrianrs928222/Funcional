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

app = FastAPI(title="Top Picks Backend", version="15.0.0")

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
    return pytz.timezone(TZ_NAME).localize(
        datetime.combine(tomorrow, datetime.min.time())
    ) + timedelta(minutes=5)


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
    except Exception:
        pass


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
    except Exception:
        pass


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
            log("Error sport odds", alias, str(e))
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
            log("Error sport scores", alias, str(e))
            continue

    return []


def get_today_fixtures() -> List[Dict[str, Any]]:
    now = madrid_now()
    today_str = now.strftime("%Y-%m-%d")

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

            # solo hoy
            if dt.strftime("%Y-%m-%d") != today_str:
                continue

            # nunca live
            if dt <= now:
                continue

            item["_priority"] = sport_priority(item.get("sport_key", ""))
            events.append(item)
            seen.add(event_id)

    events.sort(
        key=lambda x: (-x.get("_priority", 10), x.get("commence_time", ""))
    )

    log("Fixtures de hoy encontrados:", len(events))
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

            over = None
            under = None

            for outcome in market.get("outcomes", []):
                point = safe_float(outcome.get("point"))
                if point != target_point:
                    continue
                name = str(outcome.get("name", "")).strip().lower()
                if name == "over":
                    over = outcome
                elif name == "under":
                    under = outcome

            if over:
                candidate = {
                    "bookmaker": bookmaker.get("title", "Bookmaker"),
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

    avg_home = avg(home_prices, 2.15)
    avg_away = avg(away_prices, 2.15)
    avg_draw = avg(draw_prices, 3.15)
    avg_over25 = avg(over25_prices, 1.95)

    home_imp = implied_probability(avg_home)
    away_imp = implied_probability(avg_away)
    draw_imp = implied_probability(avg_draw)
    total = max(home_imp + away_imp + draw_imp, 1e-9)

    p_home = clamp(home_imp / total, 0.12, 0.78)
    p_away = clamp(away_imp / total, 0.12, 0.78)
    p_draw = clamp(draw_imp / total, 0.08, 0.40)

    balance = 1.0 - abs(p_home - p_away)
    p_over25 = clamp(0.43 + (balance * 0.17), 0.30, 0.70)
    if over25_prices:
        p_over25 = clamp(implied_probability(avg_over25) + 0.03, 0.30, 0.72)

    return {
        "p_home": p_home,
        "p_away": p_away,
        "p_draw": p_draw,
        "p_over25": p_over25,
    }


def build_reasons(side: str) -> List[str]:
    if side == "home":
        return ["mejor consenso de mercado", "ventaja prepartido", "partido favorable"][:3]
    if side == "away":
        return ["mejor consenso de mercado", "valor en visitante", "perfil competitivo sólido"][:3]
    return ["partido abierto", "línea favorable", "perfil goleador"][:3]


def classify_pick_type(odds: float) -> str:
    if 1.75 <= odds <= 2.39:
        return "medio"
    return "agresivo"


def confidence_from_edge(edge: float, model_prob: float) -> str:
    if edge >= 0.08 and model_prob >= 0.53:
        return "verde"
    if edge >= 0.02:
        return "amarillo"
    return "rojo"


def score_pick(edge: float, model_prob: float, pick_type: str) -> float:
    type_bonus = {"medio": 0.09, "agresivo": 0.11}.get(pick_type, 0.0)
    return edge * 0.58 + model_prob * 0.24 + type_bonus


def valid_band(odds: float) -> bool:
    return 1.65 <= odds <= 4.80


def candidate_quality_tier(candidate: Dict[str, Any]) -> str:
    odds = float(candidate.get("odds", 0))
    edge = float(candidate.get("value_edge", 0))
    model_prob = float(candidate.get("model_probability", 0)) / 100.0
    confidence = str(candidate.get("confidence", "")).lower()

    if edge >= 5.0 and model_prob >= 0.50 and 1.70 <= odds <= 3.60:
        return "A"

    if edge >= 2.0 and model_prob >= 0.45 and 1.65 <= odds <= 4.20:
        return "B"

    if edge >= -1.5 and model_prob >= 0.40 and 1.65 <= odds <= 4.80:
        return "C"

    if confidence in {"verde", "amarillo"} and 1.65 <= odds <= 4.80:
        return "D"

    return "Z"


def build_tipster_explanation(label: str, reasons: List[str], model_prob: float, implied_prob_: float, odds: float) -> str:
    edge = round((model_prob - implied_prob_) * 100, 1)
    joined = ", ".join(reasons[:3])

    return (
        f"{label} entra por {joined}. "
        f"Cuota {round(odds, 2)} con probabilidad implícita del {round(implied_prob_ * 100, 1)}%, "
        f"frente a una estimación del modelo del {round(model_prob * 100, 1)}%. "
        f"Value estimado: {edge:+.1f}%."
    )


def build_candidate(
    event: Dict[str, Any],
    competition: str,
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
) -> Dict[str, Any]:
    return {
        "fixture_id": event.get("id"),
        "competition": competition,
        "country": "N/D",
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
        "source_type": "real_odds",
        "tipster_explanation": explanation,
        "score": round(score, 6),
        "status": "pending",
        "result_label": "Pendiente",
    }


def get_candidates() -> List[Dict[str, Any]]:
    events = get_today_fixtures()
    candidates: List[Dict[str, Any]] = []

    for event in events:
        competition = event.get(
            "sport_title",
            TARGET_SPORTS.get(event.get("sport_key", ""), {}).get("title", "Competition")
        )
        home_name = event.get("home_team", "Local")
        away_name = event.get("away_team", "Visitante")
        match = f"{home_name} vs {away_name}"
        starts_at = iso_to_local_hhmm(event.get("commence_time"))

        consensus = build_market_consensus(event)
        event_candidates: List[Dict[str, Any]] = []

        h2h_market = get_best_h2h_market(event)
        totals_market = get_best_totals_market(event, 2.5)

        if h2h_market:
            home_odds = None
            away_odds = None

            for outcome in h2h_market["outcomes"]:
                price = safe_float(outcome.get("price"))
                if price is None:
                    continue
                if outcome.get("name") == home_name:
                    home_odds = price
                elif outcome.get("name") == away_name:
                    away_odds = price

            if home_odds and valid_band(home_odds):
                p = consensus["p_home"]
                imp = implied_probability(home_odds)
                edge = p - imp
                pick_type = classify_pick_type(home_odds)
                event_candidates.append(
                    build_candidate(
                        event, competition, match, starts_at,
                        f"Gana {home_name}", "winner",
                        home_odds, p, imp, confidence_from_edge(edge, p), pick_type,
                        h2h_market["bookmaker"], "h2h",
                        build_tipster_explanation(f"Gana {home_name}", build_reasons("home"), p, imp, home_odds),
                        score_pick(edge, p, pick_type),
                    )
                )

            if away_odds and valid_band(away_odds):
                p = consensus["p_away"]
                imp = implied_probability(away_odds)
                edge = p - imp
                pick_type = classify_pick_type(away_odds)
                event_candidates.append(
                    build_candidate(
                        event, competition, match, starts_at,
                        f"Gana {away_name}", "winner",
                        away_odds, p, imp, confidence_from_edge(edge, p), pick_type,
                        h2h_market["bookmaker"], "h2h",
                        build_tipster_explanation(f"Gana {away_name}", build_reasons("away"), p, imp, away_odds),
                        score_pick(edge, p, pick_type),
                    )
                )

        if totals_market:
            over = totals_market.get("over")
            over_odds = safe_float(over.get("price")) if over else None
            if over_odds and valid_band(over_odds):
                p = consensus["p_over25"]
                imp = implied_probability(over_odds)
                edge = p - imp
                pick_type = classify_pick_type(over_odds)
                event_candidates.append(
                    build_candidate(
                        event, competition, match, starts_at,
                        "Más de 2.5 goles", "over_2_5",
                        over_odds, p, imp, confidence_from_edge(edge, p), pick_type,
                        totals_market["bookmaker"], "totals_2.5",
                        build_tipster_explanation("Más de 2.5 goles", build_reasons("over25"), p, imp, over_odds),
                        score_pick(edge, p, pick_type),
                    )
                )

        if event_candidates:
            best = sorted(event_candidates, key=lambda x: x["score"], reverse=True)[0]
            candidates.append(best)

    candidates.sort(
        key=lambda x: (
            x["league_priority"],
            x["score"],
            x["model_probability"],
            x["odds"],
        ),
        reverse=True,
    )

    return candidates


def select_daily_picks(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    used_fixtures = set()

    def can_add(item: Dict[str, Any]) -> bool:
        return item["fixture_id"] not in used_fixtures

    def add_item(item: Dict[str, Any]) -> bool:
        if not can_add(item):
            return False
        selected.append(item)
        used_fixtures.add(item["fixture_id"])
        return True

    tier_a = [c for c in candidates if candidate_quality_tier(c) == "A"]
    tier_b = [c for c in candidates if candidate_quality_tier(c) == "B"]
    tier_c = [c for c in candidates if candidate_quality_tier(c) == "C"]
    tier_d = [c for c in candidates if candidate_quality_tier(c) == "D"]

    def sort_key(x: Dict[str, Any]):
        return (
            x.get("league_priority", 0),
            x.get("score", 0),
            x.get("value_edge", 0),
            x.get("odds", 0),
        )

    tier_a.sort(key=sort_key, reverse=True)
    tier_b.sort(key=sort_key, reverse=True)
    tier_c.sort(key=sort_key, reverse=True)
    tier_d.sort(key=sort_key, reverse=True)

    for item in tier_a:
        if len(selected) >= 2:
            break
        add_item(item)

    for item in tier_b:
        if len(selected) >= 4:
            break
        add_item(item)

    for item in tier_c:
        if len(selected) >= 5:
            break
        add_item(item)

    remainder = tier_a + tier_b + tier_c + tier_d
    for item in remainder:
        if len(selected) >= 5:
            break
        add_item(item)

    if len(selected) < 5:
        all_sorted = sorted(candidates, key=sort_key, reverse=True)
        for item in all_sorted:
            if len(selected) >= 5:
                break
            add_item(item)

    return selected[:5]


def determine_pick_result(pick: Dict[str, Any], score_event: Dict[str, Any]) -> Dict[str, str]:
    scores = score_event.get("scores") or []
    if not score_event.get("completed") or len(scores) < 2:
        return {"status": "pending", "result_label": "Pendiente"}

    score_map = {}
    for item in scores:
        try:
            score_map[item.get("name")] = int(item.get("score"))
        except Exception:
            return {"status": "pending", "result_label": "Pendiente"}

    home = score_event.get("home_team")
    away = score_event.get("away_team")
    if home not in score_map or away not in score_map:
        return {"status": "pending", "result_label": "Pendiente"}

    home_goals = score_map[home]
    away_goals = score_map[away]
    total_goals = home_goals + away_goals

    market_group = pick.get("market_group")
    pick_text = str(pick.get("pick", ""))

    won = False

    if market_group == "winner":
        if pick_text == f"Gana {home}" and home_goals > away_goals:
            won = True
        elif pick_text == f"Gana {away}" and away_goals > home_goals:
            won = True
    elif market_group == "over_2_5":
        won = total_goals > 2.5

    return {
        "status": "won" if won else "lost",
        "result_label": "Acertada" if won else "Perdida",
    }


def settle_history() -> Dict[str, Any]:
    history = load_history()

    scores_index: Dict[str, Dict[str, Any]] = {}
    for sport_key in TARGET_SPORTS.keys():
        for event in fetch_scores_for_sport(sport_key, days_from=3):
            event_id = event.get("id")
            if event_id:
                scores_index[event_id] = event

    changed = False

    for _, day_data in history.get("days", {}).items():
        picks = day_data.get("picks", [])
        for pick in picks:
            if pick.get("status") in {"won", "lost"}:
                continue

            event_id = pick.get("fixture_id")
            score_event = scores_index.get(event_id)
            if not score_event:
                continue

            result = determine_pick_result(pick, score_event)
            if result["status"] != pick.get("status"):
                pick["status"] = result["status"]
                pick["result_label"] = result["result_label"]
                changed = True

        won = sum(1 for p in picks if p.get("status") == "won")
        lost = sum(1 for p in picks if p.get("status") == "lost")
        pending = sum(1 for p in picks if p.get("status") == "pending")

        day_data["stats"] = {
            "won": won,
            "lost": lost,
            "pending": pending,
            "total": len(picks),
        }

    if changed:
        save_history(history)

    return history


def persist_today_in_history(data: Dict[str, Any]) -> None:
    history = load_history()
    day_key = data["date"]

    won = sum(1 for p in data["picks"] if p.get("status") == "won")
    lost = sum(1 for p in data["picks"] if p.get("status") == "lost")
    pending = sum(1 for p in data["picks"] if p.get("status") == "pending")

    history["days"][day_key] = {
        "date": data["date"],
        "generated_at": data["generated_at"],
        "count": data["count"],
        "stats": {
            "won": won,
            "lost": lost,
            "pending": pending,
            "total": data["count"],
        },
        "picks": data["picks"],
    }
    save_history(history)


def build_history_response() -> Dict[str, Any]:
    history = settle_history()

    days = list(history.get("days", {}).values())
    days.sort(key=lambda x: x.get("date", ""), reverse=True)

    total_won = sum(day.get("stats", {}).get("won", 0) for day in days)
    total_lost = sum(day.get("stats", {}).get("lost", 0) for day in days)
    total_pending = sum(day.get("stats", {}).get("pending", 0) for day in days)
    total_picks = sum(day.get("stats", {}).get("total", 0) for day in days)
    settled = total_won + total_lost
    hit_rate = round((total_won / settled) * 100, 1) if settled > 0 else 0.0

    return {
        "summary": {
            "total_picks": total_picks,
            "won": total_won,
            "lost": total_lost,
            "pending": total_pending,
            "hit_rate": hit_rate,
        },
        "days": days[:30],
    }


def generate_daily_picks() -> Dict[str, Any]:
    candidates = get_candidates()
    picks = select_daily_picks(candidates)

    now = madrid_now()
    cached_until = daily_cache_deadline()

    data = {
        "date": now.strftime("%Y-%m-%d"),
        "generated_at": now.strftime("%H:%M"),
        "cached_until": cached_until.isoformat(),
        "cache_day": now.strftime("%Y-%m-%d"),
        "source": "The Odds API",
        "count": len(picks),
        "picks": picks,
    }

    save_cache(data)
    persist_today_in_history(data)
    return data


@app.get("/")
def root():
    return {"status": "ok", "service": "top-picks-backend-v15"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug-top-picks")
def debug_top_picks():
    try:
        fixtures = get_today_fixtures()
        candidates = get_candidates()
        picks = select_daily_picks(candidates)
        return {
            "fixtures_today_found": len(fixtures),
            "candidates_found": len(candidates),
            "daily_picks_count": len(picks),
            "preview": picks[:5],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug error: {str(e)}")


@app.get("/history-picks")
def history_picks():
    try:
        return build_history_response()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"History error: {str(e)}")


@app.get("/top-picks-today")
def top_picks_today(refresh: int = Query(default=0)):
    # refresh se ignora a propósito para no cambiar los picks del día
    cached = load_cache()
    if cached:
        return cached

    try:
        return generate_daily_picks()
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error The Odds API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")