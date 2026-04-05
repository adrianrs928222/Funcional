import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# CONFIG
# =========================================================

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
BASE_URL = "https://api.the-odds-api.com"
TZ = pytz.timezone("Europe/Madrid")

if not ODDS_API_KEY:
    raise RuntimeError("Falta ODDS_API_KEY")

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"

CACHE_TTL_HOURS = 24
LOOKAHEAD_HOURS = 24
MAX_PICKS = 12
SCORES_DAYS_BACK = 3
MAX_HISTORY_DAYS = 10

# Menos ligas = menos consumo
SPORTS: Dict[str, str] = {
    "soccer_uefa_champs_league": "Champions League",
    "soccer_uefa_europa_league": "Europa League",
    "soccer_spain_la_liga": "LaLiga",
    "soccer_spain_segunda_division": "LaLiga Hypermotion",
    "soccer_epl": "Premier League",
    "soccer_italy_serie_a": "Serie A",
    "soccer_germany_bundesliga": "Bundesliga",
    "soccer_netherlands_eredivisie": "Eredivisie",
}

LEAGUE_PRIORITY: Dict[str, int] = {
    "Champions League": 100,
    "Europa League": 95,
    "LaLiga": 90,
    "Premier League": 89,
    "Serie A": 88,
    "Bundesliga": 87,
    "Eredivisie": 86,
    "LaLiga Hypermotion": 82,
}

app = FastAPI(title="Top Picks Pro API - Optimized")

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

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")

def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()

def safe_float(value: Any) -> Optional[float]:
    try:
        n = float(value)
        return n if n > 0 else None
    except Exception:
        return None

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

def parse_iso_to_local(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ)

def to_iso_z(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return utc_dt.isoformat().replace("+00:00", "Z")

def get_league_priority(name: str) -> int:
    return LEAGUE_PRIORITY.get(name, 0)

def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    generated_at = cache.get("generated_at")
    picks = cache.get("picks")

    if not generated_at or not isinstance(picks, list) or len(picks) == 0:
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    if dt.tzinfo is None:
        dt = TZ.localize(dt)

    return (now_local() - dt.astimezone(TZ)) < timedelta(hours=CACHE_TTL_HOURS)

def api(path: str, params: Dict[str, Any]) -> Any:
    q = dict(params)
    q["apiKey"] = ODDS_API_KEY

    r = requests.get(BASE_URL + path, params=q, timeout=25)

    if not r.ok:
        detail = r.text[:500]
        raise requests.HTTPError(f"{r.status_code} {detail}")

    return r.json()

# =========================================================
# FETCH EVENTS
# =========================================================

def get_events_window() -> List[Dict[str, Any]]:
    """
    Solo usa H2H para ahorrar cuota.
    """
    start_utc = now_utc()
    end_utc = start_utc + timedelta(hours=LOOKAHEAD_HOURS)

    events: List[Dict[str, Any]] = []

    for sport_key, league_name in SPORTS.items():
        try:
            data = api(
                f"/v4/sports/{sport_key}/odds",
                {
                    "regions": "eu",
                    "markets": "h2h",
                    "dateFormat": "iso",
                    "oddsFormat": "decimal",
                    "commenceTimeFrom": to_iso_z(start_utc),
                    "commenceTimeTo": to_iso_z(end_utc),
                },
            )
        except Exception:
            continue

        for e in data:
            try:
                dt_local = parse_iso_to_local(e["commence_time"])
            except Exception:
                continue

            e["_league"] = league_name
            e["_sport"] = sport_key
            e["_dt_local"] = dt_local
            events.append(e)

    return events

# =========================================================
# MARKET PARSER
# =========================================================

def get_market(bookmaker: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    for market in bookmaker.get("markets", []):
        if market.get("key") == key:
            return market
    return None

def pick_best_bookmaker(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    # como solo pedimos h2h, basta el primero con h2h
    for b in bookmakers:
        if get_market(b, "h2h"):
            return b

    return bookmakers[0] if bookmakers else None

def parse_h2h(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = pick_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "h2h")
    if not market:
        return None

    home = event.get("home_team")
    away = event.get("away_team")

    if not home or not away:
        teams = event.get("teams", [])
        if isinstance(teams, list) and len(teams) >= 2:
            home = event.get("home_team") or teams[0]
            away = next((t for t in teams if t != home), None)

    if not home or not away:
        return None

    odds_map = {o.get("name"): safe_float(o.get("price")) for o in market.get("outcomes", [])}

    return {
        "bookmaker": bookmaker.get("title", "Bookmaker"),
        "home_team": home,
        "away_team": away,
        "home_odds": odds_map.get(home),
        "away_odds": odds_map.get(away),
        "draw_odds": odds_map.get("Draw"),
    }

# =========================================================
# ESTIMACIONES DESDE H2H
# =========================================================

def implied_prob(odds: Optional[float]) -> Optional[float]:
    if not odds or odds <= 1:
        return None
    return 1.0 / odds

def normalize_three_way_probs(home_odds: float, draw_odds: Optional[float], away_odds: float) -> Tuple[float, float, float]:
    ph = implied_prob(home_odds) or 0.0
    pd = implied_prob(draw_odds) or 0.0
    pa = implied_prob(away_odds) or 0.0

    total = ph + pd + pa
    if total <= 0:
        return 0.40, 0.25, 0.35

    return ph / total, pd / total, pa / total

def estimate_over25_prob(ph: float, pd: float, pa: float) -> float:
    """
    Heurística:
    - menos empate => más partido abierto
    - favoritismo muy desbalanceado => puede bajar un poco
    """
    imbalance = abs(ph - pa)
    prob = 0.54 + (0.24 * (1 - pd)) - (0.08 * imbalance)
    return max(0.42, min(prob, 0.74))

def estimate_btts_prob(ph: float, pd: float, pa: float) -> float:
    """
    Heurística:
    - equilibrio entre equipos favorece BTTS
    - mucho empate también ayuda un poco
    - mucho favoritismo lo baja
    """
    imbalance = abs(ph - pa)
    prob = 0.50 + (0.16 * pd) + (0.08 * (1 - imbalance)) - (0.10 * imbalance)
    return max(0.40, min(prob, 0.70))

def pseudo_price_from_prob(prob: float) -> float:
    # margen ligero
    return round(max(1.35, min(3.20, 1.06 / prob)), 2)

# =========================================================
# RANKING
# =========================================================

def market_priority(pick_type: str) -> int:
    return {
        "over_2_5": 3,
        "btts_yes": 2,
        "winner": 1,
    }.get(pick_type, 0)

def fit_score_for_odds(odds: float) -> float:
    ideal = 1.80
    return -abs(ideal - odds)

def confidence_from_odds(odds: float, market_type: str, league: str) -> int:
    base_map = {
        "winner": 73,
        "over_2_5": 70,
        "btts_yes": 69,
    }
    base = base_map.get(market_type, 68)

    implied = 100 / odds if odds > 1 else 40
    implied_component = int(round((implied - 40) * 0.55))
    league_bonus = 2 if get_league_priority(league) >= 86 else 0

    conf = base + implied_component + league_bonus
    return max(55, min(conf, 90))

# =========================================================
# TIPSTER
# =========================================================

def tipster_explanation(
    league: str,
    home: str,
    away: str,
    pick_type: str,
    odds: float,
    h2h_data: Dict[str, Any],
    ph: float,
    pd: float,
    pa: float,
    est_btts_odds: float,
    est_over_odds: float,
) -> str:
    home_odds = h2h_data.get("home_odds")
    draw_odds = h2h_data.get("draw_odds")
    away_odds = h2h_data.get("away_odds")

    parts: List[str] = [
        f"{league}: {home} vs {away}.",
        f"Mercado 1X2: 1 en {home_odds}, X en {draw_odds}, 2 en {away_odds}.",
    ]

    if pick_type == "winner":
        fav = home if (home_odds or 99) < (away_odds or 99) else away
        parts.append(
            f"Se prioriza ganador porque {fav} sale favorito claro en cuota principal."
        )
        parts.append(
            f"Probabilidades normalizadas estimadas: local {round(ph*100)}%, empate {round(pd*100)}%, visitante {round(pa*100)}%."
        )

    elif pick_type == "btts_yes":
        parts.append(
            f"Se elige Ambos marcan por perfil relativamente equilibrado y proyección de BTTS estimada alrededor de cuota {est_btts_odds}."
        )
        parts.append(
            f"El reparto 1X2 sugiere opciones para ambos equipos sin un dominio extremo."
        )

    elif pick_type == "over_2_5":
        parts.append(
            f"Se selecciona Más de 2.5 goles por señal ofensiva estimada desde el mercado principal, con proyección cercana a cuota {est_over_odds}."
        )
        parts.append(
            f"El bajo peso relativo del empate favorece un escenario más abierto."
        )

    parts.append(f"Cuota elegida: {odds}.")
    return " ".join(parts)

# =========================================================
# PICK ENGINE
# =========================================================

def build_candidates(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    h2h_data = parse_h2h(event)
    if not h2h_data:
        return []

    home = h2h_data["home_team"]
    away = h2h_data["away_team"]
    league = event["_league"]
    dt_local = event["_dt_local"]

    home_odds = h2h_data.get("home_odds")
    away_odds = h2h_data.get("away_odds")
    draw_odds = h2h_data.get("draw_odds")

    if not home_odds or not away_odds:
        return []

    ph, pd, pa = normalize_three_way_probs(home_odds, draw_odds, away_odds)

    est_over_prob = estimate_over25_prob(ph, pd, pa)
    est_btts_prob = estimate_btts_prob(ph, pd, pa)

    est_over_odds = pseudo_price_from_prob(est_over_prob)
    est_btts_odds = pseudo_price_from_prob(est_btts_prob)

    candidates: List[Dict[str, Any]] = []

    # winner
    fav = home if home_odds < away_odds else away
    fav_odds = min(home_odds, away_odds)

    candidates.append({
        "id": event["id"],
        "match": f"{home} vs {away}",
        "league": league,
        "time_local": dt_local.strftime("%d/%m %H:%M"),
        "kickoff_iso": dt_local.isoformat(),
        "pick": f"Gana {fav}",
        "pick_type": "winner",
        "odds": round(fav_odds, 2),
        "confidence": confidence_from_odds(fav_odds, "winner", league),
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "bookmaker": h2h_data.get("bookmaker", "Bookmaker"),
        "tipster_explanation": tipster_explanation(
            league, home, away, "winner", round(fav_odds, 2), h2h_data, ph, pd, pa, est_btts_odds, est_over_odds
        ),
        "market_snapshot": {
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "btts_yes": est_btts_odds,
            "over_2_5": est_over_odds,
        },
    })

    # btts estimado
    candidates.append({
        "id": event["id"],
        "match": f"{home} vs {away}",
        "league": league,
        "time_local": dt_local.strftime("%d/%m %H:%M"),
        "kickoff_iso": dt_local.isoformat(),
        "pick": "Ambos marcan",
        "pick_type": "btts_yes",
        "odds": est_btts_odds,
        "confidence": confidence_from_odds(est_btts_odds, "btts_yes", league),
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "bookmaker": h2h_data.get("bookmaker", "Bookmaker"),
        "tipster_explanation": tipster_explanation(
            league, home, away, "btts_yes", est_btts_odds, h2h_data, ph, pd, pa, est_btts_odds, est_over_odds
        ),
        "market_snapshot": {
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "btts_yes": est_btts_odds,
            "over_2_5": est_over_odds,
        },
    })

    # over estimado
    candidates.append({
        "id": event["id"],
        "match": f"{home} vs {away}",
        "league": league,
        "time_local": dt_local.strftime("%d/%m %H:%M"),
        "kickoff_iso": dt_local.isoformat(),
        "pick": "Más de 2.5 goles",
        "pick_type": "over_2_5",
        "odds": est_over_odds,
        "confidence": confidence_from_odds(est_over_odds, "over_2_5", league),
        "home_team": home,
        "away_team": away,
        "status": "pending",
        "score_line": "",
        "bookmaker": h2h_data.get("bookmaker", "Bookmaker"),
        "tipster_explanation": tipster_explanation(
            league, home, away, "over_2_5", est_over_odds, h2h_data, ph, pd, pa, est_btts_odds, est_over_odds
        ),
        "market_snapshot": {
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "btts_yes": est_btts_odds,
            "over_2_5": est_over_odds,
        },
    })

    return candidates

def deduplicate_event_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for c in candidates:
        grouped.setdefault(c["id"], []).append(c)

    chosen: List[Dict[str, Any]] = []

    for _, arr in grouped.items():
        arr.sort(
            key=lambda x: (
                market_priority(x["pick_type"]),
                x["confidence"],
                get_league_priority(x["league"]),
                fit_score_for_odds(x["odds"]),
            ),
            reverse=True,
        )
        chosen.append(arr[0])

    return chosen

def select_best_picks(events: List[Dict[str, Any]], league_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    all_candidates: List[Dict[str, Any]] = []

    for event in events:
        all_candidates.extend(build_candidates(event))

    picks = deduplicate_event_candidates(all_candidates)

    if league_filter:
        lf = normalize_text(league_filter)
        picks = [p for p in picks if lf in normalize_text(p.get("league"))]

    picks.sort(
        key=lambda x: (
            get_league_priority(x["league"]),
            x["confidence"],
            market_priority(x["pick_type"]),
            fit_score_for_odds(x["odds"]),
        ),
        reverse=True,
    )

    return picks[:MAX_PICKS]

# =========================================================
# RESULTS / HISTORY
# =========================================================

def score_map_from_scores(scores: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for s in scores:
        name = s.get("name")
        try:
            score = int(s.get("score"))
        except Exception:
            continue
        if name:
            out[name] = score
    return out

def resolve_pick_result(pick: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    smap = score_map_from_scores(scores)

    home = pick["home_team"]
    away = pick["away_team"]

    if home not in smap or away not in smap:
        return "pending"

    h = smap[home]
    a = smap[away]
    total = h + a

    if pick["pick_type"] == "winner":
        target = pick["pick"].replace("Gana ", "").strip()
        if h > a:
            return "won" if target == home else "lost"
        if a > h:
            return "won" if target == away else "lost"
        return "lost"

    if pick["pick_type"] == "btts_yes":
        return "won" if h > 0 and a > 0 else "lost"

    if pick["pick_type"] == "over_2_5":
        return "won" if total >= 3 else "lost"

    return "pending"

def build_score_line(pick: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    smap = score_map_from_scores(scores)
    home = pick["home_team"]
    away = pick["away_team"]
    if home in smap and away in smap:
        return f"{smap[home]}-{smap[away]}"
    return ""

def update_results(history: Dict[str, Any]) -> Dict[str, Any]:
    history.setdefault("days", {})

    for sport_key in SPORTS.keys():
        try:
            scores = api(
                f"/v4/sports/{sport_key}/scores",
                {
                    "daysFrom": SCORES_DAYS_BACK,
                    "dateFormat": "iso",
                },
            )
        except Exception:
            continue

        scores_by_id = {s.get("id"): s for s in scores if s.get("id")}

        for _, day_data in history.get("days", {}).items():
            for pick in day_data.get("picks", []):
                match_score = scores_by_id.get(pick.get("id"))
                if not match_score:
                    continue

                if match_score.get("completed"):
                    pick["status"] = resolve_pick_result(pick, match_score.get("scores", []))
                    pick["score_line"] = build_score_line(pick, match_score.get("scores", []))

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
    history[today_key()] = history.get(today_key(), {})
    history["days"][today_key()] = {"picks": picks}

    history = update_results(history)
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
# BUILD PAYLOAD
# =========================================================

def build_payload(league: Optional[str] = None) -> Dict[str, Any]:
    events = get_events_window()
    picks = select_best_picks(events, league_filter=league)

    history = read_json(HISTORY_FILE)
    history = merge_today_history(history, picks)
    write_json(HISTORY_FILE, history)

    payload = {
        "generated_at": now_local().isoformat(),
        "cache_day": today_key(),
        "lookahead_hours": LOOKAHEAD_HOURS,
        "count": len(picks),
        "picks": picks,
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

@app.get("/")
def root():
    return {
        "ok": True,
        "name": "Top Picks Pro API - Optimized",
        "endpoints": [
            "/api/picks",
            "/api/picks?force_refresh=true",
            "/api/picks?league=LaLiga",
            "/api/history",
        ],
    }

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
        }

    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando The Odds API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.get("/api/history")
def get_history():
    try:
        raw = read_json(HISTORY_FILE)
        raw = update_results(raw)
        raw = rebuild_history_stats(raw)
        raw = trim_history(raw)
        write_json(HISTORY_FILE, raw)
        return history_to_frontend(raw)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# =========================================================
# LOCAL RUN
# =========================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)