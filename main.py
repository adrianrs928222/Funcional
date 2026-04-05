import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
BASE_URL = "https://api.the-odds-api.com"
TZ = pytz.timezone("Europe/Madrid")

if not ODDS_API_KEY:
    raise RuntimeError("Falta ODDS_API_KEY")

app = FastAPI(title="Top Picks Pro API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CACHE_FILE = "cache.json"
HISTORY_FILE = "history.json"

# Cache real: 24 horas
CACHE_TTL_HOURS = 24

# Máximo picks a devolver
MAX_PICKS = 6

# Ventana máxima de búsqueda: hoy hasta 23:59 Madrid
LOOKAHEAD_HOURS = 24

# Ligas objetivo
SPORTS: Dict[str, str] = {
    # UEFA / internacionales
    "soccer_uefa_champs_league": "Champions League",
    "soccer_uefa_europa_league": "Europa League",
    "soccer_uefa_europa_conference_league": "Conference League",
    "soccer_fifa_world_cup": "Mundial",
    "soccer_uefa_european_championship": "Eurocopa",

    # España
    "soccer_spain_la_liga": "LaLiga",
    "soccer_spain_segunda_division": "LaLiga Hypermotion",

    # Inglaterra
    "soccer_epl": "Premier League",
    "soccer_efl_champ": "Championship",

    # Italia
    "soccer_italy_serie_a": "Serie A",
    "soccer_italy_serie_b": "Serie B",

    # Alemania
    "soccer_germany_bundesliga": "Bundesliga",
    "soccer_germany_bundesliga2": "2. Bundesliga",

    # Países Bajos
    "soccer_netherlands_eredivisie": "Eredivisie",

    # Francia
    "soccer_france_ligue_one": "Ligue 1",
    "soccer_france_ligue_two": "Ligue 2",

    # Portugal
    "soccer_portugal_primeira_liga": "Primeira Liga",

    # Bélgica / Escocia / Turquía / Brasil / Argentina
    "soccer_belgium_first_div": "Belgian Pro League",
    "soccer_spl": "Scottish Premiership",
    "soccer_turkey_super_league": "Super Lig",
    "soccer_brazil_campeonato": "Brasileirão",
    "soccer_argentina_primera_division": "Primera División Argentina",
}

# Prioridad de ligas para ordenar
LEAGUE_PRIORITY: Dict[str, int] = {
    "Champions League": 100,
    "Europa League": 95,
    "Conference League": 90,
    "Mundial": 88,
    "Eurocopa": 86,

    "LaLiga": 85,
    "Premier League": 84,
    "Serie A": 83,
    "Bundesliga": 82,
    "Eredivisie": 81,
    "Ligue 1": 80,
    "Primeira Liga": 79,

    "LaLiga Hypermotion": 76,
    "Championship": 75,
    "Serie B": 74,
    "2. Bundesliga": 73,
    "Ligue 2": 72,

    "Brasileirão": 70,
    "Primera División Argentina": 69,
    "Belgian Pro League": 68,
    "Scottish Premiership": 67,
    "Super Lig": 66,
}


# =========================================================
# Helpers generales
# =========================================================

def now() -> datetime:
    return datetime.now(TZ)


def madrid_today_str() -> str:
    return now().strftime("%Y-%m-%d")


def api(path: str, params: Dict[str, Any]) -> Any:
    params = dict(params)
    params["apiKey"] = ODDS_API_KEY
    r = requests.get(BASE_URL + path, params=params, timeout=25)
    r.raise_for_status()
    return r.json()


def load_json(file_path: str) -> Any:
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_json(file_path: str, data: Any) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_event_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ)


def is_today_in_madrid(dt: datetime) -> bool:
    return dt.date() == now().date()


def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    generated_at = cache.get("generated_at")
    if not generated_at:
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    return now() - dt < timedelta(hours=CACHE_TTL_HOURS)


def normalize_league_filter(league: Optional[str]) -> str:
    return (league or "").strip().lower()


def safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def get_league_priority(league_name: str) -> int:
    return LEAGUE_PRIORITY.get(league_name, 0)


# =========================================================
# Fetch de eventos
# =========================================================

def get_events_today() -> List[Dict[str, Any]]:
    """
    Coge únicamente partidos de HOY en horario Madrid,
    y solo de las ligas incluidas en SPORTS.
    """
    all_events: List[Dict[str, Any]] = []

    for sport_key, league_name in SPORTS.items():
        try:
            data = api(
                f"/v4/sports/{sport_key}/odds",
                {
                    "regions": "eu",
                    "markets": "h2h,btts,totals",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                },
            )

            for event in data:
                try:
                    dt_local = parse_event_dt(event["commence_time"])
                except Exception:
                    continue

                if not is_today_in_madrid(dt_local):
                    continue

                event["_sport"] = sport_key
                event["_league"] = league_name
                event["_dt_local"] = dt_local
                all_events.append(event)

        except Exception:
            # Si una liga falla, seguimos con las demás
            continue

    return all_events


# =========================================================
# Parseo de mercados
# =========================================================

def get_best_bookmaker(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Elige el primer bookmaker disponible con mercados útiles.
    """
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    # Preferimos uno que tenga más mercados
    scored = []
    for b in bookmakers:
        keys = {m.get("key") for m in b.get("markets", [])}
        score = 0
        if "h2h" in keys:
            score += 3
        if "btts" in keys:
            score += 2
        if "totals" in keys:
            score += 2
        scored.append((score, b))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1] if scored else bookmakers[0]


def get_market(bookmaker: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    for m in bookmaker.get("markets", []):
        if m.get("key") == key:
            return m
    return None


def parse_h2h(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = get_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "h2h")
    if not market:
        return None

    home = event["home_team"]
    away = next((t for t in event["teams"] if t != home), None)
    if not away:
        return None

    outcomes = market.get("outcomes", [])
    odds_map = {o.get("name"): safe_float(o.get("price")) for o in outcomes}

    return {
        "home_team": home,
        "away_team": away,
        "home_odds": odds_map.get(home),
        "away_odds": odds_map.get(away),
        "draw_odds": odds_map.get("Draw"),
        "bookmaker": bookmaker.get("title", "Bookmaker"),
    }


def parse_btts(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = get_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "btts")
    if not market:
        return None

    yes_price = None
    no_price = None

    for o in market.get("outcomes", []):
        name = str(o.get("name", "")).strip().lower()
        price = safe_float(o.get("price"))
        if name == "yes":
            yes_price = price
        elif name == "no":
            no_price = price

    if yes_price is None and no_price is None:
        return None

    return {
        "yes": yes_price,
        "no": no_price,
        "bookmaker": bookmaker.get("title", "Bookmaker"),
    }


def parse_over25(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = get_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "totals")
    if not market:
        return None

    over = None
    under = None

    for o in market.get("outcomes", []):
        name = str(o.get("name", "")).strip().lower()
        point = safe_float(o.get("point"))
        price = safe_float(o.get("price"))

        if point == 2.5:
            if "over" in name:
                over = price
            elif "under" in name:
                under = price

    if over is None and under is None:
        return None

    return {
        "over_2_5": over,
        "under_2_5": under,
        "bookmaker": bookmaker.get("title", "Bookmaker"),
    }


# =========================================================
# Lógica de pick
# =========================================================

def implied_probability(decimal_odds: Optional[float]) -> Optional[float]:
    if not decimal_odds or decimal_odds <= 1:
        return None
    return round(100 / decimal_odds, 2)


def confidence_from_odds(odds: float, market_type: str, league: str) -> int:
    """
    Heurística simple para dar una confianza consistente.
    """
    base = 0

    if market_type == "winner":
        base = 74
    elif market_type == "btts_yes":
        base = 70
    elif market_type == "over_2_5":
        base = 71
    else:
        base = 68

    # Cuanto más baja la cuota, más confianza
    odds_adjust = int(round((2.20 - odds) * 12))
    league_adjust = 2 if get_league_priority(league) >= 80 else 0

    conf = base + odds_adjust + league_adjust
    return max(55, min(conf, 89))


def tipster_explanation(
    league: str,
    home: str,
    away: str,
    pick_type: str,
    odds: float,
    h2h_data: Optional[Dict[str, Any]],
    btts_data: Optional[Dict[str, Any]],
    over_data: Optional[Dict[str, Any]],
) -> str:
    parts: List[str] = []

    if pick_type == "winner" and h2h_data:
        home_odds = h2h_data.get("home_odds")
        away_odds = h2h_data.get("away_odds")
        draw_odds = h2h_data.get("draw_odds")

        parts.append(
            f"En {league}, el mercado 1X2 marca favorito claro."
        )

        if home_odds and away_odds:
            parts.append(
                f"Cuotas principales: {home} {home_odds}, {away} {away_odds}"
                + (f", empate {draw_odds}" if draw_odds else "")
                + "."
            )

        parts.append(
            "Se elige ganador porque es la línea con mejor ventaja relativa y menor dispersión."
        )

    elif pick_type == "btts_yes" and btts_data:
        yes = btts_data.get("yes")
        no = btts_data.get("no")

        parts.append(
            f"En {league}, el mercado de ambos marcan presenta precio competitivo."
        )

        if yes:
            parts.append(
                f"BTTS Sí está en {yes}"
                + (f" frente a BTTS No en {no}" if no else "")
                + "."
            )

        if over_data and over_data.get("over_2_5"):
            parts.append(
                f"Además, el Over 2.5 acompaña en {over_data['over_2_5']}, señal de partido abierto."
            )

        parts.append(
            "Se prioriza BTTS porque ofrece equilibrio entre probabilidad y cuota."
        )

    elif pick_type == "over_2_5" and over_data:
        over = over_data.get("over_2_5")
        under = over_data.get("under_2_5")

        parts.append(
            f"En {league}, el mercado de goles favorece el +2.5."
        )

        if over:
            parts.append(
                f"Over 2.5 en {over}"
                + (f" frente a Under 2.5 en {under}" if under else "")
                + "."
            )

        if btts_data and btts_data.get("yes"):
            parts.append(
                f"El BTTS Sí también aparece en {btts_data['yes']}, reforzando un escenario con intercambio de goles."
            )

        parts.append(
            "Se selecciona Over 2.5 por perfil de cuota sólida y contexto ofensivo del mercado."
        )

    else:
        parts.append("Pick seleccionado por mejor relación cuota/probabilidad disponible.")

    return " ".join(parts).strip()


def build_candidates(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Genera candidatos de mercados para el evento.
    """
    h2h_data = parse_h2h(event)
    btts_data = parse_btts(event)
    over_data = parse_over25(event)

    if not h2h_data:
        return []

    home = h2h_data["home_team"]
    away = h2h_data["away_team"]
    league = event["_league"]
    dt_local = event["_dt_local"]

    candidates: List[Dict[str, Any]] = []

    # ---------- Winner ----------
    home_odds = h2h_data.get("home_odds")
    away_odds = h2h_data.get("away_odds")
    draw_odds = h2h_data.get("draw_odds")

    if home_odds and away_odds:
        fav = home if home_odds < away_odds else away
        fav_odds = min(home_odds, away_odds)

        # Evitamos picks absurdamente bajos o muy largos
        if 1.30 <= fav_odds <= 2.10:
            conf = confidence_from_odds(fav_odds, "winner", league)
            candidates.append({
                "id": event["id"],
                "match": f"{home} vs {away}",
                "league": league,
                "time_local": dt_local.strftime("%H:%M"),
                "kickoff_iso": dt_local.isoformat(),
                "pick": f"Gana {fav}",
                "pick_type": "winner",
                "odds": round(fav_odds, 2),
                "confidence": conf,
                "home_team": home,
                "away_team": away,
                "status": "pending",
                "score_line": "",
                "bookmaker": h2h_data.get("bookmaker", "Bookmaker"),
                "tipster_explanation": tipster_explanation(
                    league, home, away, "winner", fav_odds, h2h_data, btts_data, over_data
                ),
                "market_snapshot": {
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "btts_yes": btts_data.get("yes") if btts_data else None,
                    "over_2_5": over_data.get("over_2_5") if over_data else None,
                }
            })

    # ---------- BTTS ----------
    if btts_data and btts_data.get("yes"):
        btts_yes = btts_data["yes"]
        if 1.45 <= btts_yes <= 2.05:
            conf = confidence_from_odds(btts_yes, "btts_yes", league)
            candidates.append({
                "id": event["id"],
                "match": f"{home} vs {away}",
                "league": league,
                "time_local": dt_local.strftime("%H:%M"),
                "kickoff_iso": dt_local.isoformat(),
                "pick": "Ambos marcan",
                "pick_type": "btts_yes",
                "odds": round(btts_yes, 2),
                "confidence": conf,
                "home_team": home,
                "away_team": away,
                "status": "pending",
                "score_line": "",
                "bookmaker": btts_data.get("bookmaker", "Bookmaker"),
                "tipster_explanation": tipster_explanation(
                    league, home, away, "btts_yes", btts_yes, h2h_data, btts_data, over_data
                ),
                "market_snapshot": {
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "btts_yes": btts_yes,
                    "btts_no": btts_data.get("no"),
                    "over_2_5": over_data.get("over_2_5") if over_data else None,
                }
            })

    # ---------- Over 2.5 ----------
    if over_data and over_data.get("over_2_5"):
        over_25 = over_data["over_2_5"]
        if 1.50 <= over_25 <= 2.05:
            conf = confidence_from_odds(over_25, "over_2_5", league)
            candidates.append({
                "id": event["id"],
                "match": f"{home} vs {away}",
                "league": league,
                "time_local": dt_local.strftime("%H:%M"),
                "kickoff_iso": dt_local.isoformat(),
                "pick": "Más de 2.5 goles",
                "pick_type": "over_2_5",
                "odds": round(over_25, 2),
                "confidence": conf,
                "home_team": home,
                "away_team": away,
                "status": "pending",
                "score_line": "",
                "bookmaker": over_data.get("bookmaker", "Bookmaker"),
                "tipster_explanation": tipster_explanation(
                    league, home, away, "over_2_5", over_25, h2h_data, btts_data, over_data
                ),
                "market_snapshot": {
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "btts_yes": btts_data.get("yes") if btts_data else None,
                    "over_2_5": over_25,
                    "under_2_5": over_data.get("under_2_5"),
                }
            })

    return candidates


def deduplicate_event_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Si un mismo partido genera varios mercados, nos quedamos con el mejor por confianza,
    y en empate por mejor cuota/mercado.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for c in candidates:
        grouped.setdefault(c["id"], []).append(c)

    chosen: List[Dict[str, Any]] = []

    for _, arr in grouped.items():
        arr.sort(
            key=lambda x: (
                x["confidence"],
                get_league_priority(x["league"]),
                -abs(1.75 - x["odds"])  # preferencia a cuotas cercanas a 1.75
            ),
            reverse=True,
        )
        chosen.append(arr[0])

    return chosen


def select_best_picks(events: List[Dict[str, Any]], league_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    all_candidates: List[Dict[str, Any]] = []

    for event in events:
        event_candidates = build_candidates(event)
        all_candidates.extend(event_candidates)

    picks = deduplicate_event_candidates(all_candidates)

    league_filter_norm = normalize_league_filter(league_filter)
    if league_filter_norm:
        picks = [p for p in picks if league_filter_norm in p["league"].lower()]

    picks.sort(
        key=lambda x: (
            get_league_priority(x["league"]),
            x["confidence"],
            -abs(1.75 - x["odds"])
        ),
        reverse=True,
    )

    return picks[:MAX_PICKS]


# =========================================================
# Resultados e historial
# =========================================================

def resolve_pick_result(pick: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    """
    scores esperado tipo:
    [
      {"name": "Equipo A", "score": "2"},
      {"name": "Equipo B", "score": "1"}
    ]
    """
    try:
        score_map = {s["name"]: int(s["score"]) for s in scores}
    except Exception:
        return "pending"

    home_team = pick["home_team"]
    away_team = pick["away_team"]

    if home_team not in score_map or away_team not in score_map:
        return "pending"

    h = score_map[home_team]
    a = score_map[away_team]
    total = h + a

    if pick["pick_type"] == "winner":
        target = pick["pick"].replace("Gana ", "").strip()
        winner = None
        if h > a:
            winner = home_team
        elif a > h:
            winner = away_team
        else:
            return "lost"
        return "won" if winner == target else "lost"

    if pick["pick_type"] == "over_2_5":
        return "won" if total >= 3 else "lost"

    if pick["pick_type"] == "btts_yes":
        return "won" if h > 0 and a > 0 else "lost"

    return "pending"


def score_line_from_scores(pick: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    try:
        score_map = {s["name"]: int(s["score"]) for s in scores}
        h = score_map[pick["home_team"]]
        a = score_map[pick["away_team"]]
        return f"{h}-{a}"
    except Exception:
        return ""


def update_results(history: Dict[str, Any]) -> Dict[str, Any]:
    if not history or "days" not in history:
        return {"days": {}}

    for sport_key in SPORTS.keys():
        try:
            scores = api(
                f"/v4/sports/{sport_key}/scores",
                {
                    "daysFrom": 3,
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
                    result = resolve_pick_result(pick, match_score.get("scores", []))
                    pick["status"] = result
                    pick["score_line"] = score_line_from_scores(pick, match_score.get("scores", []))

    return history


def rebuild_day_stats(history: Dict[str, Any]) -> Dict[str, Any]:
    for _, day_data in history.get("days", {}).items():
        picks = day_data.get("picks", [])
        won = sum(1 for p in picks if p.get("status") == "won")
        lost = sum(1 for p in picks if p.get("status") == "lost")
        pending = sum(1 for p in picks if p.get("status") == "pending")
        day_data["stats"] = {
            "won": won,
            "lost": lost,
            "pending": pending,
        }
    return history


def merge_today_history(history: Dict[str, Any], today_picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    history.setdefault("days", {})
    today_key = madrid_today_str()

    # Sobrescribe picks del día actual para mantenerlos limpios
    history["days"][today_key] = {
        "picks": today_picks
    }

    history = update_results(history)
    history = rebuild_day_stats(history)
    return history


def history_as_frontend_array(history: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte:
    { "days": { "2026-04-05": {...} } }
    a:
    { "days": [ {date:..., stats:..., picks:[...]}, ... ] }
    para que tu frontend lo pinte bien.
    """
    days_obj = history.get("days", {})
    out = []

    for day, data in sorted(days_obj.items(), reverse=True):
        out.append({
            "date": day,
            "stats": data.get("stats", {"won": 0, "lost": 0, "pending": 0}),
            "picks": data.get("picks", []),
        })

    return {"days": out}


# =========================================================
# Cache
# =========================================================

def build_and_store_payload() -> Dict[str, Any]:
    events = get_events_today()
    picks = select_best_picks(events)

    history = load_json(HISTORY_FILE)
    history = merge_today_history(history, picks)
    save_json(HISTORY_FILE, history)

    payload = {
        "cache_day": madrid_today_str(),
        "generated_at": now().isoformat(),
        "count": len(picks),
        "picks": picks,
    }

    save_json(CACHE_FILE, payload)
    return payload


def get_cached_or_refresh(force_refresh: bool = False) -> Dict[str, Any]:
    cache = load_json(CACHE_FILE)

    if not force_refresh and cache and cache_is_valid(cache):
        return cache

    return build_and_store_payload()


# =========================================================
# API
# =========================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "name": "Top Picks Pro API",
        "endpoints": [
            "/api/picks",
            "/api/picks?force_refresh=true",
            "/api/picks?league=LaLiga",
            "/api/history",
        ],
    }


@app.get("/api/picks")
def picks(
    force_refresh: bool = Query(False),
    league: Optional[str] = Query(None),
):
    try:
        payload = get_cached_or_refresh(force_refresh=force_refresh)

        picks_data = payload.get("picks", [])
        if league:
            league_norm = normalize_league_filter(league)
            picks_data = [p for p in picks_data if league_norm in p.get("league", "").lower()]

        return {
            "cache_day": payload.get("cache_day"),
            "generated_at": payload.get("generated_at"),
            "count": len(picks_data),
            "picks": picks_data,
        }

    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando The Odds API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get("/api/history")
def history():
    raw = load_json(HISTORY_FILE)
    raw = update_results(raw)
    raw = rebuild_day_stats(raw)
    save_json(HISTORY_FILE, raw)
    return history_as_frontend_array(raw)