import json
import os
from datetime import datetime, timedelta
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
MAX_PICKS = 12

# Si hoy no encuentra nada en estas ligas o mercados, no se rompe:
# simplemente sigue con las que sí respondan.
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

    # Más ligas útiles
    "soccer_belgium_first_div": "Belgian Pro League",
    "soccer_spl": "Scottish Premiership",
    "soccer_turkey_super_league": "Super Lig",
    "soccer_brazil_campeonato": "Brasileirão",
    "soccer_argentina_primera_division": "Primera División Argentina",
}

LEAGUE_PRIORITY: Dict[str, int] = {
    "Champions League": 100,
    "Europa League": 95,
    "Conference League": 92,
    "Mundial": 90,
    "Eurocopa": 88,

    "LaLiga": 86,
    "Premier League": 85,
    "Serie A": 84,
    "Bundesliga": 83,
    "Eredivisie": 82,
    "Ligue 1": 81,
    "Primeira Liga": 80,

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

app = FastAPI(title="Top Picks Pro API")

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

def now() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return now().strftime("%Y-%m-%d")


def parse_iso_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ)


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def safe_float(v: Any) -> Optional[float]:
    try:
        n = float(v)
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
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_league_priority(name: str) -> int:
    return LEAGUE_PRIORITY.get(name, 0)


def cache_is_valid(cache: Dict[str, Any]) -> bool:
    if not cache:
        return False

    generated_at = cache.get("generated_at")
    if not generated_at:
        return False

    # Cache vacía no sirve
    if not cache.get("picks"):
        return False

    try:
        dt = datetime.fromisoformat(generated_at)
    except Exception:
        return False

    return (now() - dt) < timedelta(hours=CACHE_TTL_HOURS)


def api(path: str, params: Dict[str, Any]) -> Any:
    q = dict(params)
    q["apiKey"] = ODDS_API_KEY

    r = requests.get(BASE_URL + path, params=q, timeout=25)
    r.raise_for_status()
    return r.json()


# =========================================================
# FETCH
# =========================================================

def get_events_today() -> List[Dict[str, Any]]:
    """
    Solo partidos del día de hoy en horario Madrid.
    No rompe si alguna liga falla.
    """
    events: List[Dict[str, Any]] = []

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
        except Exception:
            continue

        for e in data:
            try:
                dt_local = parse_iso_dt(e["commence_time"])
            except Exception:
                continue

            # Solo HOY en Madrid
            if dt_local.date() != now().date():
                continue

            e["_sport"] = sport_key
            e["_league"] = league_name
            e["_dt_local"] = dt_local
            events.append(e)

    return events


# =========================================================
# MARKET PARSERS
# =========================================================

def get_market(bookmaker: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    for m in bookmaker.get("markets", []):
        if m.get("key") == key:
            return m
    return None


def pick_best_bookmaker(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    ranked = []
    for b in bookmakers:
        market_keys = {m.get("key") for m in b.get("markets", [])}
        score = 0
        if "h2h" in market_keys:
            score += 5
        if "totals" in market_keys:
            score += 3
        if "btts" in market_keys:
            score += 2
        ranked.append((score, b))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1] if ranked else bookmakers[0]


def parse_h2h(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = pick_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "h2h")
    if not market:
        return None

    home = event.get("home_team")
    teams = event.get("teams", [])
    away = next((t for t in teams if t != home), None)

    if not home or not away:
        return None

    outcomes = market.get("outcomes", [])
    odds_map = {o.get("name"): safe_float(o.get("price")) for o in outcomes}

    return {
        "bookmaker": bookmaker.get("title", "Bookmaker"),
        "home_team": home,
        "away_team": away,
        "home_odds": odds_map.get(home),
        "away_odds": odds_map.get(away),
        "draw_odds": odds_map.get("Draw"),
    }


def parse_btts(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = pick_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "btts")
    if not market:
        return None

    yes_price = None
    no_price = None

    for o in market.get("outcomes", []):
        name = normalize_text(o.get("name"))
        price = safe_float(o.get("price"))
        if name == "yes":
            yes_price = price
        elif name == "no":
            no_price = price

    if yes_price is None and no_price is None:
        return None

    return {
        "bookmaker": bookmaker.get("title", "Bookmaker"),
        "yes": yes_price,
        "no": no_price,
    }


def parse_over25(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bookmaker = pick_best_bookmaker(event)
    if not bookmaker:
        return None

    market = get_market(bookmaker, "totals")
    if not market:
        return None

    over_25 = None
    under_25 = None

    for o in market.get("outcomes", []):
        name = normalize_text(o.get("name"))
        point = safe_float(o.get("point"))
        price = safe_float(o.get("price"))

        if point == 2.5:
            if "over" in name:
                over_25 = price
            elif "under" in name:
                under_25 = price

    if over_25 is None and under_25 is None:
        return None

    return {
        "bookmaker": bookmaker.get("title", "Bookmaker"),
        "over_2_5": over_25,
        "under_2_5": under_25,
    }


# =========================================================
# SCORING / RANKING
# =========================================================

def confidence_from_odds(odds: float, market_type: str, league: str) -> int:
    """
    No descarta por cuota.
    Solo estima confianza de forma razonable.
    """
    base_map = {
        "winner": 73,
        "over_2_5": 70,
        "btts_yes": 69,
    }
    base = base_map.get(market_type, 68)

    # Menor cuota = mayor probabilidad implícita
    implied = 100 / odds if odds > 1 else 40
    implied_component = int(round((implied - 40) * 0.55))

    league_bonus = 2 if get_league_priority(league) >= 80 else 0

    conf = base + implied_component + league_bonus
    return max(55, min(conf, 90))


def market_priority(pick_type: str) -> int:
    # Mi orden:
    # 1) Over 2.5
    # 2) BTTS
    # 3) Winner
    return {
        "over_2_5": 3,
        "btts_yes": 2,
        "winner": 1,
    }.get(pick_type, 0)


def fit_score_for_odds(odds: float) -> float:
    """
    Preferencia por cuotas útiles para picks visibles.
    Ideal cerca de 1.65-1.95, pero no se descarta ninguna.
    """
    ideal = 1.80
    return -abs(ideal - odds)


# =========================================================
# TIPSTER EXPLANATION
# =========================================================

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

    parts.append(f"{league}: {home} vs {away}.")

    if h2h_data:
        h = h2h_data.get("home_odds")
        d = h2h_data.get("draw_odds")
        a = h2h_data.get("away_odds")

        h2h_bits = []
        if h:
            h2h_bits.append(f"1 en {h}")
        if d:
            h2h_bits.append(f"X en {d}")
        if a:
            h2h_bits.append(f"2 en {a}")

        if h2h_bits:
            parts.append("Mercado 1X2: " + ", ".join(h2h_bits) + ".")

    if pick_type == "winner":
        fav = None
        if h2h_data:
            ho = h2h_data.get("home_odds")
            ao = h2h_data.get("away_odds")
            if ho and ao:
                fav = home if ho < ao else away

        if fav:
            parts.append(
                f"El pick va con {fav} porque sale como favorito por cuota en el mercado principal."
            )
        else:
            parts.append(
                "Se prioriza ganador porque es el mercado más estable disponible en este partido."
            )

        if over_data and over_data.get("over_2_5"):
            parts.append(f"El +2.5 aparece en {over_data['over_2_5']}.")
        if btts_data and btts_data.get("yes"):
            parts.append(f"BTTS Sí aparece en {btts_data['yes']}.")

    elif pick_type == "btts_yes":
        yes = btts_data.get("yes") if btts_data else None
        no = btts_data.get("no") if btts_data else None

        if yes:
            txt = f"BTTS Sí en {yes}"
            if no:
                txt += f" frente a BTTS No en {no}"
            parts.append(txt + ".")

        if over_data and over_data.get("over_2_5"):
            parts.append(
                f"Además, el Over 2.5 está en {over_data['over_2_5']}, reforzando escenario de goles de ambos lados."
            )
        else:
            parts.append(
                "Se elige ambos marcan por mejor relación entre probabilidad implícita y precio."
            )

    elif pick_type == "over_2_5":
        over = over_data.get("over_2_5") if over_data else None
        under = over_data.get("under_2_5") if over_data else None

        if over:
            txt = f"Over 2.5 en {over}"
            if under:
                txt += f" frente a Under 2.5 en {under}"
            parts.append(txt + ".")

        if btts_data and btts_data.get("yes"):
            parts.append(
                f"El BTTS Sí en {btts_data['yes']} acompaña un perfil de partido abierto."
            )
        else:
            parts.append(
                "Se selecciona +2.5 porque el mercado de goles ofrece la señal más interesante del encuentro."
            )

    parts.append(f"Cuota elegida: {odds}.")
    return " ".join(parts)


# =========================================================
# CANDIDATES
# =========================================================

def build_candidates(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    No descarta por rango de cuotas.
    Si hay H2H válido, el partido ya puede generar pick.
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

    home_odds = h2h_data.get("home_odds")
    away_odds = h2h_data.get("away_odds")
    draw_odds = h2h_data.get("draw_odds")

    candidates: List[Dict[str, Any]] = []

    # winner
    if home_odds and away_odds and home_odds > 1 and away_odds > 1:
        fav = home if home_odds < away_odds else away
        fav_odds = min(home_odds, away_odds)

        candidates.append({
            "id": event["id"],
            "match": f"{home} vs {away}",
            "league": league,
            "time_local": dt_local.strftime("%H:%M"),
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
                league, home, away, "winner", fav_odds, h2h_data, btts_data, over_data
            ),
            "market_snapshot": {
                "home_odds": home_odds,
                "draw_odds": draw_odds,
                "away_odds": away_odds,
                "btts_yes": btts_data.get("yes") if btts_data else None,
                "over_2_5": over_data.get("over_2_5") if over_data else None,
            }
        })

    # btts
    if btts_data and btts_data.get("yes") and btts_data["yes"] > 1:
        btts_yes = btts_data["yes"]

        candidates.append({
            "id": event["id"],
            "match": f"{home} vs {away}",
            "league": league,
            "time_local": dt_local.strftime("%H:%M"),
            "kickoff_iso": dt_local.isoformat(),
            "pick": "Ambos marcan",
            "pick_type": "btts_yes",
            "odds": round(btts_yes, 2),
            "confidence": confidence_from_odds(btts_yes, "btts_yes", league),
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
                "draw_odds": draw_odds,
                "away_odds": away_odds,
                "btts_yes": btts_yes,
                "btts_no": btts_data.get("no"),
                "over_2_5": over_data.get("over_2_5") if over_data else None,
            }
        })

    # over 2.5
    if over_data and over_data.get("over_2_5") and over_data["over_2_5"] > 1:
        over_25 = over_data["over_2_5"]

        candidates.append({
            "id": event["id"],
            "match": f"{home} vs {away}",
            "league": league,
            "time_local": dt_local.strftime("%H:%M"),
            "kickoff_iso": dt_local.isoformat(),
            "pick": "Más de 2.5 goles",
            "pick_type": "over_2_5",
            "odds": round(over_25, 2),
            "confidence": confidence_from_odds(over_25, "over_2_5", league),
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
                "draw_odds": draw_odds,
                "away_odds": away_odds,
                "btts_yes": btts_data.get("yes") if btts_data else None,
                "over_2_5": over_25,
                "under_2_5": over_data.get("under_2_5"),
            }
        })

    return candidates


def deduplicate_event_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Un partido = un pick.
    Se queda con el mejor mercado del partido.
    """
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


def fallback_winner_from_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Fallback duro: si existe H2H, genera sí o sí pick ganador.
    """
    h2h_data = parse_h2h(event)
    if not h2h_data:
        return None

    home = h2h_data["home_team"]
    away = h2h_data["away_team"]
    home_odds = h2h_data.get("home_odds")
    away_odds = h2h_data.get("away_odds")
    draw_odds = h2h_data.get("draw_odds")

    if not home_odds or not away_odds:
        return None

    fav = home if home_odds < away_odds else away
    fav_odds = min(home_odds, away_odds)
    league = event["_league"]
    dt_local = event["_dt_local"]

    return {
        "id": event["id"],
        "match": f"{home} vs {away}",
        "league": league,
        "time_local": dt_local.strftime("%H:%M"),
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
        "tipster_explanation": (
            f"{league}: {home} vs {away}. "
            f"Fallback del modelo: se juega ganador porque {fav} sale favorito en la cuota principal. "
            f"Mercado 1X2 con {home} en {home_odds}, empate en {draw_odds}, {away} en {away_odds}. "
            f"Cuota elegida: {round(fav_odds, 2)}."
        ),
        "market_snapshot": {
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
        }
    }


def select_best_picks(events: List[Dict[str, Any]], league_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    all_candidates: List[Dict[str, Any]] = []

    for event in events:
        all_candidates.extend(build_candidates(event))

    picks = deduplicate_event_candidates(all_candidates)

    # Si no hay picks por mercados avanzados, hacemos fallback a winner
    if not picks:
        fallback_picks: List[Dict[str, Any]] = []
        for event in events:
            p = fallback_winner_from_event(event)
            if p:
                fallback_picks.append(p)
        picks = fallback_picks

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

def resolve_pick_result(pick: Dict[str, Any], scores: List[Dict[str, Any]]) -> str:
    try:
        score_map = {s["name"]: int(s["score"]) for s in scores}
    except Exception:
        return "pending"

    home = pick["home_team"]
    away = pick["away_team"]

    if home not in score_map or away not in score_map:
        return "pending"

    h = score_map[home]
    a = score_map[away]
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
    try:
        score_map = {s["name"]: int(s["score"]) for s in scores}
        return f"{score_map[pick['home_team']]}-{score_map[pick['away_team']]}"
    except Exception:
        return ""


def update_results(history: Dict[str, Any]) -> Dict[str, Any]:
    history.setdefault("days", {})

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
            for p in day_data.get("picks", []):
                match_score = scores_by_id.get(p.get("id"))
                if not match_score:
                    continue

                if match_score.get("completed"):
                    p["status"] = resolve_pick_result(p, match_score.get("scores", []))
                    p["score_line"] = build_score_line(p, match_score.get("scores", []))

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


def merge_today_history(history: Dict[str, Any], picks: List[Dict[str, Any]]) -> Dict[str, Any]:
    history.setdefault("days", {})
    history["days"][today_str()] = {"picks": picks}

    history = update_results(history)
    history = rebuild_history_stats(history)
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

def build_payload() -> Dict[str, Any]:
    events = get_events_today()
    picks = select_best_picks(events)

    history = read_json(HISTORY_FILE)
    history = merge_today_history(history, picks)
    write_json(HISTORY_FILE, history)

    payload = {
        "cache_day": today_str(),
        "generated_at": now().isoformat(),
        "count": len(picks),
        "picks": picks,
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
def get_picks(
    force_refresh: bool = Query(False),
    league: Optional[str] = Query(None),
):
    try:
        payload = get_cached_or_refresh(force_refresh=force_refresh)

        picks = payload.get("picks", [])
        if league:
            lf = normalize_text(league)
            picks = [p for p in picks if lf in normalize_text(p.get("league"))]

        return {
            "cache_day": payload.get("cache_day"),
            "generated_at": payload.get("generated_at"),
            "count": len(picks),
            "picks": picks,
        }

    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Error consultando The Odds API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get("/api/history")
def get_history():
    raw = read_json(HISTORY_FILE)
    raw = update_results(raw)
    raw = rebuild_history_stats(raw)
    write_json(HISTORY_FILE, raw)
    return history_to_frontend(raw)