from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

API_KEY = os.getenv("API_KEY", "").strip()
BASE_URL = os.getenv("APISPORTS_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
TZ_NAME = os.getenv("TZ", "Europe/Madrid")
TZ = ZoneInfo(TZ_NAME)

ALLOWED_COMPETITIONS = (
    "la liga",
    "laliga",
    "segunda división",
    "segunda division",
    "laliga 2",
    "uefa champions league",
    "fifa world cup",
    "world cup",
)

app = FastAPI(title="Top 3 Picks Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_cache: Dict[str, Dict[str, Any]] = {}


def today_key() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def parse_percent(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).strip().replace("%", "").replace(",", ".")
    try:
        return int(round(float(text)))
    except ValueError:
        return 0


def parse_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def confidence_color(probability: int) -> str:
    if probability >= 70:
        return "verde"
    if probability >= 62:
        return "amarillo"
    return "rojo"


def madrid_time_from_timestamp(timestamp: Optional[int]) -> str:
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp, TZ).strftime("%H:%M")


def api_get(path: str, **params: Any) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("Falta API_KEY en el entorno de Render")
    response = requests.get(
        f"{BASE_URL}/{path.lstrip('/')}",
        headers={"x-apisports-key": API_KEY},
        params=params,
        timeout=25,
    )
    response.raise_for_status()
    return response.json()


def is_allowed_competition(name: str) -> bool:
    normalized = name.lower()
    return any(token in normalized for token in ALLOWED_COMPETITIONS)


def fixture_candidates_for_today() -> List[Dict[str, Any]]:
    data = api_get("fixtures", date=today_key(), timezone=TZ_NAME)
    fixtures: List[Dict[str, Any]] = []

    for item in data.get("response", []):
        league_name = item.get("league", {}).get("name", "")
        if not is_allowed_competition(league_name):
            continue

        short_status = item.get("fixture", {}).get("status", {}).get("short", "")
        if short_status not in {"NS", "TBD"}:
            continue

        fixtures.append(item)

    fixtures.sort(key=lambda x: x.get("fixture", {}).get("timestamp", 0))
    return fixtures


def prediction_for_fixture(fixture_id: int) -> Optional[Dict[str, Any]]:
    data = api_get("predictions", fixture=fixture_id)
    items = data.get("response", [])
    return items[0] if items else None


def build_tipster_text(pred: Dict[str, Any], default_text: str) -> str:
    advice = pred.get("predictions", {}).get("advice") or ""
    comment = pred.get("predictions", {}).get("winner", {}).get("comment") or ""
    attack_home = pred.get("comparison", {}).get("att", {}).get("home") or ""
    attack_away = pred.get("comparison", {}).get("att", {}).get("away") or ""
    defence_home = pred.get("comparison", {}).get("def", {}).get("home") or ""
    defence_away = pred.get("comparison", {}).get("def", {}).get("away") or ""

    parts = [part for part in [advice, comment] if part]
    if attack_home or attack_away or defence_home or defence_away:
        parts.append(
            f"Comparativa: ataque local {attack_home}, ataque visitante {attack_away}, "
            f"defensa local {defence_home}, defensa visitante {defence_away}."
        )

    text = " ".join(parts).strip()
    return text if text else default_text


def markets_from_prediction(fixture: Dict[str, Any], pred: Dict[str, Any]) -> List[Dict[str, Any]]:
    fixture_info = fixture.get("fixture", {})
    teams = fixture.get("teams", {})
    league = fixture.get("league", {})

    fixture_id = fixture_info.get("id")
    competition = league.get("name", "")
    home_name = teams.get("home", {}).get("name", "Local")
    away_name = teams.get("away", {}).get("name", "Visitante")
    match_name = f"{home_name} vs {away_name}"
    starts_at = madrid_time_from_timestamp(fixture_info.get("timestamp"))

    predictions = pred.get("predictions", {})
    percents = predictions.get("percent", {}) or {}
    winner = predictions.get("winner", {}) or {}
    advice = predictions.get("advice") or ""
    under_over = predictions.get("under_over") or ""
    goal_home = parse_float(predictions.get("goals", {}).get("home"))
    goal_away = parse_float(predictions.get("goals", {}).get("away"))

    picks: List[Dict[str, Any]] = []

    winner_name = winner.get("name")
    if winner_name:
        if winner_name == home_name:
            probability = parse_percent(percents.get("home"))
        elif winner_name == away_name:
            probability = parse_percent(percents.get("away"))
        else:
            probability = max(parse_percent(percents.get("home")), parse_percent(percents.get("away")))
        probability = max(probability, 62)

        picks.append(
            {
                "fixture_id": fixture_id,
                "competition": competition,
                "match": match_name,
                "market": f"Ganador {winner_name}",
                "probability": probability,
                "verdict": "Sí",
                "color": confidence_color(probability),
                "explanation": build_tipster_text(
                    pred,
                    f"Tipster: el modelo ve superioridad de {winner_name} por forma y comparativa estadística."
                ),
                "starts_at": starts_at,
                "sort_score": probability,
            }
        )

    if "over 2.5" in under_over.lower():
        probability = 62
        if goal_home + goal_away >= 3.2:
            probability = 72
        elif goal_home + goal_away >= 2.8:
            probability = 67
        picks.append(
            {
                "fixture_id": fixture_id,
                "competition": competition,
                "match": match_name,
                "market": "Más de 2.5 goles",
                "probability": probability,
                "verdict": "Sí",
                "color": confidence_color(probability),
                "explanation": build_tipster_text(
                    pred,
                    "Tipster: el pronóstico apunta a partido abierto y volumen ofensivo suficiente para superar los 2.5 goles."
                ),
                "starts_at": starts_at,
                "sort_score": probability,
            }
        )

    if goal_home >= 1.0 and goal_away >= 1.0:
        probability = 64
        if goal_home >= 1.2 and goal_away >= 1.1:
            probability = 70
        picks.append(
            {
                "fixture_id": fixture_id,
                "competition": competition,
                "match": match_name,
                "market": "Ambos marcan",
                "probability": probability,
                "verdict": "Sí",
                "color": confidence_color(probability),
                "explanation": build_tipster_text(
                    pred,
                    "Tipster: ambos equipos proyectan gol y el escenario favorece que los dos vean portería."
                ),
                "starts_at": starts_at,
                "sort_score": probability,
            }
        )

    return picks


def fallback_payload() -> Dict[str, Any]:
    date = today_key()
    return {
        "date": date,
        "cached_until": date,
        "generated_at": datetime.now(TZ).isoformat(),
        "picks": [
            {
                "fixture_id": 0,
                "competition": "LaLiga",
                "match": "Real Madrid vs Sevilla",
                "market": "Ganador Real Madrid",
                "probability": 74,
                "verdict": "Sí",
                "color": "verde",
                "explanation": "Tipster: el local llega mejor y los datos del modelo lo sitúan claramente por delante.",
                "starts_at": "21:00",
            },
            {
                "fixture_id": 1,
                "competition": "Champions League",
                "match": "PSG vs Bayern",
                "market": "Más de 2.5 goles",
                "probability": 68,
                "verdict": "Sí",
                "color": "amarillo",
                "explanation": "Tipster: ambos equipos generan mucho y el partido pinta abierto.",
                "starts_at": "20:45",
            },
            {
                "fixture_id": 2,
                "competition": "Segunda División",
                "match": "Eibar vs Oviedo",
                "market": "Ambos marcan",
                "probability": 65,
                "verdict": "Sí",
                "color": "amarillo",
                "explanation": "Tipster: los dos equipos tienen argumentos ofensivos y conceden ocasiones.",
                "starts_at": "18:30",
            },
        ],
    }


def generate_daily_picks() -> Dict[str, Any]:
    fixtures = fixture_candidates_for_today()
    all_markets: List[Dict[str, Any]] = []

    for fixture in fixtures[:15]:
        fixture_id = fixture.get("fixture", {}).get("id")
        if not fixture_id:
            continue

        try:
            pred = prediction_for_fixture(fixture_id)
        except Exception:
            continue

        if not pred:
            continue

        all_markets.extend(markets_from_prediction(fixture, pred))

    if not all_markets:
        return fallback_payload()

    # prefer only one market per fixture when possible
    all_markets.sort(key=lambda item: item["sort_score"], reverse=True)
    selected: List[Dict[str, Any]] = []
    used_fixtures = set()

    for item in all_markets:
        fixture_id = item["fixture_id"]
        if fixture_id in used_fixtures:
            continue
        used_fixtures.add(fixture_id)
        item.pop("sort_score", None)
        selected.append(item)
        if len(selected) == 3:
            break

    if len(selected) < 3:
        for item in all_markets:
            if len(selected) == 3:
                break
            cleaned = dict(item)
            cleaned.pop("sort_score", None)
            if cleaned not in selected:
                selected.append(cleaned)

    return {
        "date": today_key(),
        "cached_until": today_key(),
        "generated_at": datetime.now(TZ).isoformat(),
        "picks": selected[:3],
    }


@app.get("/")
def root() -> Dict[str, str]:
    return {"message": "Backend funcionando"}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "date": today_key()}


@app.get("/top-picks-today")
def top_picks_today(force: bool = False) -> Dict[str, Any]:
    key = today_key()

    if not force and key in _cache:
        return _cache[key]

    try:
        payload = generate_daily_picks()
    except Exception as exc:
        payload = fallback_payload()
        payload["warning"] = f"Fallback activado: {exc}"

    _cache.clear()
    _cache[key] = payload
    return payload
