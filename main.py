from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"ok": True, "msg": "backend vivo"}

@app.get("/api/picks")
def picks(force_refresh: bool = False):
    return {
        "generated_at": "2026-04-06T01:00:00+02:00",
        "cache_day": "2026-04-06",
        "lookahead_hours": 36,
        "count": 3,
        "picks": [
            {
                "id": 1,
                "match": "Real Madrid vs Sevilla",
                "league": "LaLiga",
                "time_local": "06/04 21:00",
                "pick": "Gana Real Madrid",
                "pick_type": "winner",
                "confidence": 84,
                "odds_estimate": 1.67,
                "odds_band": "normal",
                "pick_winner": "Real Madrid",
                "btts": "No",
                "over_2_5": "Sí",
                "cards": {"Real Madrid": 2, "Sevilla": 3},
                "status": "pending",
                "score_line": "",
                "home_team": "Real Madrid",
                "away_team": "Sevilla",
                "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                "prediction_source": "debug"
            },
            {
                "id": 2,
                "match": "Arsenal vs Chelsea",
                "league": "Premier League",
                "time_local": "06/04 18:30",
                "pick": "Ambos marcan",
                "pick_type": "btts_yes",
                "confidence": 79,
                "odds_estimate": 1.92,
                "odds_band": "media",
                "pick_winner": "Arsenal",
                "btts": "Sí",
                "over_2_5": "Sí",
                "cards": {"Arsenal": 2, "Chelsea": 2},
                "status": "pending",
                "score_line": "",
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                "prediction_source": "debug"
            },
            {
                "id": 3,
                "match": "Barcelona vs Valencia",
                "league": "LaLiga",
                "time_local": "06/04 20:00",
                "pick": "Más de 2.5 goles",
                "pick_type": "over_2_5",
                "confidence": 77,
                "odds_estimate": 2.14,
                "odds_band": "alta",
                "pick_winner": "Barcelona",
                "btts": "Sí",
                "over_2_5": "Sí",
                "cards": {"Barcelona": 2, "Valencia": 3},
                "status": "pending",
                "score_line": "",
                "home_team": "Barcelona",
                "away_team": "Valencia",
                "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                "prediction_source": "debug"
            }
        ],
        "combo_of_day": {
            "size": 2,
            "estimated_total_odds": 3.21,
            "confidence": 81,
            "picks": [
                {
                    "match": "Real Madrid vs Sevilla",
                    "pick": "Gana Real Madrid",
                    "pick_type": "winner",
                    "confidence": 84,
                    "odds_estimate": 1.67,
                    "odds_band": "normal",
                    "league": "LaLiga"
                },
                {
                    "match": "Arsenal vs Chelsea",
                    "pick": "Ambos marcan",
                    "pick_type": "btts_yes",
                    "confidence": 79,
                    "odds_estimate": 1.92,
                    "odds_band": "media",
                    "league": "Premier League"
                }
            ]
        },
        "groups": {
            "normal": [
                {
                    "id": 1,
                    "match": "Real Madrid vs Sevilla",
                    "league": "LaLiga",
                    "time_local": "06/04 21:00",
                    "pick": "Gana Real Madrid",
                    "pick_type": "winner",
                    "confidence": 84,
                    "odds_estimate": 1.67,
                    "odds_band": "normal",
                    "pick_winner": "Real Madrid",
                    "btts": "No",
                    "over_2_5": "Sí",
                    "cards": {"Real Madrid": 2, "Sevilla": 3},
                    "status": "pending",
                    "score_line": "",
                    "home_team": "Real Madrid",
                    "away_team": "Sevilla",
                    "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                    "prediction_source": "debug"
                }
            ],
            "media": [
                {
                    "id": 2,
                    "match": "Arsenal vs Chelsea",
                    "league": "Premier League",
                    "time_local": "06/04 18:30",
                    "pick": "Ambos marcan",
                    "pick_type": "btts_yes",
                    "confidence": 79,
                    "odds_estimate": 1.92,
                    "odds_band": "media",
                    "pick_winner": "Arsenal",
                    "btts": "Sí",
                    "over_2_5": "Sí",
                    "cards": {"Arsenal": 2, "Chelsea": 2},
                    "status": "pending",
                    "score_line": "",
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                    "prediction_source": "debug"
                }
            ],
            "alta": [
                {
                    "id": 3,
                    "match": "Barcelona vs Valencia",
                    "league": "LaLiga",
                    "time_local": "06/04 20:00",
                    "pick": "Más de 2.5 goles",
                    "pick_type": "over_2_5",
                    "confidence": 77,
                    "odds_estimate": 2.14,
                    "odds_band": "alta",
                    "pick_winner": "Barcelona",
                    "btts": "Sí",
                    "over_2_5": "Sí",
                    "cards": {"Barcelona": 2, "Valencia": 3},
                    "status": "pending",
                    "score_line": "",
                    "home_team": "Barcelona",
                    "away_team": "Valencia",
                    "tipster_explanation": "Pick de prueba para verificar frontend y backend.",
                    "prediction_source": "debug"
                }
            ]
        }
    }

@app.get("/api/history")
def history():
    return {
        "days": [
            {
                "date": "2026-04-05",
                "stats": {"won": 2, "lost": 1, "pending": 0},
                "picks": [
                    {
                        "match": "Liverpool vs Spurs",
                        "pick": "Más de 2.5 goles",
                        "pick_type": "over_2_5",
                        "league": "Premier League",
                        "status": "won",
                        "score_line": "3-1",
                        "odds_estimate": 1.88
                    }
                ]
            }
        ]
    }