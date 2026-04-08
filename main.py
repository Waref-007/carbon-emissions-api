from fastapi import FastAPI
from engine import run_emissions_engine

app = FastAPI(title="Carbon Emissions API")


@app.get("/")
def root():
    return {"status": "ok", "message": "Carbon emissions API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/calculate")
def calculate(payload: dict):
    try:
        activities = payload.get("activities", [])

        if not isinstance(activities, list):
            return {"error": "activities must be a list"}

        result = run_emissions_engine({"activities": activities})
        return result

    except Exception as e:
        return {"error": str(e)}
