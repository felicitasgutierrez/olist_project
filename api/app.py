"""
API simulada de ventas de la app móvil - Black Friday
Expone endpoint JSON para extracción por el pipeline Airflow
"""
import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Sales App API", description="Ventas simuladas app móvil - Black Friday")

class Sale(BaseModel):
    sale_id: str
    sale_timestamp: str
    amount: float
    channel: str = "app"
    promotion_used: bool
    promotion_code: str | None
    last_updated: str

# Simular ventas recientes (últimos 2 minutos)
def _generate_app_sales() -> List[dict]:
    sales = []
    base_id = int(datetime.now().timestamp() * 1000)
    for i in range(random.randint(2, 8)):
        ts = datetime.now() - timedelta(minutes=random.randint(0, 2), seconds=random.randint(0, 59))
        amount = round(random.uniform(29.99, 599.99), 2)
        promo = random.random() > 0.5
        sales.append({
            "sale_id": f"APP-{base_id + i}",
            "sale_timestamp": ts.isoformat(),
            "amount": amount,
            "channel": "app",
            "promotion_used": promo,
            "promotion_code": f"BF2024-{random.randint(10, 30)}" if promo else None,
            "last_updated": datetime.now().isoformat()
        })
    return sales

@app.get("/")
def root():
    return {"message": "Black Friday Sales API", "docs": "/docs"}

@app.get("/sales", response_model=List[Sale])
def get_sales():
    """Retorna ventas simuladas de la app. El pipeline las extrae cada minuto."""
    return _generate_app_sales()

@app.get("/health")
def health():
    return {"status": "ok"}
