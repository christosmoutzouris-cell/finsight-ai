"""
FinSight AI — FastAPI REST API

Εκθέτει τα δεδομένα του pipeline μέσω HTTP endpoints.

Αρχιτεκτονική:
- FastAPI: το web framework (σαν Flask αλλά πιο γρήγορο και με auto docs)
- Uvicorn: ο ASGI server που τρέχει το FastAPI
- Pydantic: validation των request/response schemas
- PostgreSQL: η βάση δεδομένων (ξεχωριστός container)

Endpoints:
GET  /                          → health check
GET  /symbols                   → λίστα active symbols
GET  /prices/{symbol}           → τελευταίες τιμές
GET  /sentiment/{symbol}        → sentiment scores
GET  /predictions/{symbol}      → LSTM + NN predictions
GET  /report/{symbol}           → LLM report
GET  /dashboard                 → συνοπτικά όλα μαζί
POST /symbols                   → προσθήκη νέου symbol
DELETE /symbols/{symbol}        → απενεργοποίηση symbol
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2
import psycopg2.extras
from datetime import datetime

# ── App initialization ─────────────────────────────────────────────────────
# FastAPI δημιουργεί αυτόματα:
# - /docs → Swagger UI (interactive documentation)
# - /redoc → ReDoc documentation
# - /openapi.json → OpenAPI schema
app = FastAPI(
    title="FinSight AI API",
    description="Financial Intelligence Platform — REST API",
    version="1.0.0",
)

# ── Database connection ────────────────────────────────────────────────────
PG_CONN = {
    "host":     "finsight-postgres",
    "port":     5432,
    "user":     "finsight",
    "password": "finsight123",
    "dbname":   "finsight_db",
}

def get_db():
    """
    Επιστρέφει database connection.
    RealDictCursor: επιστρέφει rows ως dicts αντί για tuples
    π.χ. {"symbol": "AAPL", "price": 314.92} αντί για ("AAPL", 314.92)
    """
    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cursor


# ── Pydantic Models ────────────────────────────────────────────────────────
# Ορίζουν το schema των requests και responses
# FastAPI τα χρησιμοποιεί για validation και documentation

class SymbolCreate(BaseModel):
    """Schema για προσθήκη νέου symbol."""
    symbol:  str
    company: Optional[str] = None
    sector:  Optional[str] = None


class SymbolResponse(BaseModel):
    """Schema για response ενός symbol."""
    symbol:    str
    company:   Optional[str]
    sector:    Optional[str]
    is_active: bool


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════

@app.get("/")
def health_check():
    """
    Health check endpoint.
    Χρησιμοποιείται από monitoring tools για να ελέγξουν
    αν το API τρέχει σωστά.
    """
    return {
        "status":  "healthy",
        "service": "FinSight AI API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/symbols")
def get_symbols():
    """
    Επιστρέφει όλα τα active symbols από watched_symbols.
    
    Παράδειγμα response:
    [{"symbol": "AAPL", "company": "Apple Inc.", "sector": "Technology", "is_active": true}]
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT symbol, company, sector, is_active
            FROM watched_symbols
            WHERE is_active = TRUE
            ORDER BY symbol
        """)
        return {"symbols": [dict(r) for r in cursor.fetchall()]}
    finally:
        cursor.close(); conn.close()


@app.post("/symbols", status_code=201)
def add_symbol(body: SymbolCreate):
    """
    Προσθέτει νέο symbol στη βάση.
    Μετά την επόμενη εκτέλεση του DAG, το pipeline θα το
    συμπεριλάβει αυτόματα.

    Body example:
    {"symbol": "TSLA", "company": "Tesla Inc.", "sector": "Automotive"}
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            INSERT INTO watched_symbols (symbol, company, sector)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET is_active = TRUE
            RETURNING symbol, company, sector, is_active
        """, (body.symbol.upper(), body.company, body.sector))
        conn.commit()
        result = dict(cursor.fetchone())
        return {"message": f"Symbol {body.symbol} added", "symbol": result}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close(); conn.close()


@app.delete("/symbols/{symbol}")
def deactivate_symbol(symbol: str):
    """
    Απενεργοποιεί ένα symbol (soft delete).
    Δεν διαγράφει τα ιστορικά δεδομένα.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            UPDATE watched_symbols SET is_active = FALSE
            WHERE symbol = %s
            RETURNING symbol
        """, (symbol.upper(),))
        conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
        return {"message": f"Symbol {symbol} deactivated"}
    finally:
        cursor.close(); conn.close()


@app.get("/prices/{symbol}")
def get_prices(symbol: str, limit: int = 10):
    """
    Επιστρέφει τις τελευταίες τιμές για ένα symbol.
    
    Query params:
    - limit: πόσες τιμές να επιστρέψει (default: 10)
    
    Παράδειγμα: GET /prices/AAPL?limit=5
    """
    conn, cursor = get_db()
    try:
        # Gold snapshot — τελευταία τιμή με signals
        cursor.execute("""
            SELECT symbol, price, ma5, ma20, ma5_signal, pct_change, volume, event_time
            FROM gold_symbol_snapshot
            WHERE symbol = %s
        """, (symbol.upper(),))
        snapshot = cursor.fetchone()

        # Streaming prices — τελευταία N ticks
        cursor.execute("""
            SELECT symbol, price, open, high, low, volume, event_time, ingested_at
            FROM stock_prices
            WHERE symbol = %s
            ORDER BY ingested_at DESC
            LIMIT %s
        """, (symbol.upper(), limit))
        ticks = [dict(r) for r in cursor.fetchall()]

        if not snapshot and not ticks:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        return {
            "symbol":   symbol.upper(),
            "snapshot": dict(snapshot) if snapshot else None,
            "ticks":    ticks,
        }
    finally:
        cursor.close(); conn.close()


@app.get("/sentiment/{symbol}")
def get_sentiment(symbol: str):
    """
    Επιστρέφει sentiment analysis για ένα symbol.
    Περιλαμβάνει rule-based και NN sentiment.
    """
    conn, cursor = get_db()
    try:
        # Rule-based sentiment
        cursor.execute("""
            SELECT headline, sentiment, confidence, analyzed_at
            FROM sentiment_scores
            WHERE symbol = %s
            ORDER BY analyzed_at DESC LIMIT 5
        """, (symbol.upper(),))
        rule_based = [dict(r) for r in cursor.fetchall()]

        # NN sentiment
        cursor.execute("""
            SELECT headline, nn_sentiment, nn_confidence, analyzed_at
            FROM sentiment_nn_scores
            WHERE symbol = %s
            ORDER BY analyzed_at DESC LIMIT 5
        """, (symbol.upper(),))
        nn_scores = [dict(r) for r in cursor.fetchall()]

        return {
            "symbol":     symbol.upper(),
            "rule_based": rule_based,
            "nn_scores":  nn_scores,
        }
    finally:
        cursor.close(); conn.close()


@app.get("/predictions/{symbol}")
def get_predictions(symbol: str):
    """
    Επιστρέφει AI predictions για ένα symbol:
    - LSTM price prediction
    - NN price direction
    - Accuracy report (αν υπάρχει)
    """
    conn, cursor = get_db()
    try:
        # LSTM predictions
        cursor.execute("""
            SELECT current_price, predicted_price, predicted_change,
                   predicted_direction, actual_price, direction_correct,
                   predicted_at, evaluated_at
            FROM price_predictions
            WHERE symbol = %s
            ORDER BY predicted_at DESC LIMIT 5
        """, (symbol.upper(),))
        lstm = [dict(r) for r in cursor.fetchall()]

        # NN direction
        cursor.execute("""
            SELECT predicted_dir, confidence, prob_up, prob_down, predicted_at
            FROM price_direction_predictions
            WHERE symbol = %s
            ORDER BY predicted_at DESC LIMIT 1
        """, (symbol.upper(),))
        nn_dir = cursor.fetchone()

        # Accuracy summary
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN direction_correct THEN 1 END) as correct,
                ROUND(AVG(price_error)::numeric, 2) as avg_error
            FROM price_predictions
            WHERE symbol = %s AND evaluated_at IS NOT NULL
        """, (symbol.upper(),))
        accuracy = cursor.fetchone()

        return {
            "symbol":           symbol.upper(),
            "lstm_predictions": lstm,
            "nn_direction":     dict(nn_dir) if nn_dir else None,
            "accuracy_summary": dict(accuracy) if accuracy else None,
        }
    finally:
        cursor.close(); conn.close()


@app.get("/report/{symbol}")
def get_report(symbol: str):
    """
    Επιστρέφει το τελευταίο LLM-generated report για ένα symbol.
    """
    conn, cursor = get_db()
    try:
        cursor.execute("""
            SELECT symbol, report, model, generated_at
            FROM llm_reports
            WHERE symbol = %s
            ORDER BY generated_at DESC LIMIT 1
        """, (symbol.upper(),))
        report = cursor.fetchone()

        if not report:
            raise HTTPException(
                status_code=404,
                detail=f"No report found for {symbol}"
            )

        return dict(report)
    finally:
        cursor.close(); conn.close()


@app.get("/dashboard")
def get_dashboard():
    """
    Συνοπτικό dashboard με όλα τα symbols.
    Ένα endpoint που επιστρέφει όλα τα βασικά δεδομένα.
    Χρήσιμο για frontend που θέλει να φορτώσει όλα μαζί.
    """
    conn, cursor = get_db()
    try:
        # Gold snapshots για όλα τα symbols
        cursor.execute("""
            SELECT
                g.symbol,
                g.price,
                g.ma5,
                g.ma20,
                g.ma5_signal,
                g.pct_change,
                s.sentiment,
                s.confidence as sentiment_confidence,
                p.predicted_direction as lstm_direction,
                p.predicted_change as lstm_change,
                d.predicted_dir as nn_direction,
                l.report as llm_report
            FROM gold_symbol_snapshot g
            LEFT JOIN LATERAL (
                SELECT sentiment, confidence
                FROM sentiment_scores
                WHERE symbol = g.symbol
                ORDER BY analyzed_at DESC LIMIT 1
            ) s ON TRUE
            LEFT JOIN LATERAL (
                SELECT predicted_direction, predicted_change
                FROM price_predictions
                WHERE symbol = g.symbol
                ORDER BY predicted_at DESC LIMIT 1
            ) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT predicted_dir
                FROM price_direction_predictions
                WHERE symbol = g.symbol
                ORDER BY predicted_at DESC LIMIT 1
            ) d ON TRUE
            LEFT JOIN LATERAL (
                SELECT report
                FROM llm_reports
                WHERE symbol = g.symbol
                ORDER BY generated_at DESC LIMIT 1
            ) l ON TRUE
            ORDER BY g.symbol
        """)
        rows = [dict(r) for r in cursor.fetchall()]

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "count":     len(rows),
            "symbols":   rows,
        }
    finally:
        cursor.close(); conn.close()
        

'''
Τώρα δοκίμασε στο Postman όλα τα endpoints:
GET http://localhost:8088/symbols
GET http://localhost:8088/dashboard
GET http://localhost:8088/prices/AAPL
GET http://localhost:8088/sentiment/AAPL
GET http://localhost:8088/predictions/AAPL
GET http://localhost:8088/report/AAPL
Και δοκίμασε και ένα POST για να προσθέσεις νέο symbol:
POST http://localhost:8088/symbols
'''
