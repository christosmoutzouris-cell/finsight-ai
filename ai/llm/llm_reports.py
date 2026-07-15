"""
LLM Financial Reports με Ollama

Τι κάνει:
1. Διαβάζει από PostgreSQL: τιμές, sentiment, LSTM predictions,
   NN predictions, gold metrics
2. Φτιάχνει structured prompt για κάθε symbol
3. Στέλνει στο Ollama (Llama 3.2) που τρέχει τοπικά
4. Αποθηκεύει το natural language report στη βάση

Γιατί Ollama και όχι OpenAI/Claude;
- Τοπικό = δωρεάν, χωρίς API key, χωρίς internet
- Private = τα financial δεδομένα δεν φεύγουν από τον υπολογιστή
- Production pattern: πολλές εταιρείες χρησιμοποιούν local LLM
  για sensitive financial data
"""

import logging
import json
import requests
import psycopg2
from datetime import datetime, timezone
import sys
sys.path.append("/app")
from common.db_utils import get_active_symbols, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LLM] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# Ollama τρέχει στο ίδιο Docker network
OLLAMA_URL = "http://finsight-ollama:11434/api/generate"
MODEL      = "llama3.2"


# ══════════════════════════════════════════════════════════════════════════
# DATA COLLECTION
# Μαζεύει όλα τα δεδομένα για ένα symbol από τη βάση
# ══════════════════════════════════════════════════════════════════════════

def get_symbol_data(symbol: str) -> dict:
    """
    Συλλέγει όλα τα διαθέσιμα δεδομένα για ένα symbol:
    - Τρέχουσα τιμή και moving averages (από gold_symbol_snapshot)
    - Daily performance (από daily_stock_summary)
    - Rule-based sentiment (από sentiment_scores)
    - NN sentiment (από sentiment_nn_scores)
    - LSTM prediction (από price_predictions)
    - NN price direction (από price_direction_predictions)
    """
    conn   = get_connection()
    cursor = conn.cursor()
    data   = {"symbol": symbol}

    # 1. Gold snapshot — τρέχουσα τιμή και signals
    cursor.execute("""
        SELECT price, ma5, ma20, ma5_signal, pct_change, volume
        FROM gold_symbol_snapshot
        WHERE symbol = %s
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        data["price"]      = float(row[0])
        data["ma5"]        = float(row[1]) if row[1] else None
        data["ma20"]       = float(row[2]) if row[2] else None
        data["ma5_signal"] = row[3]
        data["pct_change"] = float(row[4]) if row[4] else 0
        data["volume"]     = int(row[5]) if row[5] else 0

    # 2. Daily summary — τελευταία ημέρα
    cursor.execute("""
        SELECT date, open, high, low, close, daily_return
        FROM daily_stock_summary
        WHERE symbol = %s
        ORDER BY date DESC LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        data["last_date"]    = str(row[0])
        data["daily_open"]   = float(row[1]) if row[1] else None
        data["daily_high"]   = float(row[2]) if row[2] else None
        data["daily_low"]    = float(row[3]) if row[3] else None
        data["daily_close"]  = float(row[4]) if row[4] else None
        data["daily_return"] = float(row[5]) if row[5] else 0

    # 3. Rule-based sentiment — τελευταία 3 headlines
    cursor.execute("""
        SELECT headline, sentiment, confidence
        FROM sentiment_scores
        WHERE symbol = %s
        ORDER BY analyzed_at DESC LIMIT 3
    """, (symbol,))
    rows = cursor.fetchall()
    data["sentiment_headlines"] = [
        {"text": r[0], "sentiment": r[1], "confidence": float(r[2])}
        for r in rows
    ]

    # 4. NN sentiment — average scores
    cursor.execute("""
        SELECT
            AVG(score_positive) as avg_pos,
            AVG(score_negative) as avg_neg,
            AVG(nn_confidence)  as avg_conf,
            MODE() WITHIN GROUP (ORDER BY nn_sentiment) as dominant
        FROM sentiment_nn_scores
        WHERE symbol = %s
        AND analyzed_at > NOW() - INTERVAL '1 day'
    """, (symbol,))
    row = cursor.fetchone()
    if row and row[0]:
        data["nn_sentiment"] = {
            "avg_positive":  round(float(row[0]), 3),
            "avg_negative":  round(float(row[1]), 3),
            "avg_confidence": round(float(row[2]), 3),
            "dominant":      row[3],
        }

    # 5. LSTM prediction
    cursor.execute("""
        SELECT current_price, predicted_price, predicted_change, predicted_direction
        FROM price_predictions
        WHERE symbol = %s
        ORDER BY predicted_at DESC LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        data["lstm_prediction"] = {
            "current_price":   float(row[0]),
            "predicted_price": float(row[1]),
            "predicted_change": float(row[2]),
            "direction":       row[3],
        }

    # 6. NN price direction
    cursor.execute("""
        SELECT predicted_dir, confidence, prob_up, prob_down
        FROM price_direction_predictions
        WHERE symbol = %s
        ORDER BY predicted_at DESC LIMIT 1
    """, (symbol,))
    row = cursor.fetchone()
    if row:
        data["nn_direction"] = {
            "direction":  row[0],
            "confidence": float(row[1]),
            "prob_up":    float(row[2]),
            "prob_down":  float(row[3]),
        }

    cursor.close()
    conn.close()
    return data


# ══════════════════════════════════════════════════════════════════════════
# PROMPT BUILDING
# Φτιάχνει structured prompt που δίνει στο LLM
# Το prompt engineering είναι κρίσιμο — καλό prompt = καλό output
# ══════════════════════════════════════════════════════════════════════════

def build_prompt(data: dict) -> str:
    """
    Φτιάχνει structured prompt για το LLM.

    Καλές πρακτικές prompt engineering:
    1. Δώσε ρόλο στο LLM ("You are a financial analyst")
    2. Δώσε συγκεκριμένα δεδομένα (όχι vague)
    3. Πες ακριβώς τι format θέλεις στο output
    4. Βάλε constraints (max length, language)
    """
    symbol = data["symbol"]

    # Sentiment summary
    sentiment_text = ""
    if data.get("sentiment_headlines"):
        for h in data["sentiment_headlines"]:
            sentiment_text += f"  - '{h['text']}' → {h['sentiment']} ({h['confidence']:.2f})\n"

    nn_sent_text = ""
    if data.get("nn_sentiment"):
        nn = data["nn_sentiment"]
        nn_sent_text = (f"Neural Network Sentiment: {nn['dominant']} "
                       f"(pos={nn['avg_positive']:.2f}, neg={nn['avg_negative']:.2f})")

    lstm_text = ""
    if data.get("lstm_prediction"):
        p = data["lstm_prediction"]
        lstm_text = (f"LSTM Prediction: ${p['predicted_price']:.2f} "
                    f"({p['predicted_change']:+.2f}%) → {p['direction']}")

    nn_dir_text = ""
    if data.get("nn_direction"):
        d = data["nn_direction"]
        nn_dir_text = (f"NN Direction: {d['direction']} "
                      f"(confidence={d['confidence']:.2f}, "
                      f"P(UP)={d['prob_up']:.2f})")

    prompt = f"""You are a concise financial analyst. Write a brief 3-sentence analysis for {symbol} stock.

CURRENT DATA:
- Price: ${data.get('price', 'N/A'):.2f}
- MA5: ${data.get('ma5', 0):.2f} | MA20: ${data.get('ma20', 0):.2f}
- MA5 Signal: {data.get('ma5_signal', 'N/A')}
- Daily Return: {data.get('daily_return', 0):+.2f}%
- Volume: {data.get('volume', 0):,}

NEWS SENTIMENT:
{sentiment_text}
{nn_sent_text}

AI PREDICTIONS:
{lstm_text}
{nn_dir_text}

Write exactly 3 sentences:
1. Current price action and trend
2. Sentiment analysis summary  
3. Short-term outlook based on predictions

Be concise, factual, and professional. Max 100 words total."""

    return prompt


# ══════════════════════════════════════════════════════════════════════════
# LLM CALL
# Στέλνει το prompt στο Ollama και παίρνει την απάντηση
# ══════════════════════════════════════════════════════════════════════════

def call_ollama(prompt: str) -> str:
    """
    Καλεί το Ollama REST API.

    stream=False: περιμένει να τελειώσει το generation
    αντί να στέλνει tokens ένα-ένα (streaming)
    """
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model":  MODEL,
                "prompt": prompt,
                "stream": False,
                # Options για πιο consistent output
                "options": {
                    "temperature": 0.3,  # χαμηλό = πιο factual, λιγότερο creative
                    "top_p":       0.9,
                    "max_tokens":  200,
                }
            },
            timeout=120   # 2 λεπτά timeout
        )
        response.raise_for_status()
        return response.json()["response"].strip()
    except Exception as e:
        log.error(f"Ollama error: {e}")
        return f"Unable to generate report: {e}"


# ══════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════

def save_reports(reports: list[dict]):
    """Αποθηκεύει τα LLM reports στη PostgreSQL."""
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS llm_reports (
            id           BIGSERIAL PRIMARY KEY,
            symbol       VARCHAR(10)   NOT NULL,
            report       TEXT          NOT NULL,
            model        VARCHAR(50)   DEFAULT 'llama3.2',
            data_snapshot JSONB,
            generated_at TIMESTAMPTZ   DEFAULT NOW()
        );
    """)

    for r in reports:
        cursor.execute("""
            INSERT INTO llm_reports (symbol, report, model, data_snapshot)
            VALUES (%s, %s, %s, %s)
        """, (
            r["symbol"],
            r["report"],
            MODEL,
            json.dumps(r["data"])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"✓ Έγραψα {len(reports)} LLM reports")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  FinSight LLM Financial Reports")
    log.info("=" * 60)

    symbols = get_active_symbols()
    reports = []

    for symbol in symbols:
        log.info(f"\n{'─'*40}")
        log.info(f"Generating report for {symbol}...")

        # 1. Συλλογή δεδομένων
        data   = get_symbol_data(symbol)
        prompt = build_prompt(data)

        # 2. LLM call
        log.info(f"Calling Ollama ({MODEL})...")
        report = call_ollama(prompt)

        # 3. Log και αποθήκευση
        log.info(f"\n📊 {symbol} REPORT:")
        log.info(f"{report}")

        reports.append({
            "symbol": symbol,
            "report": report,
            "data":   data,
        })

    # 4. Save all reports
    save_reports(reports)

    log.info("\n" + "=" * 60)
    log.info("  LLM Reports Complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()