import logging
import psycopg2
import requests
from bs4 import BeautifulSoup
import sys
sys.path.append("/app")
from common.db_utils import get_active_symbols

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SENTIMENT] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

PG_CONN = {"host": "finsight-postgres", "port": 5432, "user": "finsight", "password": "finsight123", "dbname": "finsight_db"}

POSITIVE_WORDS = {"surges","beats","jump","grows","raises","upgrade","record","high","breakthrough","boosts","confidence","exceeds","rallies","strong","revenue","growth","profit","dividend","partnership"}
NEGATIVE_WORDS = {"plunges","misses","falls","tumble","loss","warning","downgrade","layoffs","investigation","concerns","lost","disruptions","cuts","low","collapse","disappointing","weak","hurt","restructuring"}

SYNTHETIC = {
    "AAPL":  ["Apple reports record iPhone sales","Apple faces antitrust scrutiny","Apple stock rises on services"],
    "MSFT":  ["Microsoft Azure revenue surges","Microsoft beats earnings","Microsoft AI investments grow"],
    "GOOGL": ["Google advertising revenue grows","Alphabet reports strong results","Google faces regulatory challenges"],
    "AMZN":  ["Amazon AWS maintains leadership","Amazon prime hits record","Amazon reports mixed results"],
    "NVDA":  ["NVIDIA data center revenue high","NVIDIA AI chip demand strong","NVIDIA stock surges on outlook"],
    "TSLA":  ["Tesla deliveries beat expectations","Tesla faces production challenges","Tesla reports record revenue"],
}

def fetch_headlines(symbol):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        soup = BeautifulSoup(requests.get(url, timeout=10).content, "xml")
        headlines = [i.title.text for i in soup.find_all("item")][:5]
        if headlines:
            return headlines
    except:
        pass
    return SYNTHETIC.get(symbol, ["company reports quarterly results"])

def predict(text):
    words = set(text.lower().split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos > neg:
        return {"text": text, "sentiment": "POSITIVE", "score_negative": round(1-pos/(pos+neg+0.1),4), "score_neutral": 0.0, "score_positive": round(pos/(pos+neg+0.1),4), "confidence": round(pos/(pos+neg+0.1),4)}
    elif neg > pos:
        return {"text": text, "sentiment": "NEGATIVE", "score_negative": round(neg/(pos+neg+0.1),4), "score_neutral": 0.0, "score_positive": round(1-neg/(pos+neg+0.1),4), "confidence": round(neg/(pos+neg+0.1),4)}
    return {"text": text, "sentiment": "NEUTRAL", "score_negative": 0.33, "score_neutral": 0.34, "score_positive": 0.33, "confidence": 0.5}

def save(results):
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS sentiment_scores (
        id BIGSERIAL PRIMARY KEY, symbol VARCHAR(10) NOT NULL,
        headline TEXT NOT NULL, sentiment VARCHAR(10) NOT NULL,
        score_neg NUMERIC(6,4), score_neu NUMERIC(6,4), score_pos NUMERIC(6,4),
        confidence NUMERIC(6,4), analyzed_at TIMESTAMPTZ DEFAULT NOW())""")
    for r in results:
        cur.execute("INSERT INTO sentiment_scores (symbol,headline,sentiment,score_neg,score_neu,score_pos,confidence) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (r["symbol"],r["text"],r["sentiment"],r["score_negative"],r["score_neutral"],r["score_positive"],r["confidence"]))
    conn.commit(); cur.close(); conn.close()
    log.info(f"Έγραψα {len(results)} scores")

def main():
    log.info("="*50)
    log.info("  FinSight Sentiment Analysis (Fast)")
    log.info("="*50)
    SYMBOLS = get_active_symbols()
    results = []
    for symbol in SYMBOLS:
        for headline in fetch_headlines(symbol):
            r = predict(headline)
            r["symbol"] = symbol
            results.append(r)
            icon = "🟢" if r["sentiment"]=="POSITIVE" else "🔴" if r["sentiment"]=="NEGATIVE" else "🟡"
            log.info(f"{icon} {symbol}: [{r['sentiment']:8s}] {headline[:50]}")
    save(results)
    log.info("Done!")

if __name__ == "__main__":
    main()