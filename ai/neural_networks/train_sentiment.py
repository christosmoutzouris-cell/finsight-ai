"""
Neural Network για Sentiment Analysis

Διαφορά από rule-based:
- Μαθαίνει patterns από data (όχι hardcoded keywords)
- Καταλαβαίνει context ("not good" ≠ "good")
- Αποθηκεύει model → επόμενες φορές φορτώνει χωρίς retraining

Αρχιτεκτονική:
Input (text) → Embedding → LSTM → Linear → Softmax → POSITIVE/NEUTRAL/NEGATIVE
"""

import os
import json
import random
import logging
import psycopg2
import torch
import torch.nn as nn
import torch.optim as optim
import sys
sys.path.append("/app")
from common.db_utils import get_active_symbols, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SENTIMENT-NN] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

MODEL_PATH = "/app/models/sentiment_nn.pt"
VOCAB_PATH = "/app/models/sentiment_vocab.json"
os.makedirs("/app/models", exist_ok=True)

# ── Training Data ──────────────────────────────────────────────────────────
TRAINING_DATA = [
    # POSITIVE (2)
    ("stock surges to record high amid strong earnings", 2),
    ("company beats revenue expectations significantly", 2),
    ("shares jump after better than expected results", 2),
    ("profit grows substantially year over year", 2),
    ("strong demand drives revenue growth", 2),
    ("company raises guidance for full year", 2),
    ("analysts upgrade stock to buy rating", 2),
    ("record quarterly earnings reported", 2),
    ("market cap reaches all time high", 2),
    ("dividend increased by board of directors", 2),
    ("breakthrough product launch drives sales", 2),
    ("partnership agreement boosts market position", 2),
    ("investor confidence grows amid positive outlook", 2),
    ("revenue exceeds analyst estimates", 2),
    ("stock rallies on positive news", 2),
    ("earnings beat sends stock higher", 2),
    ("company reports record breaking revenue", 2),
    ("strong sales growth exceeds forecasts", 2),
    ("buyback program boosts shareholder value", 2),
    ("company expands into new profitable markets", 2),

    # NEUTRAL (1)
    ("company announces quarterly earnings report", 1),
    ("stock trades sideways in mixed market", 1),
    ("analysts maintain hold rating on shares", 1),
    ("company to release results next week", 1),
    ("market closes flat amid uncertainty", 1),
    ("shares unchanged after earnings announcement", 1),
    ("company files annual report with regulators", 1),
    ("board of directors meets to discuss strategy", 1),
    ("company announces new product roadmap", 1),
    ("stock volume below average today", 1),
    ("market awaits federal reserve decision", 1),
    ("company completes acquisition as planned", 1),
    ("shares trade in narrow range", 1),
    ("analyst initiates coverage with neutral rating", 1),
    ("company reaffirms annual guidance", 1),
    ("executives present at industry conference", 1),
    ("company releases new software update", 1),
    ("market opens mixed ahead of data", 1),
    ("stock consolidates after recent moves", 1),
    ("company hires new chief financial officer", 1),

    # NEGATIVE (0)
    ("stock plunges after disappointing results", 0),
    ("company misses earnings expectations badly", 0),
    ("revenue falls short of analyst estimates", 0),
    ("shares tumble on weak guidance", 0),
    ("company reports significant loss", 0),
    ("profit warning issued by management", 0),
    ("analysts downgrade stock to sell", 0),
    ("layoffs announced amid restructuring", 0),
    ("company faces regulatory investigation", 0),
    ("debt levels raise investor concerns", 0),
    ("market share lost to competitors", 0),
    ("supply chain disruptions hurt performance", 0),
    ("company cuts dividend amid losses", 0),
    ("stock hits 52 week low", 0),
    ("earnings collapse drives sell off", 0),
    ("company faces bankruptcy concerns", 0),
    ("revenue decline accelerates amid competition", 0),
    ("massive write down hits quarterly results", 0),
    ("company loses major customer contract", 0),
    ("fraud allegations surface hurting stock", 0),
]


# ── Model ──────────────────────────────────────────────────────────────────
class SentimentNN(nn.Module):
    def __init__(self, vocab_size, embed_dim=64, hidden_dim=128, num_classes=3):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim, padding_idx=0)
        self.lstm      = nn.LSTM(embed_dim, hidden_dim, batch_first=True,
                                  num_layers=2, dropout=0.3)
        self.dropout   = nn.Dropout(0.3)
        self.fc        = nn.Linear(hidden_dim, num_classes)

    def forward(self, x):
        embedded        = self.embedding(x)
        _, (hidden, _)  = self.lstm(embedded)
        out             = self.dropout(hidden[-1])
        return self.fc(out)


# ── Vocabulary ─────────────────────────────────────────────────────────────
def build_vocab(data):
    vocab = {"<PAD>": 0, "<UNK>": 1}
    for text, _ in data:
        for word in text.lower().split():
            if word not in vocab:
                vocab[word] = len(vocab)
    return vocab


def tokenize(text, vocab, max_len=20):
    tokens = [vocab.get(w, 1) for w in text.lower().split()][:max_len]
    return tokens + [0] * (max_len - len(tokens))


# ── Training ───────────────────────────────────────────────────────────────
def train(model, data, vocab, epochs=30):
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    model.train()

    for epoch in range(epochs):
        random.shuffle(data)
        total_loss, correct = 0, 0
        for text, label in data:
            x = torch.tensor([tokenize(text, vocab)], dtype=torch.long)
            y = torch.tensor([label], dtype=torch.long)
            optimizer.zero_grad()
            loss = criterion(model(x), y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            correct += (model(x).argmax(1) == y).sum().item()

        if (epoch + 1) % 10 == 0:
            acc = correct / len(data) * 100
            log.info(f"Epoch {epoch+1}/{epochs} — Loss: {total_loss/len(data):.4f}, Acc: {acc:.1f}%")

    return model


# ── Save / Load ────────────────────────────────────────────────────────────
def save_model(model, vocab):
    torch.save(model.state_dict(), MODEL_PATH)
    with open(VOCAB_PATH, "w") as f:
        json.dump(vocab, f)
    log.info(f"✓ Model αποθηκεύτηκε στο {MODEL_PATH}")


def load_model(vocab):
    model = SentimentNN(vocab_size=len(vocab))
    model.load_state_dict(torch.load(MODEL_PATH))
    model.eval()
    log.info(f"✓ Model φορτώθηκε από {MODEL_PATH}")
    return model


# ── Predict & Save results ─────────────────────────────────────────────────
def predict_and_save(model, vocab):
    """
    Διαβάζει τα τελευταία sentiment scores (rule-based)
    και τα επαναξιολογεί με το NN.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    # Πάρε τα τελευταία headlines από sentiment_scores
    cursor.execute("""
        SELECT id, symbol, headline
        FROM sentiment_scores
        WHERE analyzed_at > NOW() - INTERVAL '1 day'
        ORDER BY analyzed_at DESC
    """)
    rows = cursor.fetchall()

    if not rows:
        log.warning("Δεν υπάρχουν πρόσφατα headlines")
        cursor.close(); conn.close()
        return

    # Δημιούργησε πίνακα για NN predictions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_nn_scores (
            id              BIGSERIAL PRIMARY KEY,
            sentiment_id    BIGINT,
            symbol          VARCHAR(10),
            headline        TEXT,
            nn_sentiment    VARCHAR(10),
            nn_confidence   NUMERIC(6,4),
            score_negative  NUMERIC(6,4),
            score_neutral   NUMERIC(6,4),
            score_positive  NUMERIC(6,4),
            analyzed_at     TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    labels  = ["NEGATIVE", "NEUTRAL", "POSITIVE"]
    results = []

    model.eval()
    with torch.no_grad():
        for row_id, symbol, headline in rows:
            tokens = tokenize(headline, vocab)
            x      = torch.tensor([tokens], dtype=torch.long)
            probs  = torch.softmax(model(x), dim=1)[0]
            pred   = probs.argmax().item()

            result = {
                "sentiment_id":   row_id,
                "symbol":         symbol,
                "headline":       headline,
                "nn_sentiment":   labels[pred],
                "nn_confidence":  round(probs[pred].item(), 4),
                "score_negative": round(probs[0].item(), 4),
                "score_neutral":  round(probs[1].item(), 4),
                "score_positive": round(probs[2].item(), 4),
            }
            results.append(result)

            icon = "🟢" if labels[pred] == "POSITIVE" else \
                   "🔴" if labels[pred] == "NEGATIVE" else "🟡"
            log.info(f"{icon} {symbol}: [{labels[pred]:8s}] "
                     f"(conf={probs[pred].item():.2f}) {headline[:50]}")

    # Insert results
    for r in results:
        cursor.execute("""
            INSERT INTO sentiment_nn_scores
                (sentiment_id, symbol, headline, nn_sentiment,
                 nn_confidence, score_negative, score_neutral, score_positive)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (r["sentiment_id"], r["symbol"], r["headline"],
              r["nn_sentiment"], r["nn_confidence"],
              r["score_negative"], r["score_neutral"], r["score_positive"]))

    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"✓ Έγραψα {len(results)} NN sentiment scores")


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("  Sentiment Neural Network")
    log.info("=" * 50)

    vocab = build_vocab(TRAINING_DATA)
    log.info(f"Vocabulary: {len(vocab)} words")

    # Αν υπάρχει ήδη trained model → φόρτωσέ το
    if os.path.exists(MODEL_PATH) and os.path.exists(VOCAB_PATH):
        with open(VOCAB_PATH) as f:
            vocab = json.load(f)
        model = load_model(vocab)
        log.info("Χρησιμοποιώ existing model — παράλειψη training")
    else:
        log.info("Δεν υπάρχει saved model — ξεκινάω training...")
        model = SentimentNN(vocab_size=len(vocab))
        model = train(model, TRAINING_DATA, vocab, epochs=30)
        save_model(model, vocab)

    # Predict με το trained model
    predict_and_save(model, vocab)

    log.info("=" * 50)
    log.info("  Sentiment NN Complete")
    log.info("=" * 50)


if __name__ == "__main__":
    main()