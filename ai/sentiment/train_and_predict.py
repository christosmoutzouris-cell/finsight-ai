"""
Sentiment Analysis Neural Network για Financial News

Αρχιτεκτονική:
- Embedding layer: μετατρέπει λέξεις σε vectors
- LSTM layer: καταλαβαίνει σειρά και context
- Linear layer: βγάζει 3 κλάσεις (positive/neutral/negative)
- Softmax: μετατρέπει σε πιθανότητες

Γιατί LSTM και όχι απλό NN;
Το "stock price UP" και το "stock price NOT UP" έχουν
αντίθετο νόημα — το LSTM θυμάται τη σειρά των λέξεων.
"""

import os
import json
import random
import logging
import psycopg2
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import sys
import os
import logging
from common.db_utils import get_active_symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SENTIMENT] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── PostgreSQL config ──────────────────────────────────────────────────────
PG_CONN = {
    "host":     "finsight-postgres",
    "port":     5432,
    "user":     "finsight",
    "password": "finsight123",
    "dbname":   "finsight_db",
}


SYMBOLS = get_active_symbols()

# ══════════════════════════════════════════════════════════════════════════
# TRAINING DATA
# Χρησιμοποιούμε labeled financial headlines για training.
# Σε production θα χρησιμοποιούσες μεγαλύτερο dataset (FinancialPhraseBank).
# ══════════════════════════════════════════════════════════════════════════

TRAINING_DATA = [
    # POSITIVE (label=2)
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

    # NEUTRAL (label=1)
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

    # NEGATIVE (label=0)
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
]


# ══════════════════════════════════════════════════════════════════════════
# VOCABULARY & TOKENIZATION
# ══════════════════════════════════════════════════════════════════════════

def build_vocab(data):
    """
    Φτιάχνει vocabulary από τα training data.
    Κάθε μοναδική λέξη παίρνει ένα μοναδικό αριθμό (index).
    """
    vocab = {"<PAD>": 0, "<UNK>": 1}  # PAD=padding, UNK=unknown word
    for text, _ in data:
        for word in text.lower().split():
            if word not in vocab:
                vocab[word] = len(vocab)
    return vocab


def tokenize(text: str, vocab: dict, max_len: int = 20) -> list[int]:
    """
    Μετατρέπει κείμενο σε λίστα από αριθμούς.
    π.χ. "stock surges" → [45, 23]
    Padding: συμπληρώνει με 0 αν το κείμενο είναι κοντό.
    """
    tokens = [vocab.get(w, 1) for w in text.lower().split()]
    # Truncate αν είναι πολύ μακρύ
    tokens = tokens[:max_len]
    # Padding αν είναι πολύ κοντό
    tokens = tokens + [0] * (max_len - len(tokens))
    return tokens


# ══════════════════════════════════════════════════════════════════════════
# NEURAL NETWORK ARCHITECTURE
# ══════════════════════════════════════════════════════════════════════════

class SentimentLSTM(nn.Module):
    """
    Αρχιτεκτονική:

    Input (token ids)
        ↓
    Embedding Layer
    Μετατρέπει κάθε token id σε dense vector (64 dimensions).
    Π.χ. "stock" → [0.2, -0.5, 0.8, ...]
        ↓
    LSTM Layer
    Διαβάζει τη σειρά των embeddings και κρατάει "μνήμη".
    Βγάζει hidden state που αναπαριστά το νόημα της πρότασης.
        ↓
    Dropout (regularization)
    Τυχαία "σβήνει" neurons κατά το training για να αποφύγει overfitting.
        ↓
    Linear Layer
    Μετατρέπει το hidden state σε 3 τιμές (positive/neutral/negative).
        ↓
    Output: [score_negative, score_neutral, score_positive]
    """
    def __init__(self, vocab_size: int, embed_dim: int = 64,
                 hidden_dim: int = 128, num_classes: int = 3):
        super().__init__()

        self.embedding = nn.Embedding(
            vocab_size, embed_dim, padding_idx=0
        )
        self.lstm = nn.LSTM(
            embed_dim, hidden_dim,
            batch_first=True,
            num_layers=2,        # 2 στρώματα LSTM
            dropout=0.3
        )
        self.dropout = nn.Dropout(0.3)
        self.fc = nn.Linear(hidden_dim, num_classes)

    def forward(self, x):
        # x shape: (batch_size, seq_len)
        embedded = self.embedding(x)
        # embedded shape: (batch_size, seq_len, embed_dim)

        lstm_out, (hidden, _) = self.lstm(embedded)
        # Παίρνουμε το hidden state του τελευταίου layer
        out = hidden[-1]
        # out shape: (batch_size, hidden_dim)

        out = self.dropout(out)
        out = self.fc(out)
        # out shape: (batch_size, num_classes)
        return out


# ══════════════════════════════════════════════════════════════════════════
# TRAINING
# ══════════════════════════════════════════════════════════════════════════

def train_model(model, train_data, vocab, epochs=50):
    """
    Εκπαιδεύει το μοντέλο.

    Loss function: CrossEntropyLoss
    Μετράει πόσο "λάθος" είναι η πρόβλεψη.

    Optimizer: Adam
    Ενημερώνει τα weights για να μειώσει το loss.
    """
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    model.train()
    log.info(f"Ξεκινάω training για {epochs} epochs...")

    for epoch in range(epochs):
        total_loss = 0
        correct = 0

        # Shuffle training data σε κάθε epoch
        random.shuffle(train_data)

        for text, label in train_data:
            tokens = tokenize(text, vocab)
            x = torch.tensor([tokens], dtype=torch.long)
            y = torch.tensor([label], dtype=torch.long)

            optimizer.zero_grad()       # Μηδενίζει τα gradients
            output = model(x)           # Forward pass
            loss = criterion(output, y) # Υπολογίζει loss
            loss.backward()             # Backpropagation
            optimizer.step()            # Ενημερώνει weights

            total_loss += loss.item()
            pred = output.argmax(dim=1)
            correct += (pred == y).sum().item()

        if (epoch + 1) % 10 == 0:
            acc = correct / len(train_data) * 100
            avg_loss = total_loss / len(train_data)
            log.info(f"Epoch {epoch+1}/{epochs} — Loss: {avg_loss:.4f}, Accuracy: {acc:.1f}%")

    log.info("Training complete!")
    return model


# ══════════════════════════════════════════════════════════════════════════
# NEWS FETCHING
# ══════════════════════════════════════════════════════════════════════════

def fetch_news_headlines(symbol: str) -> list[str]:
    """
    Τραβάει financial news headlines από Yahoo Finance RSS.
    Αν αποτύχει, επιστρέφει synthetic headlines για demo.
    """
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, "xml")
        headlines = [item.title.text for item in soup.find_all("item")][:5]
        if headlines:
            log.info(f"{symbol}: Βρήκα {len(headlines)} headlines")
            return headlines
    except Exception as e:
        log.warning(f"{symbol}: RSS failed ({e}) — χρησιμοποιώ synthetic data")

    # Synthetic headlines για demo/testing
    synthetic = {
        "AAPL": [
            "Apple reports record iPhone sales this quarter",
            "Apple stock rises on strong services revenue",
            "Apple faces antitrust scrutiny in Europe",
        ],
        "MSFT": [
            "Microsoft Azure cloud revenue surges",
            "Microsoft AI investments drive growth",
            "Microsoft beats earnings expectations",
        ],
        "GOOGL": [
            "Google advertising revenue grows steadily",
            "Alphabet reports strong quarterly results",
            "Google faces regulatory challenges globally",
        ],
        "AMZN": [
            "Amazon AWS maintains cloud market leadership",
            "Amazon prime membership hits record high",
            "Amazon reports mixed quarterly results",
        ],
        "NVDA": [
            "NVIDIA data center revenue reaches new high",
            "NVIDIA AI chip demand remains strong",
            "NVIDIA stock surges on positive outlook",
        ],
    }
    return synthetic.get(symbol, ["company reports quarterly results"])


# ══════════════════════════════════════════════════════════════════════════
# PREDICTION & STORAGE
# ══════════════════════════════════════════════════════════════════════════

def predict_sentiment(model, text: str, vocab: dict) -> dict:
    """
    Προβλέπει sentiment για ένα headline.
    Επιστρέφει dict με scores και label.
    """
    model.eval()
    with torch.no_grad():
        tokens = tokenize(text, vocab)
        x = torch.tensor([tokens], dtype=torch.long)
        output = model(x)
        probs = torch.softmax(output, dim=1)[0]

        labels = ["NEGATIVE", "NEUTRAL", "POSITIVE"]
        pred_idx = probs.argmax().item()

        return {
            "text":             text,
            "sentiment":        labels[pred_idx],
            "score_negative":   round(probs[0].item(), 4),
            "score_neutral":    round(probs[1].item(), 4),
            "score_positive":   round(probs[2].item(), 4),
            "confidence":       round(probs[pred_idx].item(), 4),
        }


def save_to_postgres(results: list[dict]):
    """Αποθηκεύει τα αποτελέσματα στη PostgreSQL."""
    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_scores (
            id           BIGSERIAL PRIMARY KEY,
            symbol       VARCHAR(10)    NOT NULL,
            headline     TEXT           NOT NULL,
            sentiment    VARCHAR(10)    NOT NULL,
            score_neg    NUMERIC(6,4),
            score_neu    NUMERIC(6,4),
            score_pos    NUMERIC(6,4),
            confidence   NUMERIC(6,4),
            analyzed_at  TIMESTAMPTZ    DEFAULT NOW()
        );
    """)

    for r in results:
        cursor.execute("""
            INSERT INTO sentiment_scores
                (symbol, headline, sentiment, score_neg, score_neu, score_pos, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            r["symbol"], r["text"], r["sentiment"],
            r["score_negative"], r["score_neutral"],
            r["score_positive"], r["confidence"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"Έγραψα {len(results)} sentiment scores στη PostgreSQL")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 50)
    log.info("  FinSight Sentiment Analysis")
    log.info("=" * 50)

    # 1. Build vocabulary
    vocab = build_vocab(TRAINING_DATA)
    log.info(f"Vocabulary size: {len(vocab)} words")

    # 2. Build και train model
    model = SentimentLSTM(vocab_size=len(vocab))
    model = train_model(model, TRAINING_DATA, vocab, epochs=50)

    # 3. Fetch news και predict
    all_results = []
    log.info("\nΑνάλυση headlines ανά symbol:")
    log.info("-" * 50)

    for symbol in SYMBOLS:
        headlines = fetch_news_headlines(symbol)
        for headline in headlines:
            result = predict_sentiment(model, headline, vocab)
            result["symbol"] = symbol
            all_results.append(result)

            icon = "🟢" if result["sentiment"] == "POSITIVE" else \
                   "🔴" if result["sentiment"] == "NEGATIVE" else "🟡"
            log.info(
                f"{icon} {symbol}: [{result['sentiment']:8s}] "
                f"(conf={result['confidence']:.2f}) {headline[:50]}..."
            )

    # 4. Save to PostgreSQL
    save_to_postgres(all_results)

    # 5. Summary
    log.info("\n" + "=" * 50)
    log.info("  SENTIMENT SUMMARY")
    log.info("=" * 50)
    for symbol in SYMBOLS:
        symbol_results = [r for r in all_results if r["symbol"] == symbol]
        avg_pos = sum(r["score_positive"] for r in symbol_results) / len(symbol_results)
        dominant = max(
            ["POSITIVE", "NEUTRAL", "NEGATIVE"],
            key=lambda s: sum(1 for r in symbol_results if r["sentiment"] == s)
        )
        log.info(f"  {symbol:5s}: {dominant:8s} (avg positive score: {avg_pos:.2f})")


if __name__ == "__main__":
    main()