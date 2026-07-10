"""
LSTM Price Prediction για FinSight AI

Αρχιτεκτονική:
- Input: 20 διαδοχικές τιμές (sequence)
- LSTM layers: μαθαίνουν temporal patterns
- Output: 1 τιμή (η επόμενη)

Γιατί normalization;
Οι τιμές AAPL (~300$) και NVDA (~200$) είναι σε διαφορετική κλίμακα.
Κανονικοποιούμε στο [0,1] για να μαθαίνει το NN ευκολότερα.
"""

import logging
import psycopg2
import numpy as np
import pandas as pd
from datetime import datetime, timezone

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import MinMaxScaler
import sys
from common.db_utils import get_active_symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LSTM] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
PG_CONN = {
    "host":     "finsight-postgres",
    "port":     5432,
    "user":     "finsight",
    "password": "finsight123",
    "dbname":   "finsight_db",
}


SYMBOLS = get_active_symbols()
SEQUENCE_LEN = 20      # πόσες τιμές κοιτάει πίσω
EPOCHS       = 30
BATCH_SIZE   = 64
HIDDEN_DIM   = 64
NUM_LAYERS   = 2
LEARNING_RATE = 0.001


# ══════════════════════════════════════════════════════════════════════════
# NEURAL NETWORK
# ══════════════════════════════════════════════════════════════════════════

class PriceLSTM(nn.Module):
    """
    Input:  (batch_size, sequence_len, 1)  ← 20 κανονικοποιημένες τιμές
    Output: (batch_size, 1)                ← 1 πρόβλεψη

    Layers:
    1. LSTM: μαθαίνει temporal patterns
    2. Dropout: αποτρέπει overfitting
    3. Linear: μετατρέπει hidden state σε τιμή
    """
    def __init__(self, input_dim=1, hidden_dim=HIDDEN_DIM,
                 num_layers=NUM_LAYERS, output_dim=1):
        super().__init__()
        self.lstm = nn.LSTM(
            input_dim, hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2
        )
        self.dropout = nn.Dropout(0.2)
        self.fc      = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        # Παίρνουμε μόνο το τελευταίο output (many-to-one)
        out = self.dropout(lstm_out[:, -1, :])
        return self.fc(out)


# ══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════

def load_prices(symbol: str) -> pd.DataFrame:
    """Φορτώνει ιστορικές τιμές από Silver layer."""
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT event_time, price
        FROM stock_prices_silver
        WHERE symbol = %s
        ORDER BY event_time ASC
        LIMIT 4000
    """, conn, params=(symbol,))
    conn.close()
    return df


def create_sequences(prices: np.ndarray, seq_len: int):
    """
    Μετατρέπει series τιμών σε sequences για το LSTM.

    Παράδειγμα με seq_len=3:
    [1,2,3,4,5] → X=[[1,2,3],[2,3,4]], y=[4,5]

    Δηλαδή: "δοσμένες οι τιμές 1,2,3 → πρόβλεψε την 4"
    """
    X, y = [], []
    for i in range(len(prices) - seq_len):
        X.append(prices[i:i + seq_len])
        y.append(prices[i + seq_len])
    return np.array(X), np.array(y)


# ══════════════════════════════════════════════════════════════════════════
# TRAINING
# ══════════════════════════════════════════════════════════════════════════

def train_model(X_train, y_train) -> PriceLSTM:
    """Εκπαιδεύει το LSTM model."""
    # Μετατροπή σε PyTorch tensors
    X_t = torch.FloatTensor(X_train).unsqueeze(-1)  # (N, seq, 1)
    y_t = torch.FloatTensor(y_train).unsqueeze(-1)  # (N, 1)

    dataset = TensorDataset(X_t, y_t)
    loader  = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

    model     = PriceLSTM()
    criterion = nn.MSELoss()  # Mean Squared Error για regression
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

    model.train()
    for epoch in range(EPOCHS):
        total_loss = 0
        for X_batch, y_batch in loader:
            optimizer.zero_grad()
            output = model(X_batch)
            loss   = criterion(output, y_batch)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        if (epoch + 1) % 10 == 0:
            avg_loss = total_loss / len(loader)
            log.info(f"Epoch {epoch+1}/{EPOCHS} — Loss: {avg_loss:.6f}")

    return model


# ══════════════════════════════════════════════════════════════════════════
# PREDICTION & STORAGE
# ══════════════════════════════════════════════════════════════════════════

def predict_next_price(model, last_sequence, scaler) -> float:
    """
    Προβλέπει την επόμενη τιμή.
    last_sequence: οι τελευταίες SEQUENCE_LEN κανονικοποιημένες τιμές
    """
    model.eval()
    with torch.no_grad():
        x = torch.FloatTensor(last_sequence).unsqueeze(0).unsqueeze(-1)
        pred_normalized = model(x).item()

    # Αντίστροφη κανονικοποίηση → πραγματική τιμή σε $
    pred_price = scaler.inverse_transform([[pred_normalized]])[0][0]
    return round(float(pred_price), 4)


def save_predictions(predictions: list[dict]):
    """Αποθηκεύει τις προβλέψεις στη PostgreSQL."""
    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_predictions (
            id              BIGSERIAL PRIMARY KEY,
            symbol          VARCHAR(10)    NOT NULL,
            current_price   NUMERIC(12,4)  NOT NULL,
            predicted_price NUMERIC(12,4)  NOT NULL,
            predicted_change NUMERIC(8,4),
            predicted_direction VARCHAR(4),
            model_type      VARCHAR(20)    DEFAULT 'LSTM',
            predicted_at    TIMESTAMPTZ    DEFAULT NOW()
        );
    """)

    for p in predictions:
        cursor.execute("""
            INSERT INTO price_predictions
                (symbol, current_price, predicted_price,
                 predicted_change, predicted_direction)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            p["symbol"],
            p["current_price"],
            p["predicted_price"],
            p["predicted_change"],
            p["predicted_direction"],
        ))

    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"Έγραψα {len(predictions)} predictions στη PostgreSQL")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 50)
    log.info("  FinSight LSTM Price Prediction")
    log.info("=" * 50)

    all_predictions = []

    for symbol in SYMBOLS:
        log.info(f"\n{'─'*40}")
        log.info(f"Processing {symbol}...")

        # 1. Load data
        df = load_prices(symbol)
        if len(df) < SEQUENCE_LEN + 50:
            log.warning(f"{symbol}: ανεπαρκή δεδομένα ({len(df)} rows)")
            continue

        prices = df["price"].values.reshape(-1, 1)
        log.info(f"  Φόρτωσα {len(prices)} τιμές")

        # 2. Normalize στο [0,1]
        scaler = MinMaxScaler(feature_range=(0, 1))
        prices_scaled = scaler.fit_transform(prices).flatten()

        # 3. Δημιούργησε sequences
        X, y = create_sequences(prices_scaled, SEQUENCE_LEN)

        # 4. Train/test split (80/20)
        split     = int(len(X) * 0.8)
        X_train   = X[:split]
        y_train   = y[:split]
        X_test    = X[split:]
        y_test    = y[split:]

        log.info(f"  Train: {len(X_train)} sequences, Test: {len(X_test)} sequences")

        # 5. Train
        model = train_model(X_train, y_train)

        # 6. Evaluate στο test set
        model.eval()
        with torch.no_grad():
            X_test_t  = torch.FloatTensor(X_test).unsqueeze(-1)
            y_pred_t  = model(X_test_t).numpy().flatten()
            y_test_t  = y_test

        # MAE σε κανονικοποιημένες τιμές
        mae = np.mean(np.abs(y_pred_t - y_test_t))
        log.info(f"  MAE (normalized): {mae:.6f}")

        # 7. Πρόβλεψη επόμενης τιμής
        last_sequence   = prices_scaled[-SEQUENCE_LEN:]
        current_price   = float(scaler.inverse_transform([[prices_scaled[-1]]])[0][0])
        predicted_price = predict_next_price(model, last_sequence, scaler)
        predicted_change = round(
            (predicted_price - current_price) / current_price * 100, 4
        )
        direction = "UP" if predicted_price > current_price else "DOWN"

        log.info(f"  Current:   ${current_price:.2f}")
        log.info(f"  Predicted: ${predicted_price:.2f} ({predicted_change:+.2f}%) {direction}")

        all_predictions.append({
            "symbol":            symbol,
            "current_price":     current_price,
            "predicted_price":   predicted_price,
            "predicted_change":  predicted_change,
            "predicted_direction": direction,
        })

    # 8. Save
    save_predictions(all_predictions)

    # 9. Summary
    log.info("\n" + "=" * 50)
    log.info("  PREDICTION SUMMARY")
    log.info("=" * 50)
    for p in all_predictions:
        icon = "📈" if p["predicted_direction"] == "UP" else "📉"
        log.info(
            f"  {icon} {p['symbol']:5s}: "
            f"${p['current_price']:.2f} → ${p['predicted_price']:.2f} "
            f"({p['predicted_change']:+.2f}%)"
        )


if __name__ == "__main__":
    main()