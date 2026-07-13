"""
Neural Network για Price Direction Prediction

Διαφορά από LSTM price prediction:
- LSTM: προβλέπει ακριβή τιμή ($307.66)
- Αυτό NN: προβλέπει κατεύθυνση (UP/DOWN) — πιο χρήσιμο στην πράξη

Features που χρησιμοποιεί:
- price_change: αλλαγή τιμής
- pct_change: % αλλαγή
- ma5, ma20: moving averages
- ma5_signal: BULLISH/BEARISH
- volume: όγκος συναλλαγών

Αρχιτεκτονική:
[6 features] → Linear(64) → ReLU → Dropout → Linear(32) → ReLU → Linear(2) → UP/DOWN
"""

import os
import json
import logging
import psycopg2
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import sys
sys.path.append("/app")
from common.db_utils import get_active_symbols, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRICE-DIR-NN] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

MODEL_PATH  = "/app/models/price_direction_nn.pt"
SCALER_PATH = "/app/models/price_direction_scaler.json"
os.makedirs("/app/models", exist_ok=True)


# ── Model ──────────────────────────────────────────────────────────────────
class PriceDirectionNN(nn.Module):
    """
    Feedforward Neural Network — απλούστερο από LSTM γιατί
    χρησιμοποιεί aggregated features αντί για sequences.

    Κάθε Linear layer μαθαίνει διαφορετικό επίπεδο abstraction:
    Layer 1: βασικές σχέσεις (price_change vs volume)
    Layer 2: σύνθετα patterns
    Layer 3: τελική απόφαση UP/DOWN
    """
    def __init__(self, input_dim=6):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(32, 2),   # 2 κλάσεις: DOWN(0), UP(1)
        )

    def forward(self, x):
        return self.net(x)


# ── Data Loading ───────────────────────────────────────────────────────────
def load_training_data():
    """
    Φορτώνει δεδομένα από silver layer για training.
    Features: price_change, pct_change, ma5, ma20, volume, ma5_vs_ma20
    Label: 1 αν η επόμενη τιμή > τρέχουσα, αλλιώς 0
    """
    conn = get_connection()
    df_rows = []

    symbols = get_active_symbols()
    for symbol in symbols:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT price, price_change, pct_change, ma5, ma20, volume
            FROM stock_prices_silver
            WHERE symbol = %s
            AND price_change IS NOT NULL
            AND ma5 IS NOT NULL
            AND ma20 IS NOT NULL
            ORDER BY event_time ASC
            LIMIT 3000
        """, (symbol,))
        rows = cursor.fetchall()
        cursor.close()

        for i in range(len(rows) - 1):
            price, price_change, pct_change, ma5, ma20, volume = rows[i]
            next_price = rows[i + 1][0]

            # Feature: διαφορά ma5 από ma20 (normalized)
            ma5_vs_ma20 = float(ma5 - ma20) / float(ma20) if ma20 else 0

            label = 1 if next_price > price else 0   # UP=1, DOWN=0

            df_rows.append([
                float(price_change or 0),
                float(pct_change or 0),
                float(ma5 or 0) / float(price),      # normalized
                float(ma20 or 0) / float(price),     # normalized
                float(volume or 0) / 1_000_000,      # σε εκατομμύρια
                ma5_vs_ma20,
                label
            ])

    conn.close()
    log.info(f"Φόρτωσα {len(df_rows)} training samples από {len(symbols)} symbols")
    return np.array(df_rows)


# ── Training ───────────────────────────────────────────────────────────────
def train(model, X_train, y_train, epochs=20):
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    X_t = torch.FloatTensor(X_train)
    y_t = torch.LongTensor(y_train)

    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        output = model(X_t)
        loss   = criterion(output, y_t)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 5 == 0:
            acc = (output.argmax(1) == y_t).float().mean().item() * 100
            log.info(f"Epoch {epoch+1}/{epochs} — Loss: {loss.item():.4f}, Acc: {acc:.1f}%")

    return model


# ── Save / Load ────────────────────────────────────────────────────────────
def save_model(model, scaler_mean, scaler_scale):
    torch.save(model.state_dict(), MODEL_PATH)
    with open(SCALER_PATH, "w") as f:
        json.dump({"mean": scaler_mean.tolist(),
                   "scale": scaler_scale.tolist()}, f)
    log.info(f"✓ Model αποθηκεύτηκε")


def load_model(scaler_mean, scaler_scale):
    model = PriceDirectionNN()
    model.load_state_dict(torch.load(MODEL_PATH))
    model.eval()
    log.info(f"✓ Model φορτώθηκε από {MODEL_PATH}")
    return model


# ── Predict ────────────────────────────────────────────────────────────────
def predict_and_save(model, scaler_mean, scaler_scale):
    """Προβλέπει direction για τις τελευταίες τιμές κάθε symbol."""
    conn    = get_connection()
    cursor  = conn.cursor()
    symbols = get_active_symbols()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_direction_predictions (
            id              BIGSERIAL PRIMARY KEY,
            symbol          VARCHAR(10),
            current_price   NUMERIC(12,4),
            predicted_dir   VARCHAR(4),
            confidence      NUMERIC(6,4),
            prob_up         NUMERIC(6,4),
            prob_down       NUMERIC(6,4),
            predicted_at    TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    log.info("\nPrice Direction Predictions:")
    log.info("-" * 50)

    for symbol in symbols:
        cursor.execute("""
            SELECT price, price_change, pct_change, ma5, ma20, volume
            FROM stock_prices_silver
            WHERE symbol = %s
            AND price_change IS NOT NULL
            AND ma5 IS NOT NULL
            ORDER BY event_time DESC
            LIMIT 1
        """, (symbol,))
        row = cursor.fetchone()
        if not row:
            continue

        price, price_change, pct_change, ma5, ma20, volume = row
        ma5_vs_ma20 = float(ma5 - ma20) / float(ma20) if ma20 else 0

        features = np.array([[
            float(price_change or 0),
            float(pct_change or 0),
            float(ma5 or 0) / float(price),
            float(ma20 or 0) / float(price),
            float(volume or 0) / 1_000_000,
            ma5_vs_ma20
        ]])

        # Normalize με saved scaler
        features_norm = (features - scaler_mean) / scaler_scale

        model.eval()
        with torch.no_grad():
            x     = torch.FloatTensor(features_norm)
            probs = torch.softmax(model(x), dim=1)[0]

        pred       = probs.argmax().item()
        direction  = "UP" if pred == 1 else "DOWN"
        confidence = round(probs[pred].item(), 4)
        prob_up    = round(probs[1].item(), 4)
        prob_down  = round(probs[0].item(), 4)

        cursor.execute("""
            INSERT INTO price_direction_predictions
                (symbol, current_price, predicted_dir, confidence, prob_up, prob_down)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (symbol, float(price), direction, confidence, prob_up, prob_down))

        icon = "📈" if direction == "UP" else "📉"
        log.info(f"{icon} {symbol}: {direction} (conf={confidence:.2f}, "
                 f"up={prob_up:.2f}, down={prob_down:.2f})")

    conn.commit()
    cursor.close()
    conn.close()


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 50)
    log.info("  Price Direction Neural Network")
    log.info("=" * 50)

    # Load data
    data = load_training_data()
    if len(data) < 100:
        log.error("Ανεπαρκή δεδομένα για training")
        return

    X = data[:, :-1]
    y = data[:, -1].astype(int)

    # Normalize features
    scaler      = StandardScaler()
    X_scaled    = scaler.fit_transform(X)
    scaler_mean  = scaler.mean_
    scaler_scale = scaler.scale_

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42
    )
    log.info(f"Train: {len(X_train)} samples, Test: {len(X_test)} samples")

    # Train ή load
    if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
        with open(SCALER_PATH) as f:
            saved = json.load(f)
        scaler_mean  = np.array(saved["mean"])
        scaler_scale = np.array(saved["scale"])
        model = load_model(scaler_mean, scaler_scale)
        log.info("Χρησιμοποιώ existing model")
    else:
        log.info("Training νέο model...")
        model = PriceDirectionNN()
        model = train(model, X_train, y_train, epochs=20)

        # Evaluate
        model.eval()
        with torch.no_grad():
            X_t   = torch.FloatTensor(X_test)
            preds = model(X_t).argmax(1).numpy()
        acc = (preds == y_test).mean() * 100
        log.info(f"Test Accuracy: {acc:.1f}%")

        save_model(model, scaler_mean, scaler_scale)

    # Predict
    predict_and_save(model, scaler_mean, scaler_scale)

    log.info("=" * 50)
    log.info("  Price Direction NN Complete")
    log.info("=" * 50)


if __name__ == "__main__":
    main()