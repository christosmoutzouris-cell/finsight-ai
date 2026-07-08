-- Ενεργοποιεί αυτόματη χρονοσήμανση με timezone
CREATE TABLE IF NOT EXISTS stock_prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(10)    NOT NULL,
    price           NUMERIC(12, 4) NOT NULL,
    open            NUMERIC(12, 4),
    high            NUMERIC(12, 4),
    low             NUMERIC(12, 4),
    volume          BIGINT,
    market_cap      BIGINT,
    -- event_time: η ώρα που τραβήχτηκε η τιμή (από τον producer)
    event_time      TIMESTAMPTZ    NOT NULL,
    -- ingested_at: η ώρα που γράφτηκε στη βάση (αυτόματα)
    ingested_at     TIMESTAMPTZ    DEFAULT NOW()
);

-- Index για γρήγορες queries ανά symbol και χρόνο
CREATE INDEX idx_stock_symbol      ON stock_prices (symbol);
CREATE INDEX idx_stock_event_time  ON stock_prices (event_time DESC);
CREATE INDEX idx_stock_symbol_time ON stock_prices (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS watched_symbols (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10)  NOT NULL UNIQUE,
    company     VARCHAR(100),
    sector      VARCHAR(50),
    is_active   BOOLEAN      DEFAULT TRUE,
    added_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- Insert αρχικά symbols
INSERT INTO watched_symbols (symbol, company, sector) VALUES
    ('AAPL',  'Apple Inc.',            'Technology'),
    ('MSFT',  'Microsoft Corporation', 'Technology'),
    ('GOOGL', 'Alphabet Inc.',         'Technology'),
    ('AMZN',  'Amazon.com Inc.',       'Consumer'),
    ('NVDA',  'NVIDIA Corporation',    'Technology')
ON CONFLICT (symbol) DO NOTHING;