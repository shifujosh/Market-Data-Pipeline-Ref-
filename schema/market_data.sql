-- Reference Schema for High-Frequency Tick Data
-- Optimization: Partitioned by TradeDate and Ticker for rapid retrieval

CREATE TABLE market_data.tick_feed (
    event_id UUID PRIMARY KEY,
    ticker_symbol VARCHAR(10) NOT NULL,
    trade_timestamp TIMESTAMP_NTZ NOT NULL, -- No Time Zone for absolute UTC
    price DECIMAL(18, 4) NOT NULL,          -- 4 decimal precision for financial accuracy
    volume INT NOT NULL,
    exchange_code VARCHAR(4),
    ingestion_latency_ms INT,               -- Tracking infrastructure lag
    
    CONSTRAINT check_price_positive CHECK (price > 0),
    CONSTRAINT check_volume_positive CHECK (volume >= 0)
);

-- Index for time-series analysis
CREATE INDEX idx_ticker_time ON market_data.tick_feed (ticker_symbol, trade_timestamp DESC);
