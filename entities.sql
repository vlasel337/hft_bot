DROP table if exists crypto_prices;
CREATE TABLE crypto_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставка 10 тестовых тикеров и их случайных цен
INSERT INTO crypto_prices (ticker, price) VALUES
('BTCUSDT', random() * 70000 + 60000),
('ETHUSDT', random() * 4000 + 3000),
('BNBUSDT', random() * 600 + 500),
('XRPUSDT', random() * 1.5 + 0.5),
('ADAUSDT', random() * 0.8 + 0.3),
('DOGEUSDT', random() * 0.2 + 0.05),
('SOLUSDT', random() * 200 + 100),
('DOTUSDT', random() * 30 + 15),
('MATICUSDT', random() * 1.2 + 0.7),
('LTCUSDT', random() * 100 + 70);

select * from crypto_prices;

DROP table if exists okx_price_snapshots;
CREATE TABLE okx_price_snapshots (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка снимка стакана
    best_bid NUMERIC(20, 10) NOT NULL, -- Лучшая цена покупки (bid)
    best_ask NUMERIC(20, 10) NOT NULL, -- Лучшая цена продажи (ask)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка создания записи (опционально)
);

select * from okx_price_snapshots;


