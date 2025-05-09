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

-- Таблица со всеми ценами (5 bid и 5 ask за 1 снэпшот)
DROP table if exists okx_price_levels;
CREATE TABLE okx_price_levels (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи уровня
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка всего снимка стакана
    price NUMERIC(20, 10) NOT NULL, -- Цена на этом уровне
    size NUMERIC(20, 10) NOT NULL, -- Объем на этом уровне
    type VARCHAR(4) NOT NULL CHECK (type IN ('bid', 'ask')), -- Тип: 'bid' или 'ask'
    level INT NOT NULL, -- Уровень в стакане (например, 1 для лучшей цены, 2 для следующей и т.д.)
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка фактической записи в БД (опционально)
);


-- Зачищаем таблицу
delete from okx_price_levels;

-- ETH-USDT
DROP table if exists okx_prices_BTC;
CREATE TABLE okx_prices_BTC as
select * from okx_price_levels where 1=0;

-- ETH-USDT
DROP table if exists okx_prices_ETH;
CREATE TABLE okx_prices_ETH as
select * from okx_price_levels where 1=0;

-- SOL-USDT
DROP table if exists okx_prices_SOL;
CREATE TABLE okx_prices_SOL as
select * from okx_price_levels where 1=0;

-- TON-USDT
DROP table if exists okx_prices_TON;
CREATE TABLE okx_prices_TON as
select * from okx_price_levels where 1=0;

-- Смотрим результаты
select count(*)/10 as cnt_snapshots
from okx_price_levels;

select * from okx_price_levels
where 1=1
    and type = 'ask'
order by snapshot_timestamp desc, id desc
limit 1000;

-- Чистки таблиц
delete from okx_prices_btc;
delete from okx_prices_eth;
delete from okx_prices_sol;
delete from okx_prices_ton;

drop table "okx_prices_TON";
drop table "okx_prices_BTC";
drop table "okx_prices_ETH";
drop table "okx_prices_SOL";

------ Проверяем заполняемость
select 'BTC' as ticker, count(*) as cnt_rows
from okx_prices_btc

UNION

select
    'ETH' as ticker,
    count(*) as cnt_rows
--     max(case when type = 'bid' then price else 0 end) as max_bid_price,
--     max(case when type = 'ask' then price else 0 end) as min_ask_price
from okx_prices_eth

UNION

select 'SOL' as ticker, count(*) as cnt_rows
from okx_prices_sol

UNION

select 'TON' as ticker, count(*) as cnt_rows
from okx_prices_ton;


