-------------------------------------------------------
-- DDL таблиц
DROP table if exists okx_prices_btc CASCADE;
CREATE TABLE okx_prices_btc (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи уровня
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка всего снимка стакана
    price NUMERIC(20, 10) NOT NULL, -- Цена на этом уровне
    size NUMERIC(20, 10) NOT NULL, -- Объем на этом уровне
    type VARCHAR(4) NOT NULL CHECK (type IN ('bid', 'ask')), -- Тип: 'bid' или 'ask'
    level INT NOT NULL, -- Уровень в стакане (например, 1 для лучшей цены, 2 для следующей и т.д.)
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка фактической записи в БД (опционально)
);

DROP table if exists okx_prices_eth CASCADE;
CREATE TABLE okx_prices_eth (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи уровня
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка всего снимка стакана
    price NUMERIC(20, 10) NOT NULL, -- Цена на этом уровне
    size NUMERIC(20, 10) NOT NULL, -- Объем на этом уровне
    type VARCHAR(4) NOT NULL CHECK (type IN ('bid', 'ask')), -- Тип: 'bid' или 'ask'
    level INT NOT NULL, -- Уровень в стакане (например, 1 для лучшей цены, 2 для следующей и т.д.)
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка фактической записи в БД (опционально)
);

DROP table if exists okx_prices_sol CASCADE;
CREATE TABLE okx_prices_sol (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи уровня
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка всего снимка стакана
    price NUMERIC(20, 10) NOT NULL, -- Цена на этом уровне
    size NUMERIC(20, 10) NOT NULL, -- Объем на этом уровне
    type VARCHAR(4) NOT NULL CHECK (type IN ('bid', 'ask')), -- Тип: 'bid' или 'ask'
    level INT NOT NULL, -- Уровень в стакане (например, 1 для лучшей цены, 2 для следующей и т.д.)
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка фактической записи в БД (опционально)
);

DROP table if exists okx_prices_ton CASCADE;
CREATE TABLE okx_prices_ton (
    id SERIAL PRIMARY KEY, -- Уникальный идентификатор записи уровня
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL, -- Временная метка всего снимка стакана
    price NUMERIC(20, 10) NOT NULL, -- Цена на этом уровне
    size NUMERIC(20, 10) NOT NULL, -- Объем на этом уровне
    type VARCHAR(4) NOT NULL CHECK (type IN ('bid', 'ask')), -- Тип: 'bid' или 'ask'
    level INT NOT NULL, -- Уровень в стакане (например, 1 для лучшей цены, 2 для следующей и т.д.)
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Временная метка фактической записи в БД (опционально)
);


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
-------------------------------------------------------
-- Чистки таблиц
delete from okx_prices_btc;
delete from okx_prices_eth;
delete from okx_prices_sol;
delete from okx_prices_ton;
-------------------------------------------------------
-- Проверяем заполняемость
drop view if exists data_load_stats;
create view data_load_stats as (
    select
        'BTC' as ticker,
        count(*) as cnt_rows
    from okx_prices_btc

    UNION

    select
        'ETH' as ticker,
        count(*) as cnt_rows
    from okx_prices_eth

    UNION

    select
        'SOL' as ticker,
        count(*) as cnt_rows
    from okx_prices_sol

    UNION

    select
        'TON' as ticker,
        count(*) as cnt_rows
    from okx_prices_ton
);

select * from data_load_stats;
-------------------------------------------------------
-- Проверяем таблицы

-- Анализ временных гэпов между снэпшотами
drop view if exists timegaps;
create view timegaps as
with timegaps_btc as (
    select
        *,
        lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_snapshot,
        TO_CHAR(
            snapshot_timestamp::timestamp - (lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp))::timestamp,
            'MI:SS'
        ) as timedelta
    from okx_prices_btc
    where 1 = 1
    order by type, level, snapshot_timestamp desc
),
timegaps_eth as (
    select
        *,
        lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_snapshot,
        TO_CHAR(
            snapshot_timestamp::timestamp - (lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp))::timestamp,
            'MI:SS'
        ) as timedelta
    from okx_prices_eth
    where 1 = 1
    order by type, level, snapshot_timestamp desc
),
timegaps_sol as (
    select
        *,
        lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_snapshot,
        TO_CHAR(
            snapshot_timestamp::timestamp - (lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp))::timestamp,
            'MI:SS'
        ) as timedelta
    from okx_prices_sol
    where 1 = 1
    order by type, level, snapshot_timestamp desc
),
timegaps_ton as (
    select
        *,
        lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_snapshot,
        TO_CHAR(
            snapshot_timestamp::timestamp - (lag(snapshot_timestamp, 1) OVER (partition by type, level order by snapshot_timestamp))::timestamp,
            'MI:SS'
        ) as timedelta
    from okx_prices_ton
    where 1 = 1
    order by type, level, snapshot_timestamp desc
)
select 'BTC' as ticker, max(timedelta) from timegaps_btc
union all
select 'ETH' as ticker, max(timedelta) from timegaps_eth
union all
select 'SOL' as ticker, max(timedelta) from timegaps_sol
union all
select 'TON' as ticker, max(timedelta) from timegaps_ton;

select * from timegaps;

-- Проверка спрэда
select * from okx_prices_sol;

with pricegaps_btc as (
    select
        *,
        lag(price, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_price,
        price - lag(price, 1) OVER (partition by type, level order by snapshot_timestamp) as pricedelta
    from okx_prices_btc
    where 1 = 1
    order by type, level, snapshot_timestamp desc
),
pricegaps_eth as (
    select
        *,
        lag(price, 1) OVER (partition by type, level order by snapshot_timestamp) as previous_price,
        price - lag(price, 1) OVER (partition by type, level order by snapshot_timestamp) as pricedelta
    from okx_prices_eth
    where 1 = 1
    order by type, level, snapshot_timestamp desc
)
select
    'ETH' as ticker,
    max(pricedelta) as max_pricedelta,
    min(pricedelta) as min_pricedelta
from pricegaps_eth

UNION

select
    'BTC' as ticker,
    max(pricedelta) as max_pricedelta,
    min(pricedelta) as min_pricedelta
from pricegaps_btc;


-- Проверка спрэда
select * from okx_prices_btc
where level = 1
order by snapshot_timestamp desc;

-- Цены
select
    'BTC' as ticker,
    max(price) as max_price,
    min(price) as min_price,
    max(price) - min(price) as range
from okx_prices_btc

UNION

select
    'ETH' as ticker,
    max(price) as max_price,
    min(price) as min_price,
    max(price) - min(price) as range
from okx_prices_eth

UNION

select
    'SOL' as ticker,
    max(price) as max_price,
    min(price) as min_price,
    max(price) - min(price) as range
from okx_prices_sol

UNION

select
    'TON' as ticker,
    max(price) as max_price,
    min(price) as min_price,
    max(price) - min(price) as range
from okx_prices_ton;

--------------------------------- Features marts ---------------------------------
-- BTC
drop view if exists features_mart_btc;
create view features_mart_btc as
select
    snapshot_timestamp,

    sum(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    sum(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    sum(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    sum(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    sum(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    sum(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    sum(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    sum(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    sum(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    sum(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    sum(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    sum(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    sum(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    sum(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    sum(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    sum(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    sum(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    sum(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    sum(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    sum(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_btc
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- ETH
drop view if exists features_mart_eth;
create view features_mart_eth as
select
    snapshot_timestamp,

    sum(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    sum(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    sum(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    sum(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    sum(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    sum(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    sum(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    sum(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    sum(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    sum(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    sum(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    sum(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    sum(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    sum(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    sum(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    sum(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    sum(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    sum(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    sum(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    sum(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_eth
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- SOL
drop view if exists features_mart_sol;
create view features_mart_sol as
select
    snapshot_timestamp,

    sum(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    sum(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    sum(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    sum(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    sum(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    sum(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    sum(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    sum(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    sum(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    sum(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    sum(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    sum(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    sum(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    sum(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    sum(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    sum(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    sum(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    sum(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    sum(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    sum(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_sol
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- TON
drop view if exists features_mart_ton;
create view features_mart_ton as
select
    snapshot_timestamp,

    sum(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    sum(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    sum(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    sum(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    sum(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    sum(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    sum(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    sum(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    sum(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    sum(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    sum(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    sum(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    sum(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    sum(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    sum(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    sum(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    sum(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    sum(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    sum(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    sum(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_ton
group by snapshot_timestamp
order by snapshot_timestamp desc;
--------------------------------------------------------------------------------
-- Смотрим вьюхи с фичами
select * from features_mart_btc;

select * from features_mart_sol;

select * from features_mart_ton;

select * from features_mart_eth;

-- Смотрим заполняемость
select * from data_load_stats;



