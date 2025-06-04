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

    max(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    max(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    max(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    max(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    max(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    max(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    max(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    max(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    max(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    max(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    max(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    max(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    max(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    max(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    max(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    max(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    max(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    max(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    max(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    max(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_btc
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- ETH
drop view if exists features_mart_eth;
create view features_mart_eth as
select
    snapshot_timestamp,

    max(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    max(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    max(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    max(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    max(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    max(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    max(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    max(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    max(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    max(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    max(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    max(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    max(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    max(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    max(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    max(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    max(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    max(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    max(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    max(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_eth
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- SOL
drop view if exists features_mart_sol;
create view features_mart_sol as
select
    snapshot_timestamp,

    max(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    max(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    max(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    max(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    max(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    max(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    max(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    max(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    max(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    max(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    max(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    max(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    max(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    max(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    max(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    max(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    max(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    max(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    max(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    max(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from okx_prices_sol
group by snapshot_timestamp
order by snapshot_timestamp desc;

-- TON
drop view if exists features_mart_ton;
create view features_mart_ton as
with
select
    snapshot_timestamp,

    max(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    max(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    max(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    max(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    max(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    max(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    max(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    max(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    max(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    max(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    max(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    max(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    max(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    max(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    max(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    max(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    max(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    max(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    max(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    max(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
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

-- Ищем разрыв во времени, связанный с переносом БД на новый хост ()
-- 2025-05-23 09:27:59.954000 +00:00
-- 2025-05-23 09:48:29.804000 +00:00
select distinct snapshot_timestamp from okx_prices_btc
order by snapshot_timestamp desc;


-- Поиск дублей внутри таймстемпа
with inner_query as (
select
    TO_CHAR(snapshot_timestamp, 'YYYY-MM-DD HH24:MI') as dttm,
    row_number()
    over (partition by TO_CHAR(snapshot_timestamp, 'YYYY-MM-DD HH24:MI') order by snapshot_timestamp) as rn,
    *
from features_mart_btc
order by snapshot_timestamp desc
)
select *
from inner_query
where 1=1
    and dttm = '2025-05-31 16:51'
order by dttm desc;


-- Логика дедубликации
'2025-06-01 13:14:57.604000 +00:00' -- Оставляем + округляем до '2025-06-01 13:15:00'
'2025-06-01 13:14:02.604000 +00:00' -- Дедублицируем

-- Дедубликация + округление секунд в snapshot_timestamp до следующей минуты
drop view if exists features_mart_btc;
create view features_mart_btc as
with inner_tab as (
    select
        snapshot_timestamp,
        TO_CHAR(snapshot_timestamp, 'YYYY-MM-DD HH24:MI') as dttm,
        row_number() over (partition by level, type, TO_CHAR(snapshot_timestamp, 'YYYY-MM-DD HH24:MI') order by snapshot_timestamp desc) as rn,
        type,
        level,
        price,
        size
    from okx_prices_btc
)
select
    CASE
        WHEN EXTRACT(SECOND FROM snapshot_timestamp::timestamp) > 0 THEN DATE_TRUNC('minute', snapshot_timestamp::timestamp) + INTERVAL '1 minute'
        ELSE snapshot_timestamp::timestamp
    END AS rounded_up_st,
    snapshot_timestamp,

    max(case when type = 'bid' and level = 1 then price else 0 end) as bid_1,
    max(case when type = 'bid' and level = 1 then size else 0 end) as size_bid_1,

    max(case when type = 'bid' and level = 2 then price else 0 end) as bid_2,
    max(case when type = 'bid' and level = 2 then size else 0 end) as size_bid_2,

    max(case when type = 'bid' and level = 3 then price else 0 end) as bid_3,
    max(case when type = 'bid' and level = 3 then size else 0 end) as size_bid_3,

    max(case when type = 'bid' and level = 4 then price else 0 end) as bid_4,
    max(case when type = 'bid' and level = 4 then size else 0 end) as size_bid_4,

    max(case when type = 'bid' and level = 5 then price else 0 end) as bid_5,
    max(case when type = 'bid' and level = 5 then size else 0 end) as size_bid_5,

    max(case when type = 'ask' and level = 1 then price else 0 end) as ask_1,
    max(case when type = 'ask' and level = 1 then size else 0 end) as size_ask_1,

    max(case when type = 'ask' and level = 2 then price else 0 end) as ask_2,
    max(case when type = 'ask' and level = 2 then size else 0 end) as size_ask_2,

    max(case when type = 'ask' and level = 3 then price else 0 end) as ask_3,
    max(case when type = 'ask' and level = 3 then size else 0 end) as size_ask_3,

    max(case when type = 'ask' and level = 4 then price else 0 end) as ask_4,
    max(case when type = 'ask' and level = 4 then size else 0 end) as size_ask_4,

    max(case when type = 'ask' and level = 5 then price else 0 end) as ask_5,
    max(case when type = 'ask' and level = 5 then size else 0 end) as size_ask_5
from inner_tab
where 1=1 -- and dttm in ('2025-05-20 05:31') -- 2025-05-23 09:08;
    and rn = 1
group by rounded_up_st, snapshot_timestamp
order by snapshot_timestamp desc;