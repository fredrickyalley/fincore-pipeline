-- Raw landing tables for the Airflow load TaskGroup.
-- COPY ... FORMAT AS PARQUET maps columns BY POSITION: the column order here must match
-- fincore_etl.schemas.{TRADE,MARKET_DATA,POSITION}_OUTPUT_COLUMNS exactly.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.fincore_trades (
    order_id             VARCHAR(64)    NOT NULL,
    client_order_id      VARCHAR(64),
    portfolio_id         VARCHAR(64)    NOT NULL,
    symbol               VARCHAR(32)    NOT NULL,
    side                 VARCHAR(4)     NOT NULL,
    quantity             DECIMAL(18, 6) NOT NULL,
    price                DECIMAL(18, 8) NOT NULL,
    currency             CHAR(3)        NOT NULL,
    transact_time        TIMESTAMP      NOT NULL,
    trade_date           DATE           NOT NULL,
    close_price          DECIMAL(18, 8),
    close_currency       CHAR(3),
    realized_pnl_usd     DECIMAL(38, 6),
    pnl_unresolvable     BOOLEAN        NOT NULL,
    unresolvable_reason  VARCHAR(32),
    etl_loaded_at        TIMESTAMP      NOT NULL
)
DISTKEY (portfolio_id)
SORTKEY (trade_date, portfolio_id);

CREATE TABLE IF NOT EXISTS raw.fincore_market_data (
    symbol         VARCHAR(32)    NOT NULL,
    asset_class    VARCHAR(32),
    currency       CHAR(3)        NOT NULL,
    close_price    DECIMAL(18, 8) NOT NULL,
    price_date     DATE           NOT NULL,
    etl_loaded_at  TIMESTAMP      NOT NULL
)
DISTSTYLE ALL
SORTKEY (price_date, symbol);

CREATE TABLE IF NOT EXISTS raw.fincore_portfolio_positions (
    portfolio_id             VARCHAR(64)     NOT NULL,
    symbol                   VARCHAR(32)     NOT NULL,
    as_of_date               DATE            NOT NULL,
    position_quantity        DECIMAL(18, 6)  NOT NULL,
    average_cost             DECIMAL(18, 8),
    close_price              DECIMAL(18, 8)  NOT NULL,
    price_currency           CHAR(3)         NOT NULL,
    fx_rate_to_usd           DECIMAL(18, 12) NOT NULL,
    market_value_usd         DECIMAL(38, 6)  NOT NULL,
    unrealized_pnl_usd       DECIMAL(38, 6),
    realized_pnl_usd         DECIMAL(38, 6)  NOT NULL,
    portfolio_manager        VARCHAR(128),
    max_position_value_usd   DECIMAL(38, 6),
    max_var_usd              DECIMAL(38, 6),
    max_single_name_pct      DECIMAL(9, 4),
    position_limit_breached  BOOLEAN         NOT NULL,
    etl_loaded_at            TIMESTAMP       NOT NULL
)
DISTKEY (portfolio_id)
SORTKEY (as_of_date, portfolio_id);
