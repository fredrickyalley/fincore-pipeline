-- Local stand-in for Redshift (docker compose). Same tables and column order as
-- redshift_ddl.sql; DIST/SORT keys become plain indexes on the filtered date columns.

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
);
CREATE INDEX IF NOT EXISTS ix_fincore_trades_date ON raw.fincore_trades (trade_date, portfolio_id);

CREATE TABLE IF NOT EXISTS raw.fincore_market_data (
    symbol         VARCHAR(32)    NOT NULL,
    asset_class    VARCHAR(32),
    currency       CHAR(3)        NOT NULL,
    close_price    DECIMAL(18, 8) NOT NULL,
    price_date     DATE           NOT NULL,
    etl_loaded_at  TIMESTAMP      NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fincore_market_data_date ON raw.fincore_market_data (price_date, symbol);

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
);
CREATE INDEX IF NOT EXISTS ix_fincore_positions_date ON raw.fincore_portfolio_positions (as_of_date, portfolio_id);
