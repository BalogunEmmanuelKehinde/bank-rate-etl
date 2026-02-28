-- CBN Bank Rates Schema
-- Reporting date: weekly (every Wednesday)

CREATE TABLE IF NOT EXISTS deposit_rates (
    id                   SERIAL PRIMARY KEY,
    bank_name            VARCHAR(100) NOT NULL,
    demand_deposit_rate  NUMERIC(6,2),
    savings_deposit_rate NUMERIC(6,2),
    time_deposit_rate    NUMERIC(6,2),
    reporting_date       DATE NOT NULL,
    created_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (bank_name, reporting_date)
);

CREATE TABLE IF NOT EXISTS lending_rates (
    id             SERIAL PRIMARY KEY,
    bank_name      VARCHAR(100) NOT NULL,
    sector         VARCHAR(100) NOT NULL,
    prime_rate     NUMERIC(6,2),
    max_rate       NUMERIC(6,2),
    reporting_date DATE NOT NULL,
    created_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (bank_name, sector, reporting_date)
);