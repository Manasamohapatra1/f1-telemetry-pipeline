-- SQL script to initialize the PostgreSQL Star Schema for F1 Telemetry

-- Dimension Table: Drivers
CREATE TABLE IF NOT EXISTS dim_drivers (
    driver_id VARCHAR(10) PRIMARY KEY, -- e.g., 'VER', 'HAM'
    driver_number INT,
    full_name VARCHAR(100),
    team_name VARCHAR(100)
);

-- Dimension Table: Races
CREATE TABLE IF NOT EXISTS dim_races (
    race_id VARCHAR(50) PRIMARY KEY, -- e.g., '2023_Bahrain'
    year INT,
    round_number INT,
    race_name VARCHAR(100),
    circuit_name VARCHAR(100),
    session_date DATE
);

-- Fact Table: Laps
CREATE TABLE IF NOT EXISTS fct_laps (
    lap_id SERIAL PRIMARY KEY,
    race_id VARCHAR(50) REFERENCES dim_races(race_id),
    driver_id VARCHAR(10) REFERENCES dim_drivers(driver_id),
    lap_number INT,
    lap_time_ms INT,          -- lap time in milliseconds
    sector1_time_ms INT,
    sector2_time_ms INT,
    sector3_time_ms INT,
    is_pit_out_lap BOOLEAN,
    compound VARCHAR(20),     -- Tire compound (SOFT, MEDIUM, HARD)
    tyre_life INT,            -- Age of the tire
    UNIQUE (race_id, driver_id, lap_number)
);

-- Fact Table: Telemetry (High Frequency Data)
CREATE TABLE IF NOT EXISTS fct_telemetry (
    telemetry_id BIGSERIAL PRIMARY KEY,
    race_id VARCHAR(50) REFERENCES dim_races(race_id),
    driver_id VARCHAR(10) REFERENCES dim_drivers(driver_id),
    lap_number INT,
    session_time_ms BIGINT,   -- milliseconds from session start
    distance FLOAT,           -- cumulative distance on track
    speed INT,                -- km/h
    rpm INT,
    gear INT,
    throttle INT,             -- percentage 0-100
    brake INT,                -- percentage 0-100
    drs INT,                  -- DRS state
    x_position FLOAT,
    y_position FLOAT,
    z_position FLOAT
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_telemetry_race_driver ON fct_telemetry(race_id, driver_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_session_time ON fct_telemetry(session_time_ms);
