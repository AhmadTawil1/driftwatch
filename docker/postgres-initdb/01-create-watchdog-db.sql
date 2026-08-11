-- Runs once, only on first container start (empty data volume).
-- The postgres image already creates the database named by POSTGRES_DB
-- (airflow's own metadata db). This adds the second database that holds
-- this project's own schema, kept separate so Airflow's internals never
-- mix with our results.
CREATE DATABASE watchdog;
