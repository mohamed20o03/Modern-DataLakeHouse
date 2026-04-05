-- ==========================================================================
-- Source Database Initialization Script
-- Creates the 'customers' sample table and seeds it with 10 rows.
-- Also configures logical replication for Debezium CDC.
-- ==========================================================================

-- Grant REPLICATION privilege to the debezium user
-- (The user is created by POSTGRES_USER env var in docker-compose)
ALTER USER debezium WITH REPLICATION;

-- Create the customers table
CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL       PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) UNIQUE,
    status     VARCHAR(50)  NOT NULL DEFAULT 'active',
    created_at TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed 10 sample rows
INSERT INTO customers (name, email, status) VALUES
    ('Ahmed Hassan',      'ahmed.hassan@example.com',      'active'),
    ('Fatma Ali',         'fatma.ali@example.com',         'active'),
    ('Mohamed Ibrahim',   'mohamed.ibrahim@example.com',   'active'),
    ('Sara Mahmoud',      'sara.mahmoud@example.com',      'inactive'),
    ('Omar Khaled',       'omar.khaled@example.com',       'active'),
    ('Nour El-Din',       'nour.eldin@example.com',        'active'),
    ('Yasmin Tarek',      'yasmin.tarek@example.com',      'suspended'),
    ('Karim Mostafa',     'karim.mostafa@example.com',     'active'),
    ('Hana Saeed',        'hana.saeed@example.com',        'active'),
    ('Amr Fathy',         'amr.fathy@example.com',         'inactive');

-- Create publication for Debezium (pgoutput plugin)
-- This tells PostgreSQL which tables to include in logical replication
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
