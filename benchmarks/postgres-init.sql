-- Initialize PostgreSQL for benchmarking
-- Enable required extensions

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- pg_stat_statements configuration is now in postgresql.conf
-- This ensures the extension is preloaded before these settings are applied

-- Create benchmark schema
CREATE SCHEMA IF NOT EXISTS benchmark;

-- Grant permissions
GRANT ALL ON SCHEMA benchmark TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA benchmark TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA benchmark TO postgres;

-- Create initial tables for stress testing
-- These will be populated by the benchmark CLI

-- Users table (will be heavily queried)
CREATE TABLE IF NOT EXISTS benchmark.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    profile_data JSONB
);

-- Posts table (large table for sequential scan testing)
CREATE TABLE IF NOT EXISTS benchmark.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES benchmark.users(id),
    title VARCHAR(500),
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    view_count INTEGER DEFAULT 0,
    tags TEXT[],
    metadata JSONB
);

-- Comments table (for join operations)
CREATE TABLE IF NOT EXISTS benchmark.comments (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES benchmark.posts(id),
    user_id INTEGER REFERENCES benchmark.users(id),
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parent_id INTEGER REFERENCES benchmark.comments(id),
    likes INTEGER DEFAULT 0
);

-- Analytics events table (for aggregation queries)
CREATE TABLE IF NOT EXISTS benchmark.events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    event_type VARCHAR(50),
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id UUID,
    ip_address INET
);

-- Intentionally create tables without indexes to test diagnostics
-- The benchmark will demonstrate finding missing indexes

-- Add one partial index as an example
CREATE INDEX idx_users_status_active ON benchmark.users(username) WHERE status = 'active';

-- Reset statistics
SELECT pg_stat_reset();
SELECT pg_stat_statements_reset();