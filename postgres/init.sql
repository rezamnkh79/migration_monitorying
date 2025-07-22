-- Create database if not exists
-- Note: In Docker, the database is already created via environment variable

-- Create trigger function for updated_at columns
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ADTRACE TABLES START HERE
-- PostgreSQL schema for AdTrace database tables

-- Table: adtrace_tracker
CREATE TABLE IF NOT EXISTS adtrace_tracker (
    id SERIAL PRIMARY KEY,
    device_matching_for_click_window_days INTEGER NOT NULL,
    device_fingerprinting_for_click_enabled BOOLEAN NOT NULL,
    device_fingerprinting_for_click_window_hours INTEGER NOT NULL,
    reattribution_enabled BOOLEAN NOT NULL,
    reattribution_inactivity_window_days INTEGER NOT NULL,
    reattribution_for_click_window_days INTEGER NOT NULL,
    device_matching_for_impression_enabled BOOLEAN NOT NULL,
    device_matching_for_impression_window_hours INTEGER NOT NULL,
    device_fingerprinting_for_impression_enabled BOOLEAN NOT NULL,
    device_fingerprinting_for_impression_window_hours INTEGER NOT NULL,
    name VARCHAR(128) NOT NULL,
    tracker_token VARCHAR(16) NOT NULL UNIQUE,
    description VARCHAR(2048),
    level INTEGER NOT NULL,
    status INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    campaign_name VARCHAR(50),
    adgroup_name VARCHAR(50),
    creative_name VARCHAR(50),
    account_id INTEGER NOT NULL,
    mobile_app_id INTEGER NOT NULL,
    parent_id INTEGER,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP,
    ancestor_ids_regex VARCHAR(128),
    tracker_type INTEGER NOT NULL DEFAULT 1,
    mobile_app_token VARCHAR(128) NOT NULL,
    deep_link_url VARCHAR(1024),
    default_redirect_url VARCHAR(1024),
    default_redirect_url_android VARCHAR(1024),
    default_redirect_url_ios VARCHAR(1024),
    default_redirect_url_windows VARCHAR(1024),
    default_redirect_url_windows_phone VARCHAR(1024),
    impression_callback_url VARCHAR(2048),
    install_callback_url VARCHAR(2048),
    session_callback_url VARCHAR(2048),
    is_hide BOOLEAN NOT NULL,
    network_id INTEGER,
    network_token VARCHAR(16),
    default_redirect_url_android_type INTEGER NOT NULL DEFAULT 4,
    ad_network_enum INTEGER NOT NULL DEFAULT 0,
    is_deferred_deep_link BOOLEAN NOT NULL DEFAULT FALSE,
    enable_redirect_url_change BOOLEAN NOT NULL DEFAULT TRUE,
    track_dependency_id INTEGER
);

-- Table: buy_transaction
CREATE TABLE IF NOT EXISTS buy_transaction (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    user_id INTEGER,
    wallet_id INTEGER NOT NULL,
    amount VARCHAR(20) NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    deletion_time TIMESTAMP,
    mobile_app_id INTEGER,
    description JSONB
);

-- Table: mobile_app_detail
CREATE TABLE IF NOT EXISTS mobile_app_detail (
    id SERIAL PRIMARY KEY,
    firebase_certificate JSONB,
    mobile_app_id INTEGER NOT NULL UNIQUE,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    deletion_time TIMESTAMP,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    event_value_keys VARCHAR(255),
    default_currency VARCHAR(20),
    all_mobile_app_currency VARCHAR(64),
    is_ban_tracker_limit BOOLEAN DEFAULT TRUE
);

-- Table: adtrace_transaction
CREATE TABLE IF NOT EXISTS adtrace_transaction (
    id SERIAL PRIMARY KEY,
    amount DOUBLE PRECISION NOT NULL,
    reference_id VARCHAR(64),
    account_id INTEGER NOT NULL,
    bank_payment_attempt_id INTEGER,
    creation_time TIMESTAMP NOT NULL,
    deletion_time TIMESTAMP,
    is_deleted BOOLEAN NOT NULL,
    last_update_time TIMESTAMP NOT NULL
);

-- Table: adtrace_mobile_app
CREATE TABLE IF NOT EXISTS adtrace_mobile_app (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    bundle_id VARCHAR(255),
    app_token VARCHAR(128) NOT NULL UNIQUE,
    account_id INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP,
    status INTEGER NOT NULL DEFAULT 1
);

-- Table: adtrace_event_type
CREATE TABLE IF NOT EXISTS adtrace_event_type (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    event_token VARCHAR(16) NOT NULL,
    mobile_app_id INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP,
    default_value DECIMAL(10,2),
    description TEXT
);

-- Additional important AdTrace tables
CREATE TABLE IF NOT EXISTS wallet (
    id SERIAL PRIMARY KEY,
    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    account_id INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS business_profile (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) NOT NULL,
    account_id INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    phone_number VARCHAR(50),
    address TEXT,
    tax_id VARCHAR(100),
    business_type VARCHAR(100),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invoice (
    id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(100) NOT NULL UNIQUE,
    account_id INTEGER NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    due_date TIMESTAMP,
    paid_date TIMESTAMP,
    payment_method VARCHAR(50),
    description TEXT,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS partner (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    partner_token VARCHAR(128) NOT NULL UNIQUE,
    api_key VARCHAR(255),
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    status INTEGER NOT NULL DEFAULT 1,
    contact_email VARCHAR(255),
    webhook_url VARCHAR(1024),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deletion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plan (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    billing_period VARCHAR(50) NOT NULL,
    features JSONB,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    max_monthly_tracked_users INTEGER,
    max_apps INTEGER
);

CREATE TABLE IF NOT EXISTS summary_event_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    mobile_app_id INTEGER NOT NULL,
    event_type_id INTEGER NOT NULL,
    total_events INTEGER NOT NULL DEFAULT 0,
    unique_users INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.00,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS summary_tracker_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    tracker_id INTEGER NOT NULL,
    total_clicks INTEGER NOT NULL DEFAULT 0,
    total_installs INTEGER NOT NULL DEFAULT 0,
    total_sessions INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.00,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS user_subscription_info (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    plan_id INTEGER NOT NULL,
    subscription_start TIMESTAMP NOT NULL,
    subscription_end TIMESTAMP,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    payment_method VARCHAR(100),
    auto_renew BOOLEAN NOT NULL DEFAULT TRUE,
    creation_time TIMESTAMP NOT NULL,
    last_update_time TIMESTAMP NOT NULL
);

-- Create indexes for AdTrace tables
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_account_id ON adtrace_tracker(account_id);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_mobile_app_id ON adtrace_tracker(mobile_app_id);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_status ON adtrace_tracker(status);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_name ON adtrace_tracker(name);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_campaign_name ON adtrace_tracker(campaign_name);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_adgroup_name ON adtrace_tracker(adgroup_name);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_creative_name ON adtrace_tracker(creative_name);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_parent_id ON adtrace_tracker(parent_id);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_is_deleted ON adtrace_tracker(is_deleted);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_network_id ON adtrace_tracker(network_id);
CREATE INDEX IF NOT EXISTS idx_adtrace_tracker_network_token ON adtrace_tracker(network_token);

CREATE INDEX IF NOT EXISTS idx_buy_transaction_account_id ON buy_transaction(account_id);
CREATE INDEX IF NOT EXISTS idx_buy_transaction_wallet_id ON buy_transaction(wallet_id);

CREATE INDEX IF NOT EXISTS idx_adtrace_transaction_account_id ON adtrace_transaction(account_id);
CREATE INDEX IF NOT EXISTS idx_adtrace_transaction_bank_payment_attempt_id ON adtrace_transaction(bank_payment_attempt_id);

CREATE INDEX IF NOT EXISTS idx_summary_event_daily_date ON summary_event_daily(date);
CREATE INDEX IF NOT EXISTS idx_summary_event_daily_mobile_app_id ON summary_event_daily(mobile_app_id);
CREATE INDEX IF NOT EXISTS idx_summary_event_daily_event_type_id ON summary_event_daily(event_type_id);

CREATE INDEX IF NOT EXISTS idx_summary_tracker_daily_date ON summary_tracker_daily(date);
CREATE INDEX IF NOT EXISTS idx_summary_tracker_daily_tracker_id ON summary_tracker_daily(tracker_id);

-- Create migration tracking table
CREATE TABLE IF NOT EXISTS migration_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    mysql_id INTEGER,
    postgres_id INTEGER,
    data_hash VARCHAR(64),
    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,
    verification_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_migration_log_table ON migration_log(table_name);
CREATE INDEX IF NOT EXISTS idx_migration_log_mysql_id ON migration_log(mysql_id);
CREATE INDEX IF NOT EXISTS idx_migration_log_verified ON migration_log(verified);

-- Create updated_at triggers for AdTrace tables that need them
CREATE OR REPLACE FUNCTION update_last_update_time_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_adtrace_tracker_updated_at 
    BEFORE UPDATE ON adtrace_tracker
    FOR EACH ROW EXECUTE FUNCTION update_last_update_time_column();

CREATE TRIGGER update_buy_transaction_updated_at 
    BEFORE UPDATE ON buy_transaction
    FOR EACH ROW EXECUTE FUNCTION update_last_update_time_column();

CREATE TRIGGER update_mobile_app_detail_updated_at 
    BEFORE UPDATE ON mobile_app_detail
    FOR EACH ROW EXECUTE FUNCTION update_last_update_time_column();

CREATE TRIGGER update_adtrace_transaction_updated_at 
    BEFORE UPDATE ON adtrace_transaction
    FOR EACH ROW EXECUTE FUNCTION update_last_update_time_column();

-- Create a view for active trackers
CREATE OR REPLACE VIEW active_trackers AS
SELECT 
    id,
    name,
    tracker_token,
    account_id,
    mobile_app_id,
    status,
    creation_time,
    last_update_time
FROM adtrace_tracker 
WHERE is_deleted = FALSE AND status = 1;

-- Create a view for daily statistics
CREATE OR REPLACE VIEW daily_stats AS
SELECT 
    date,
    COUNT(DISTINCT mobile_app_id) as active_apps,
    SUM(total_events) as total_events,
    SUM(unique_users) as total_unique_users,
    SUM(total_revenue) as total_revenue
FROM summary_event_daily
GROUP BY date
ORDER BY date DESC; 


