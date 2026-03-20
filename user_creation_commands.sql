-- CREATE USER user_ps WITH PASSWORD 'ps123';

-- ALTER USER user_ps WITH SUPERUSER;

--GRANT ALL PRIVILEGES ON DATABASE prd_policyscanner TO user_ps;
-- If table creation fails with "permission denied for schema public", run:
-- GRANT USAGE, CREATE ON SCHEMA public TO user_ps;

-- Tables used by the Streamlit app (policy_scanner_ui.py)
CREATE TABLE IF NOT EXISTS user_info (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    contact TEXT NOT NULL,
    email TEXT NOT NULL,
    dob DATE,
    gender TEXT,
    nicotine_status TEXT,
    status TEXT NOT NULL DEFAULT 'Active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_info_email_contact ON user_info (email, contact);

CREATE TABLE IF NOT EXISTS user_coverage_selected (
    id SERIAL PRIMARY KEY,
    user_info_id INTEGER NOT NULL REFERENCES user_info(id) ON DELETE CASCADE,
    coverage_selected INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- If you created these tables as an admin user, grant the app role permissions:
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE user_info, user_coverage_selected TO user_ps;
GRANT USAGE, SELECT ON SEQUENCE user_info_id_seq, user_coverage_selected_id_seq TO user_ps;
