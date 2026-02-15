DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (name, email)
SELECT
    'user_' || i,
    'user_' || i || '@example.com'
FROM generate_series(1, 1000) AS i;
