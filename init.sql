CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    address TEXT,
    phone_number VARCHAR(20),
    date_of_birth DATE,
    email VARCHAR(100),
    credit_card VARCHAR(100),
    key_access_frequency INTEGER DEFAULT 0
);