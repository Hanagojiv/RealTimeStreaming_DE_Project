DROP TABLE IF EXISTS created_users;
CREATE TABLE created_users (
    id BIGINT IDENTITY(1, 1) PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
