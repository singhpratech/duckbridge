-- Sample DuckDB database exercising every type the converter handles.
CREATE SCHEMA sales;
CREATE SEQUENCE seq_orders_id START 1;
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR UNIQUE,
    born DATE,
    vip BOOLEAN DEFAULT false,
    score DOUBLE,
    balance DECIMAL(18,4),
    uid UUID DEFAULT uuid(),
    created TIMESTAMP DEFAULT now(),
    notes VARCHAR
);
COMMENT ON TABLE customers IS 'Customer master';
COMMENT ON COLUMN customers.name IS 'Display name';

CREATE TABLE orders (
    id INTEGER DEFAULT nextval('seq_orders_id') PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    ordered_at TIMESTAMP WITH TIME ZONE,
    ship_date DATE,
    qty SMALLINT,
    tiny TINYINT,
    utiny UTINYINT,
    big BIGINT,
    ubig UBIGINT,
    huge HUGEINT,
    price FLOAT,
    amount DECIMAL(12,2),
    paid BOOLEAN,
    tags VARCHAR[],
    attrs STRUCT(color VARCHAR, size INTEGER),
    payload JSON,
    blob BLOB,
    t TIME,
    iv INTERVAL,
    m mood,
    ts_ms TIMESTAMP_MS,
    ts_ns TIMESTAMP_NS,
    CHECK (qty >= 0)
);
CREATE INDEX ix_orders_customer ON orders(customer_id);
CREATE UNIQUE INDEX ux_orders_cust_date ON orders(customer_id, ship_date);

INSERT INTO customers (id, name, email, born, vip, score, balance, notes) VALUES
 (1, 'Alice O''Neil', 'alice@example.com', DATE '1980-02-29', true, 1.5, 1234.5678, 'first'),
 (2, 'Bob 𝔘nicode', 'bob@example.com', DATE '1999-12-31', false, NULL, -0.0001, NULL),
 (3, 'Carol
Multiline', NULL, NULL, true, 'inf'::DOUBLE, 0, 'tab	here');

INSERT INTO orders (customer_id, ordered_at, ship_date, qty, tiny, utiny, big, ubig, huge, price, amount, paid, tags, attrs, payload, blob, t, iv, m, ts_ms, ts_ns)
SELECT
    1 + (i % 3),
    TIMESTAMPTZ '2024-01-01 10:00:00+00' + to_minutes(i),
    DATE '2024-01-01' + to_days(i),
    i % 100,
    -100 + (i % 200),
    i % 250,
    9000000000 + i,
    18000000000000000000 + i,
    170141183460469231731687303715884105727 - i,
    i * 0.25,
    i * 1.01,
    i % 2 = 0,
    ['a', 'b', CAST(i AS VARCHAR)],
    {'color': 'red', 'size': i},
    to_json({'k': i, 'arr': [1,2,3]}),
    encode(CAST(i AS VARCHAR)),
    TIME '12:34:56.123456',
    INTERVAL (i) DAY,
    CASE i % 3 WHEN 0 THEN 'sad' WHEN 1 THEN 'ok' ELSE 'happy' END::mood,
    TIMESTAMP_MS '2024-06-01 01:02:03.456',
    TIMESTAMP_NS '2024-06-01 01:02:03.123456789'
FROM range(1, 1001) r(i);

-- big-ish table for throughput
CREATE TABLE events (
    event_id BIGINT PRIMARY KEY,
    customer_id INTEGER,
    kind VARCHAR,
    at_ts TIMESTAMP,
    value DOUBLE
);
INSERT INTO events SELECT i, 1 + (i % 3), 'kind_' || (i % 10), TIMESTAMP '2024-01-01' + to_seconds(i), random() FROM range(1, 200001) r(i);

CREATE VIEW v_customer_totals AS
SELECT c.id, c.name, count(o.id) AS n_orders, sum(o.amount) AS total
FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.name;

CREATE VIEW sales.v_recent AS SELECT * FROM orders WHERE ship_date >= DATE '2024-06-01' ORDER BY ship_date DESC LIMIT 100;

CREATE TABLE sales.regions (code VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, manager_id INTEGER);
INSERT INTO sales.regions VALUES ('EU', 'Europe', 1), ('US', 'United States', 2), ('APAC', 'Asia Pacific', NULL);
