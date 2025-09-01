import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "sqlite:///sample.db")
engine = create_engine(DB_URL)

ddl = """
CREATE TABLE IF NOT EXISTS customers (
  customer_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT,
  signup_date TEXT
);
CREATE TABLE IF NOT EXISTS orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  order_date TEXT NOT NULL,
  amount REAL NOT NULL,
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);
"""
seed = """
INSERT INTO customers (customer_id, name, city, signup_date) VALUES
  (1,'Alice','Hyderabad','2024-01-05'),
  (2,'Bob','Bengaluru','2024-03-11'),
  (3,'Chitra','Chennai','2024-04-20')
ON CONFLICT DO NOTHING;

INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES
  (101,1,'2024-05-01',199.99),
  (102,2,'2024-05-03',79.49),
  (103,1,'2024-05-09',25.00),
  (104,3,'2024-06-15',300.00)
ON CONFLICT DO NOTHING;
"""

with engine.begin() as conn:
    # SQLite doesn't understand ON CONFLICT DO NOTHING in all variants; ignore failures
    for stmt in ddl.strip().split(";\n"):
        if stmt.strip():
            conn.execute(text(stmt))
    try:
        for stmt in seed.strip().split(";\n\n"):
            if stmt.strip():
                conn.execute(text(stmt))
    except Exception:
        pass

print("Initialized sample DB at", DB_URL)
