import os
import pandas as pd

# Directory to store CSV files
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

# Define file paths
customers_file = os.path.join(data_dir, "customers.csv")
orders_file = os.path.join(data_dir, "orders.csv")

# Sample data for customers and orders
customers_data = [
    {"customer_id": 1, "name": "Alice", "city": "Hyderabad", "signup_date": "2024-01-05"},
    {"customer_id": 2, "name": "Bob", "city": "Bengaluru", "signup_date": "2024-03-11"},
    {"customer_id": 3, "name": "Chitra", "city": "Chennai", "signup_date": "2024-04-20"}
]

orders_data = [
    {"order_id": 101, "customer_id": 1, "order_date": "2024-05-01", "amount": 199.99},
    {"order_id": 102, "customer_id": 2, "order_date": "2024-05-03", "amount": 79.49},
    {"order_id": 103, "customer_id": 1, "order_date": "2024-05-09", "amount": 25.00},
    {"order_id": 104, "customer_id": 3, "order_date": "2024-06-15", "amount": 300.00}
]

# Create CSV files if they don't exist
if not os.path.exists(customers_file):
    pd.DataFrame(customers_data).to_csv(customers_file, index=False)

if not os.path.exists(orders_file):
    pd.DataFrame(orders_data).to_csv(orders_file, index=False)

print(f"Initialized sample CSV data in '{data_dir}' directory:")
print(f"- {customers_file}")
print(f"- {orders_file}")
