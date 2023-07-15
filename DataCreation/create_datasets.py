import csv
import json
import random
from datetime import datetime

# Function to generate a random date between a given range
def generate_random_date(start_date, end_date):
    start_timestamp = datetime.strptime(start_date, '%Y-%m-%d').timestamp()
    end_timestamp = datetime.strptime(end_date, '%Y-%m-%d').timestamp()
    random_timestamp = random.uniform(start_timestamp, end_timestamp)
    return datetime.fromtimestamp(random_timestamp).strftime('%Y-%m-%d')

# Read customer data from CSV file
def read_customer_data(filename):
    customer_data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            customer_data.append(row)
    return customer_data

# Read country names from CSV file
def read_country_names(filename):
    country_names = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            country_names.append(row['countries'])
    return country_names

# Generate random data
def generate_data(customer_data, country_names, start_date, end_date, order_id_range):
    data = []
    order_ids = list(range(order_id_range[0], order_id_range[1] + 1))  # List of possible orderIds
    random.shuffle(order_ids)  # Shuffle the orderIds

    num_customers = len(customer_data)

    for i, customer in enumerate(customer_data):
        if len(order_ids) < 1:
            break  # Exit loop if there are no more order IDs available

        num_orders = random.randint(1, min(6, len(order_ids)))  # Random number of orders per customer between 1 and 6

        orders = []
        for _ in range(num_orders):
            if order_ids:
                order_id = order_ids.pop(0)  # Take the next unique orderId from the list
                order = {
                    "orderId": str(order_id),
                    "orderDate": generate_random_date(start_date, end_date)
                }
                orders.append(order)

        account_created = generate_random_date(start_date, end_date)

        data.append({
            "customerId": customer['customerId'],
            "name": customer['name'],
            "contactName": customer['contactName'],
            "country": random.choice(country_names),
            "accountCreated": account_created,
            "orders": orders
        })

    return data

# CSV file paths
customer_data_file = 'customer_data.csv'
country_names_file = 'country_names.csv'

# Date range for accountCreated and orderDate
start_date = '2023-07-01'
end_date =   '2023-07-31'

# Order ID range [A, B]
order_id_range = [1, 110]  # Customize the range as needed

# Validate order ID range
if order_id_range[0] > order_id_range[1]:
    raise ValueError("Invalid order ID range. The lower limit should be less than or equal to the upper limit.")

# Read customer data and country names from CSV files
customer_data = read_customer_data(customer_data_file)
country_names = read_country_names(country_names_file)

# Generate data
data = generate_data(customer_data, country_names, start_date, end_date, order_id_range)

# Remove customers with no orders
data = [customer for customer in data if customer["orders"]]

# Generate filename based on the date range
filename = f'Orders_{start_date}_to_{end_date}.json'

# Save data to a JSON file
with open(filename, 'w') as file:
    json.dump(data, file, indent=2)

print(f"Data saved to {filename}.")