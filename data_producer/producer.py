# data_producer/producer.py
import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sales_transactions'

# Expanded and localized lists
STORES = {
    1: "Cairo Festival City",
    2: "Mall of Arabia",
    3: "City Stars",
    4: "San Stefano Grand Plaza",
    5: "Mall of Egypt",
    6: "Point 90",
    7: "Downtown Katameya",
    8: "City Centre Almaza"
}

PRODUCTS = {
    101: ("Juhayna Full Cream Milk 1L", "Dairy"),
    102: ("Domty Feta Cheese 500g", "Dairy"),
    103: ("Fine Healthy Tissues", "Household"),
    104: ("Edita Molto Croissant", "Bakery"),
    105: ("Chipsy Salt & Vinegar", "Snacks"),
    106: ("Coca-Cola 1L", "Beverages"),
    107: ("Abu Auf Turkish Coffee", "Pantry"),
    108: ("Lipton Yellow Label Tea", "Pantry"),
    109: ("Indomie Chicken Noodles", "Pantry"),
    110: ("El-Shams Rice 1kg", "Pantry"),
    111: ("Persil Gel Detergent 2.5L", "Household"),
    112: ("Eva Honey Body Lotion", "Personal Care"),
    113: ("Farm Frites Frozen Fries", "Frozen"),
    114: ("Halwani Beef Burger", "Frozen"),
    115: ("Schweppes Gold Peach", "Beverages"),
}

CUSTOMER_IDS = [f'cust_{i}' for i in range(500, 700)] # 200 unique customers

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit()

def generate_transaction():
    """Generates a single, realistic sales transaction with Egyptian context."""
    transaction_id = str(uuid.uuid4())
    store_id = random.choice(list(STORES.keys()))
    customer_id = random.choice(CUSTOMER_IDS)
    num_products_in_transaction = random.randint(1, 7) # Increased max products
    
    products = []
    total_amount = 0

    # Ensure unique products in a single transaction
    selected_product_ids = random.sample(list(PRODUCTS.keys()), num_products_in_transaction)

    for product_id in selected_product_ids:
        quantity = random.randint(1, 4)
        unit_price = round(random.uniform(10.0, 350.0), 2) # Prices in EGP
        
        products.append({
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price
        })
        total_amount += quantity * unit_price

    return {
        "transaction_id": transaction_id,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "store_id": store_id,
        "customer_id": customer_id,
        "products": products,
        "total_amount": round(total_amount, 2)
    }

if __name__ == "__main__":
    print(f"Starting to produce messages to topic '{KAFKA_TOPIC}'...")
    print("Using Egyptian store and product data. Press Ctrl+C to stop.")
    while True:
        try:
            transaction_data = generate_transaction()
            print(f"Producing: Store ID {transaction_data['store_id']}, Total: EGP {transaction_data['total_amount']:.2f}")
            producer.send(KAFKA_TOPIC, transaction_data)
            producer.flush()             
            time.sleep(random.uniform(0.1, 1.5))

        except KeyboardInterrupt:
            print("\nStopping producer.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)