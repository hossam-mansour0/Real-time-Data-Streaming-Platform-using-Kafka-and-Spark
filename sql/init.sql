-- -- sql/init.sql

CREATE TABLE dim_stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(255),
    city VARCHAR(255),
    governorate VARCHAR(100)
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100)
);

CREATE TABLE dim_customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    join_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day_of_week VARCHAR(20),
    month VARCHAR(20),
    year INT
);

-- Fact Table
CREATE TABLE fact_sales (
    sale_id VARCHAR(255) PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    store_id INT REFERENCES dim_stores(store_id),
    product_id INT REFERENCES dim_products(product_id),
    customer_id VARCHAR(255) REFERENCES dim_customers(customer_id),
    quantity_sold INT,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2)
);

INSERT INTO dim_stores (store_id, store_name, city, governorate) VALUES
(1, 'Cairo Festival City', 'New Cairo', 'Cairo'),
(2, 'Mall of Arabia', '6th of October', 'Giza'),
(3, 'City Stars', 'Nasr City', 'Cairo'),
(4, 'San Stefano Grand Plaza', 'San Stefano', 'Alexandria'),
(5, 'Mall of Egypt', '6th of October', 'Giza'),
(6, 'Point 90', 'New Cairo', 'Cairo'),
(7, 'Downtown Katameya', 'New Cairo', 'Cairo'),
(8, 'City Centre Almaza', 'Heliopolis', 'Cairo');

INSERT INTO dim_products (product_id, product_name, category) VALUES
(101, 'Juhayna Full Cream Milk 1L', 'Dairy'),
(102, 'Domty Feta Cheese 500g', 'Dairy'),
(103, 'Fine Healthy Tissues', 'Household'),
(104, 'Edita Molto Croissant', 'Bakery'),
(105, 'Chipsy Salt & Vinegar', 'Snacks'),
(106, 'Coca-Cola 1L', 'Beverages'),
(107, 'Abu Auf Turkish Coffee', 'Pantry'),
(108, 'Lipton Yellow Label Tea', 'Pantry'),
(109, 'Indomie Chicken Noodles', 'Pantry'),
(110, 'El-Shams Rice 1kg', 'Pantry'),
(111, 'Persil Gel Detergent 2.5L', 'Household'),
(112, 'Eva Honey Body Lotion', 'Personal Care'),
(113, 'Farm Frites Frozen Fries', 'Frozen'),
(114, 'Halwani Beef Burger', 'Frozen'),
(115, 'Schweppes Gold Peach', 'Beverages');