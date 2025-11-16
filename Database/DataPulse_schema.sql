create database DataPulse_db;
use DataPulse_db;

-- Customer table:
create table customers (customer_id INT auto_increment PRIMARY KEY,
						customer_name varchar(50) Not Null,
                        email varchar(30) Unique,
                        phone varchar(10) Not Null,
                        city varchar(20),
                        created_at DATETIME	DEFAULT NOW()                        
);

-- Product table:
create table products (product_id INT auto_increment PRIMARY KEY,
						product_name varchar(50) Not Null Unique,
                        category varchar(50) Not Null,
                        selling_price decimal(10,2) Not Null,
                        cost_price decimal(10,2) Not Null,
                        stock INT CHECK(stock>0) DEFAULT 0,
                        added_at DATETIME DEFAULT NOW()                        
);

-- Orders_table
create table orders (order_id INT auto_increment PRIMARY KEY,
						customer_id int NOT NULL,
                        product_id int NOT NULL,
                        quantity INT NOT NULL,
                        order_status ENUM('Pending', 'Processing','Shipped', 'Delivered', 'Cancelled', 'Returned') DEFAULT 'Pending',
                        payment_method ENUM('Credit Card','Debit Card', 'UPI','Cash on Delivery', 'Net Banking', 'Wallet') Not Null,
                        order_date DATETIME DEFAULT NOW(),
                        constraint fk_customer foreign key (customer_id) references customers(customer_id),
                        constraint fk_product foreign key (product_id) references products(product_id)
);

-- Sales table
create table sales (sale_id INT auto_increment PRIMARY KEY,
						order_id int NOT NULL,
                        product_id int NOT NULL,
                        sale_amount DECIMAL(10,2) Not Null,
                        profit int,
                        region varchar(20),
                        sale_date DATETIME DEFAULT NOW(),
                        constraint fk_order foreign key (order_id) references orders(order_id)
);


show tables;
DESC customers;
DESC products;
DESC orders;
DESC sales;

alter table products modify column stock int DEFAULT 0 Check(stock>=0) ;
alter table sales modify column profit DECIMAL(10,2);
alter table sales drop product_id;

-- CREATING RAW/STAGING TABLES:
-- Raw Customer table:
CREATE TABLE customers_raw (
    customer_id INT,
    customer_name VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(15),
    city VARCHAR(30),
    created_at DATETIME
);

-- Raw Product table:
create table products_raw (product_id INT,
						product_name varchar(50),
                        category varchar(50),
                        selling_price decimal(10,2),
                        cost_price decimal(10,2),
                        stock INT,
                        added_at DATETIME                        
);

-- Raw Orders_table
CREATE TABLE orders_raw (
    order_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    order_status ENUM('Pending', 'Processing','Shipped', 'Delivered', 'Cancelled', 'Returned') DEFAULT 'Pending',
    payment_method ENUM('Credit Card','Debit Card', 'UPI','Cash on Delivery', 'Net Banking', 'Wallet') NOT NULL,
    order_date DATETIME DEFAULT NOW()
);

-- Raw Sales table
create table sales_raw (sale_id INT,
						order_id INT,
                        sale_amount DECIMAL(10,2),
                        profit DECIMAL(10,2),
                        region varchar(20),
                        sale_date DATETIME DEFAULT NOW()
);


ALTER TABLE customers_raw MODIFY COLUMN customer_id INT AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE products_raw MODIFY COLUMN product_id INT AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE orders_raw MODIFY COLUMN order_id INT AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE sales_raw MODIFY COLUMN sale_id INT AUTO_INCREMENT PRIMARY KEY;

ALTER TABLE customers_raw MODIFY COLUMN created_at DATETIME DEFAULT NOW();

ALTER TABLE products_raw MODIFY COLUMN stock INT CHECK(stock>=0);
ALTER TABLE products_raw MODIFY COLUMN added_at DATETIME DEFAULT NOW();

ALTER TABLE orders_raw MODIFY COLUMN order_date DATETIME DEFAULT NOW();

ALTER TABLE customers_raw MODIFY COLUMN customer_name varchar(50) NOT NULL;
ALTER TABLE customers_raw MODIFY COLUMN phone varchar(10) NOT NULL;

ALTER TABLE products_raw MODIFY COLUMN product_name varchar(50) NOT NULL;
ALTER TABLE products_raw MODIFY COLUMN category varchar(50) NOT NULL;
ALTER TABLE products_raw MODIFY COLUMN selling_price DECIMAL(10,2) NOT NULL;
ALTER TABLE products_raw MODIFY COLUMN cost_price DECIMAL(10,2) NOT NULL;


ALTER TABLE customers_raw ADD CONSTRAINT unique_phone UNIQUE (phone);
ALTER TABLE customers_raw ADD CONSTRAINT unique_email UNIQUE (email);
ALTER TABLE products_raw ADD CONSTRAINT unique_product_name UNIQUE (product_name);

ALTER TABLE customers_raw DROP CONSTRAINT unique_email;
ALTER TABLE sales_raw MODIFY COLUMN order_id INT UNIQUE;

show tables;
desc orders_raw;
desc customers_raw;
desc products_raw;
desc sales_raw;
