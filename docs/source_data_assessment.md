# Source Data Assessment

## Dataset

Instacart Market Basket Analysis

---

## aisles.csv

### Business Meaning

Contains aisle master data.

### Grain

One row per aisle.

### Candidate Primary Key

aisle_id

### Relationships

products.aisle_id -> aisles.aisle_id

### Data Quality Risks

* Duplicate aisle_id
* Null aisle name

---

## departments.csv

### Business Meaning

Contains department master data.

### Grain

One row per department.

### Candidate Primary Key

department_id

### Relationships

products.department_id -> departments.department_id

### Data Quality Risks

* Duplicate department_id
* Null department name

---

## products.csv

### Business Meaning

Contains product master information.

### Grain

One row per product.

### Candidate Primary Key

product_id

### Relationships

products.aisle_id -> aisles.aisle_id

products.department_id -> departments.department_id

order_products.product_id -> products.product_id

### Data Quality Risks

* Duplicate product_id
* Null product_name
* Invalid aisle_id
* Invalid department_id

---

## orders.csv

### Business Meaning

Contains customer order headers.

### Grain

One row per order.

### Candidate Primary Key

order_id

### Relationships

orders.order_id -> order_products.order_id

### Data Quality Risks

* Duplicate order_id
* Null user_id
* Invalid order_number
* Missing days_since_prior_order

---

## order_products__prior.csv

### Business Meaning

Historical products purchased in orders.

### Grain

One row per product within an order.

### Candidate Primary Key

(order_id, product_id)

### Relationships

order_products.order_id -> orders.order_id

order_products.product_id -> products.product_id

### Data Quality Risks

* Duplicate order-product combinations
* Invalid order_id
* Invalid product_id

---

## order_products__train.csv

### Business Meaning

Training dataset containing products purchased in orders.

### Grain

One row per product within an order.

### Candidate Primary Key

(order_id, product_id)

### Relationships

order_products.order_id -> orders.order_id

order_products.product_id -> products.product_id

### Data Quality Risks

* Duplicate order-product combinations
* Invalid order_id
* Invalid product_id
