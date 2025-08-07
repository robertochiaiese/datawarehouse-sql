# Data Catalog for Gold Layer

## Overview

The **Gold Layer** represents the business-ready data model, optimized for analytics and reporting. It consists of **dimension tables** that provide descriptive context and **fact tables** that store measurable business events.

---

## 1. `gold.dim_customers`

**Purpose:**  
Captures enriched customer information, integrating demographic and geographic attributes to support customer analysis and segmentation.

**Columns:**

| Column Name     | Data Type     | Description |
|------------------|---------------|-------------|
| `customer_key`     | INT           | A surrogate key uniquely identifying each customer in the dimension table. |
| `customer_id`      | INT           | A system-generated unique numeric ID assigned to each customer. |
| `customer_number`  | NVARCHAR(50)  | An alphanumeric customer identifier used for external tracking and referencing. |
| `first_name`       | NVARCHAR(50)  | The customer's first name as recorded in the source system. |
| `last_name`        | NVARCHAR(50)  | The customer's last name or surname. |
| `country`          | NVARCHAR(50)  | The customer's country of residence (e.g., 'Australia'). |
| `marital_status`   | NVARCHAR(50)  | The customer's marital status (e.g., 'Married', 'Single'). |
| `gender`           | NVARCHAR(50)  | The customer's gender (e.g., 'Male', 'Female', 'n/a'). |
| `birthdate`        | DATE          | The customer's birth date in `YYYY-MM-DD` format. |
| `create_date`      | DATE          | The timestamp when the customer record was created in the system. |

---

## 2. `gold.dim_products`

**Purpose:**  
Provides descriptive metadata about products, supporting classification, inventory tracking, and product-level analysis.

**Columns:**

| Column Name         | Data Type     | Description |
|----------------------|---------------|-------------|
| `product_key`          | INT           | A surrogate key uniquely identifying each product in the dimension table. |
| `product_id`           | INT           | A system-generated unique identifier assigned to each product. |
| `product_number`       | NVARCHAR(50)  | An alphanumeric code representing the product, typically used for categorization or inventory. |
| `product_name`         | NVARCHAR(50)  | A detailed product name including relevant attributes like type, size, or color. |
| `category_id`          | NVARCHAR(50)  | A unique identifier referencing the product’s category for classification purposes. |
| `category`             | NVARCHAR(50)  | The general category to which the product belongs (e.g., Bikes, Components). |
| `subcategory`          | NVARCHAR(50)  | A more specific classification within the main product category. |
| `maintenance_required` | NVARCHAR(50)  | Indicates whether the product requires ongoing maintenance (e.g., 'Yes', 'No'). |
| `cost`                 | INT           | The base cost of the product, represented in whole currency units. |
| `product_line`         | NVARCHAR(50)  | The specific product line or series (e.g., Road, Mountain). |
| `start_date`           | DATE          | The date when the product became available for sale or distribution. |

---

## 3. `gold.fact_sales`

**Purpose:**  
Stores detailed sales transaction data, enabling performance tracking, revenue analysis, and customer behavior insights.

**Columns:**

| Column Name     | Data Type     | Description |
|------------------|---------------|-------------|
| `order_number`     | NVARCHAR(50)  | A unique alphanumeric identifier for each sales transaction (e.g., 'SO54496'). |
| `product_key`      | INT           | Foreign key linking the transaction to the product dimension. |
| `customer_key`     | INT           | Foreign key linking the transaction to the customer dimension. |
| `order_date`       | DATE          | The date the sales order was placed. |
| `shipping_date`    | DATE          | The date the order was shipped to the customer. |
| `due_date`         | DATE          | The date when payment for the order was due. |
| `sales_amount`     | INT           | The total value of the sale for the line item, in whole currency units. |
| `quantity`         | INT           | The number of product units sold in the transaction. |
| `price`            | INT           | The per-unit selling price for the product in the transaction. |
