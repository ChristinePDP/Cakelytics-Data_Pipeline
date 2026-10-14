## 2. Source Tables and Column Specification

Only fields required for sales, expense, and profit pipelines are extracted.

### 2.1 orders

* **Primary Key:** `id` (`uuid`)
* **Purpose:** Stores header information for online and walk-in sales transactions.

| Column | Source Type | Constraint / Relationship | Reason for Inclusion |
|---|---|---|---|
| `id` | `uuid` | PK | Uniquely identifies orders; target for `order_items.order_id` join. |
| `order_number` | `text` | Unique | Human-readable order identifier (`ORD-XXXX`). |
| `order_type` | `order_type` (enum) | Required | Distinguishes 'Pre-Order' vs 'Buy Now'. |
| `source` | `order_source` (enum) | Required | Distinguishes 'online' vs 'walk-in' sales. |
| `status` | `order_status` (enum) | Required | Filters completed vs cancelled orders in Transformation. |
| `grand_total` | `numeric(10,2)` | Required | Reference value for order total validation. |
| `created_at` | `timestamptz` | Required | Transaction date/time timestamp. |
| `updated_at` | `timestamptz` | Required | Incremental extraction watermark field. |

### 2.2 order_items

* **Primary Key:** `id` (`uuid`)
* **Purpose:** Captures individual line items within an order.

| Column | Source Type | Constraint / Relationship | Reason for Inclusion |
|---|---|---|---|
| `id` | `uuid` | PK | Line-item primary key. |
| `order_id` | `uuid` | FK -> `orders.id` | Links item to parent order. |
| `product_id` | `uuid` | FK -> `products.id` (Nullable) | Identifies product reference. |
| `product_name` | `text` | Required | Snapshot product name. |
| `variant_label` | `text` | Nullable | Captures size/variant info. |
| `quantity` | `integer` | `> 0` | Units sold. |
| `unit_price` | `numeric(10,2)` | `>= 0` | Unit selling price. |
| `total_price` | `numeric(10,2)` | `>= 0` | Line-item total revenue (`quantity * unit_price`). |

### 2.3 waste_logs

* **Primary Key:** `id` (`uuid`)
* **Purpose:** Tracks spoiled or damaged materials/products contributing to daily waste expenses.

| Column | Source Type | Constraint / Relationship | Reason for Inclusion |
|---|---|---|---|
| `id` | `uuid` | PK | Primary key. |
| `waste_type` | `waste_type` (enum) | Required | Classifies waste ('ingredient', 'material', 'product'). |
| `item_name` | `text` | Required | Name of wasted item. |
| `quantity` | `numeric(12,4)` | `> 0` | Waste quantity. |
| `unit` | `text` | Required | Measurement unit. |
| `cost` | `numeric(10,2)` | `>= 0` | Financial loss value from waste. |
| `logged_at` | `timestamptz` | Required | Watermark field for incremental extraction. |

### 2.4 production_logs & production_deductions

* **Primary Key:** `production_logs.id` (`uuid`), `production_deductions.id` (`uuid`)
* **Purpose:** Tracks inventory material deductions associated with product production runs.

| Column | Source Table | Source Type | Reason for Inclusion |
|---|---|---|---|
| `id` | `production_logs` | `uuid` | Primary key. |
| `produced_at` | `production_logs` | `timestamptz` | Extraction watermark field. |
| `product_name` | `production_logs` | `text` | Product baked/produced. |
| `batches` | `production_logs` | `integer` | Number of batches produced. |
| `item_name` | `production_deductions` | `text` | Name of raw ingredient/material deducted. |
| `quantity` | `production_deductions` | `numeric(12,4)` | Amount consumed during production. |
| `unit` | `production_deductions` | `text` | Unit of measurement. |

---