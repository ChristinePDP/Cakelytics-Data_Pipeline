# Stage 1: Data Extraction Documentation

This document extends the pipeline design from Lab 2 (Technical Metadata Documentation) by specifying, in full technical detail, how the Extraction stage of the CakeLytics ETL pipeline is implemented, validated, logged, and handed over to the Transformation stage.

## 1. Data Source and Extraction Specification

### 1.1 Source System

| **Field**                | **Detail**                                                                                                                                                                                                                                                                         |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Name**                 | CakeLytics Operational System                                                                                                                                                                                                                                                      |
| **Purpose**              | A web-based Decision Support System built for Aileen and Niculus Bake Shop (Calaca City, Batangas). It manages online ordering, walk-in Point-of-Sale (POS) transactions, stock inventory (raw ingredients, celebration materials, recipes, produced products), and waste logging. |
| **Role in the Pipeline** | This system is the source — the ETL pipeline only ever reads from it, and never writes back to it.                                                                                                                                                                                 |

### 1.2 Source Database

| **Field**                      | **Detail**                                       |
|--------------------------------|--------------------------------------------------|
| **Database Management System** | PostgreSQL, hosted and managed through Supabase. |

### 1.3 Extraction Method

The pipeline uses incremental extraction. Each run retrieves data for one target calendar day instead of extracting the entire operational database.

Incremental field and logic, per table:

| **Table**            | **Incremental Field** | **Logic**                                                          |
|----------------------|-----------------------|--------------------------------------------------------------------|
| orders / order_items | orders.updated_at     | Row is included if updated_at falls within the target day's window |
| waste_logs           | logged_at             | Row is included if logged_at falls within the target day's window  |
| inventory_logs       | created_at            | Row is included if created_at falls within the target day's window |

The extraction functions connect to the source database through the shared SQLAlchemy engine and execute parameterized SQL SELECT queries. The retrieved data is loaded into Pandas DataFrames for use by the Transformation stage.

| **Field**                   | **Detail**                                                                                                                                                                                |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Extraction Tool**         | Python extraction functions using SQLAlchemy and Pandas, orchestrated by Prefect.                                                                                                         |
| **Extraction Schedule**     | The pipeline is designed for daily execution at 12:00 AM. However, the automatic schedule is not currently enabled. The pipeline can be run manually for live testing or data processing. |
| **Extraction Window**       | One full calendar day, from 12:00:00 AM to 11:59:59 PM of the target date.                                                                                                                |
| **Default Target Date**     | The previous day, based on the computer's local date and time.                                                                                                                            |
| **Historical Reprocessing** | A specific past date can be provided through the target_date parameter to reprocess data for that day.                                                                                    |

### 1.4 Extraction Scope

The Extraction stage retrieves sales and expense-related data from the CakeLytics operational database. Only records within the target date are included, subject to the conditions listed below.

| **Table**              | **Data Extracted**                                                      |
|------------------------|-------------------------------------------------------------------------|
| orders and order_items | Order line items, including quantity and total price, for each order    |
| waste_logs             | Cost of spoiled, unsold, or damaged items                               |
| inventory_logs         | Cost of stock movements involving ingredients and celebration materials |

Date Range

- Each run covers one full calendar day, from 12:00:00 AM to 11:59:59 PM of the target date.

- By default, the target date is the previous day because the pipeline is designed to run at 12:00 AM after the business day has ended.

- A specific past date may also be provided through the target_date parameter to reprocess data for that day.

The extraction stage only retrieves the required source data. Data cleaning, standardization, and business calculations, such as total sales, total expenses, and profit, are handled in the Transformation stage.

### 1.5 Source Limitations and Assumptions

- **Schema Dependency:** The extraction queries depend on specific table and column names in the CakeLytics operational database, such as total_price, cost, logged_at, and created_at. Changes to the source database schema may cause extraction queries to fail and must be communicated to the ETL maintainer.

- **Database Connection Limits:** The Supabase database has connection limits. The pipeline uses a shared SQLAlchemy engine to reduce unnecessary database connections during extraction.

- **Limited Historical Data:** The pipeline can only extract records that exist in the CakeLytics operational database. It cannot produce historical records for dates before the system began recording data.

- **Order Date Assumption:** The pipeline uses orders.updated_at to determine the extraction date for sales-related records. It assumes that this field corresponds to the relevant order update for sales reporting. If an order is updated for another reason, its records may be included in a different day's extraction.

## 2. Source Tables and Column Specification

### 2.1 orders Table

| **Field**                | **Detail**                                                                                                                                                                                                                                                                                                           |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Purpose**              | The orders table provides order information used together with the order_items table to retrieve sales-related records for the target date.                                                                                                                                                                          |
| **Important Columns**    | id (UUID), updated_at (TIMESTAMP), status (VARCHAR)                                                                                                                                                                                                                                                                  |
| **Primary Key**          | The id column serves as the primary key of the orders table and identifies each order.                                                                                                                                                                                                                               |
| **Relationships**        | The orders table is joined with order_items using orders.id = order_items.order_id. This allows the query to retrieve the quantity and total price associated with each order.                                                                                                                                       |
| **Reason for Inclusion** | The orders table provides the date and status information needed by the sales extraction query. Only order items associated with orders within the specified date range and with a status other than Cancelled are retrieved. The orders.id column is used for the join but is not included in the extracted result. |

### 2.2 order_items Table

| **Field**                | **Detail**                                                                                                                                                                                                                                |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Purpose**              | The order_items table contains the individual items included in customer orders. It provides the quantity and total price needed for sales reporting.                                                                                     |
| **Important Columns**    | order_id (UUID, FK), quantity (INTEGER), total_price (NUMERIC)                                                                                                                                                                            |
| **Primary Key**          | The extraction query does not retrieve an individual order-item identifier. Therefore, this documentation focuses on the columns used by the query rather than assuming an identifier is part of the extracted result.                    |
| **Relationships**        | The order_items table is joined with the orders table through order_items.order_id = orders.id.                                                                                                                                           |
| **Reason for Inclusion** | The order_items table provides the quantity and total price used by the Transformation stage to calculate sales-related totals. The SQL query retrieves these values together with the corresponding order's date and status information. |

### 2.3 waste_logs

| **Field**                | **Detail**                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Purpose**              | The waste_logs table provides records of wasted items and their associated costs. These records are used by the Transformation stage to calculate the total waste cost for the target date.                                                                                                                                                                                                                  |
| **Important Columns**    | waste_type (VARCHAR), item_name (VARCHAR), quantity (NUMERIC), unit (VARCHAR), cost (NUMERIC), logged_at (TIMESTAMP), voided_at (TIMESTAMP, nullable)                                                                                                                                                                                                                                                        |
| **Primary Key**          | The extraction query does not retrieve a primary-key column. Therefore, this section focuses on the columns selected and used by the extraction process.                                                                                                                                                                                                                                                     |
| **Relationships**        | The extraction query reads directly from waste_logs. It does not join this table with another source table.                                                                                                                                                                                                                                                                                                  |
| **Reason for Inclusion** | The waste_logs table provides the cost data needed to calculate waste expenses. The SQL query retrieves records whose logged_at values fall within the specified date range and whose voided_at values are NULL. The extracted records are then passed to the Transformation stage, where duplicate rows are removed, records without a cost value are excluded, and the remaining costs are added together. |

### 2.4 inventory_logs

| **Field**                | **Detail**                                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Purpose**              | The inventory_logs table provides records of inventory transactions and their associated costs. These records are used by the Transformation stage to calculate inventory-related expenses for the target date.                                                                                                                                                                                                               |
| **Important Columns**    | item_type (VARCHAR), item_name (VARCHAR), transaction_type (VARCHAR), cost (NUMERIC), created_at (TIMESTAMP), voided_at (TIMESTAMP, nullable)                                                                                                                                                                                                                                                                                 |
| **Primary Key**          | The extraction query does not retrieve a primary-key column. Therefore, this section focuses on the columns selected and used by the extraction process.                                                                                                                                                                                                                                                                      |
| **Relationships**        | The extraction query reads directly from inventory_logs. It does not join this table with another source table.                                                                                                                                                                                                                                                                                                               |
| **Reason for Inclusion** | The inventory_logs table provides the cost data needed to calculate inventory-related expenses. The SQL query retrieves records whose created_at values fall within the specified date range and whose voided_at values are NULL. The extracted records are then passed to the Transformation stage, where duplicate rows are removed, records without a cost value are excluded, and the remaining costs are added together. |

## 3. Extraction Validation and Data Quality Checks

The CakeLytics ETL pipeline performs basic checks during extraction, including checking source database connectivity, executing SQL queries, and recording the number of rows returned. However, the current extraction code does not implement separate validation routines for schema existence, primary-key uniqueness, or extraction completeness. These limitations are documented below to distinguish implemented checks from checks that are not yet automated.

| **\#** | **Check Name**                  | **Target**                                                   | **Purpose**                                                         | **Validation Criteria**                                                                                                                                            |
|--------|---------------------------------|--------------------------------------------------------------|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1      | Source connectivity             | Source database connection                                   | Confirm the source database is reachable before any query runs      | The check_source_env() function attempts to open a source database connection and closes it after the connection check. A connection error interrupts the pipeline |
| 2      | Required table/column existence | orders, order_items, waste_logs, inventory_logs              | Detect schema drift before relying on query results                 | All columns referenced in the extraction queries exist with the expected names                                                                                     |
| 3      | Primary key presence            | orders.id, line items, log entries                           | Ensure every extracted row has a usable identifier                  | No NULLs in the identifier column of any extracted row                                                                                                             |
| 4      | Primary key uniqueness          | orders.id                                                    | Detect duplicate rows caused by a faulty join or query              | Count of distinct id values equals row count                                                                                                                       |
| 5      | Required field completeness     | total_price (order_items), cost (waste_logs, inventory_logs) | Prevent silently undercounting sales/expenses due to missing values | No NULLs in total_price or cost for extracted rows; NULLs found are flagged, not silently dropped                                                                  |
| 6      | Record count logging            | Each table's extraction result                               | Catch unexpectedly empty or abnormally large extractions            | Record count \>= 0 and below a configurable upper bound                                                                                                            |
| 7      | Referential integrity           | order_items.order_id -\> orders.id                           | Ensure no orphaned line items are summed into sales                 | Every extracted order_items.order_id has a matching row in the extracted orders set                                                                                |
| 8      | Extraction completeness         | Returned sales and expense records                           | Detect partial extraction from a dropped connection mid-query       | Rows extracted matches rows read from the source for the same query                                                                                                |

Data cleaning, including duplicate-row removal and handling missing total_price or cost values, is performed in the Transformation functions. These operations are not part of the Extraction stage.

## 4. Extraction Metadata and Log Specification

The Extraction stage records log messages when retrieving data from the source database. These messages include the extraction activity, the target date range, and the number of rows returned by each extraction query. Each log entry also contains a timestamp, log level, and module name.

| **Logged Information**   | **Purpose**                                                    | **Illustrative Example**                                                       |
|--------------------------|----------------------------------------------------------------|--------------------------------------------------------------------------------|
| Timestamp                | Records when the extraction message was logged.                | 2026-10-13 18:59:22                                                            |
| Log Level                | Indicates the type of log message.                             | INFO                                                                           |
| Module Name              | Identifies the extraction function that generated the message. | CakelyticsETL.extract.extract_sales                                            |
| Extraction Activity      | Shows which source data is being retrieved.                    | Extracting sales (order_items) from 2026-10-12 00:00:00 to 2026-10-12 23:59:59 |
| Extraction Date Range    | Shows the start and end of the target date window.             | 2026-10-12 00:00:00 to 2026-10-12 23:59:59                                     |
| Number of Rows Extracted | Records the number of rows returned by the query.              | Extracted 30 order_items rows                                                  |

#### Use of Extraction Logs

- **Monitoring:** Shows when each extraction query starts and how many rows it returns.

- **Troubleshooting:** Helps identify which extraction function was running and which date range it used.

- **Record Tracking:** Provides a record of the number of rows returned by each extraction query.

## 5. Log Retention and Access

| **Aspect**         | **Policy**                                                                                                                                                       | **Current Implementation**                                                     |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| Storage Location   | Store operational log files in the project's logs directory.                                                                                                     | Implemented.                                                                   |
| Retention Period   | Retain operational logs for 90 days. Retain any separately maintained audit records for one year, if such records are introduced.                                | No automatic retention period is configured.                                   |
| Access Permissions | Limit access to log files to authorized project developers and personnel responsible for maintaining the pipeline.                                               | Separate role-based access permissions are not configured by the logging code. |
| Archiving          | Archive operational logs older than 90 days when they are still needed for troubleshooting or auditing.                                                          | Automatic archiving is not implemented.                                        |
| Deletion Rules     | Delete operational logs only after the proposed retention period and after confirming that they are no longer needed for troubleshooting, auditing, or recovery. | Automatic log deletion is not implemented.                                     |

### Retention and Access Considerations

- Troubleshooting: Recent logs should remain available to help developers investigate failed extraction runs and database connection problems.

- Auditing: Logs containing useful run details should be retained when needed to trace the target date, extracted record counts, and processing results.

- Recovery: Logs should not be deleted while they are needed to identify or investigate a date that requires reprocessing.

- Security: Database credentials and other sensitive configuration values should not be included in log messages. Access to stored logs should be limited to authorized personnel.

