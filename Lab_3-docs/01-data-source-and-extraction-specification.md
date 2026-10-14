# Stage 1: Data Extraction Documentation

This document extends the pipeline design from Lab 2 (Technical Metadata Documentation) by specifying, in full technical detail, how the Extraction stage of the CakeLytics ETL pipeline is implemented, validated, logged, and handed over to the Transformation stage.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Field | Detail |
|---|---|
| **Name** | CakeLytics Operational System |
| **Purpose** | A web-based Decision Support System built for Aileen and Niculus Bake Shop (Calaca City, Batangas). It manages online ordering, walk-in Point-of-Sale (POS) transactions, stock inventory (raw ingredients, celebration materials, recipes, produced products), and waste logging. |
| **Role in the Pipeline** | This system is the **source**. The ETL pipeline only ever reads from it, and never writes back to it. |

### 1.2 Source Database

| Field | Detail |
|---|---|
| **Database Management System** | PostgreSQL, hosted and managed through Supabase. |

### 1.3 Extraction Method

**Type of extraction:** Incremental, not full. Each run extracts only one day's worth of new/finalized records, rather than re-reading the entire operational database every time.

**Incremental field and logic, per table:**

| Table | Incremental Field | Logic |
|---|---|---|
| `orders` / `order_items` | `orders.updated_at` | Row is included if `updated_at` falls within the target day's window |
| `waste_logs` | `logged_at` | Row is included if `logged_at` falls within the target day's window |
| `inventory_logs` | `created_at` | Row is included if `created_at` falls within the target day's window |

The extraction window for a single run is always one full calendar day (24 hrs). By default this is the previous day, but the pipeline also accepts an explicit `target_date` parameter for processing a specific historical day.

**How data is extracted:** Python functions connect to the source database through the shared SQLAlchemy engine and run parameterized SQL `SELECT` queries filtered to the extraction window. Results are loaded directly into Pandas DataFrames in memory.

| Field | Detail |
|---|---|
| **Extraction Tool** | Two dedicated extraction routines, one for sales-related data and one for expense-related data, orchestrated by Prefect, a workflow scheduling tool. Each stage of the run is logged. |
| **Extraction Schedule** | Daily at 12:00 AM, after the bake shop's business day has ended, via a Prefect scheduled deployment. |

### 1.4 Extraction Scope
 
| In Scope | Out of Scope |
|---|---|
| `order_items` joined with `orders` (for sales) | Products, occasions/events, promo bundles |
| `waste_logs` (for expenses) | Customer personal information beyond what's needed for joins |
| `inventory_logs` (for expenses) | Admin/auth data, notifications, pending/unpaid orders |
| Orders where `status != 'Cancelled'` | Cancelled orders |
| Log entries where `voided_at IS NULL` | Voided waste/inventory entries |
| One calendar day per run (previous day by default, or an explicit `target_date`) | Dates earlier than the first day with any activity in the source database |
 
### 1.5 Source Limitations and Assumptions
 
- **Schema coupling risk:** The extraction queries reference exact column names (`total_price`, `cost`, `logged_at`, etc.) from the operational schema. Since the ETL pipeline is a separate codebase from the Node.js backend, a schema change made by the development team will silently break extraction unless communicated in advance.
- **Connection limits (mitigated):** Supabase projects have a limited connection pool.
- **No historical data before system launch:** Extraction cannot produce records for dates before the CakeLytics system went live, since no data exists in the source for that period.
- **`updated_at` vs. `created_at` assumption for orders:** The pipeline assumes `orders.updated_at` reflects when an order was finalized, not merely when it was last touched for an unrelated reason. If the application updates this field for other reasons, sales could be misattributed to the wrong day.
 