## 3. Extraction Validation and Data Quality Checks

| Check Name | Target | Purpose | Validation Criteria|
|---|---|---|---|
| **Database Connection** | Supabase PostgreSQL Engine | Ensure database is online prior to execution. | **PASS:** Connection established within 10s.<br>**FAIL:** Connection error/timeout. |
| **Schema Validation** | All target tables & columns | Verify source schema structure matches expectation. | **PASS:** All tables and columns present.<br>**FAIL:** Table or column missing. |
| **Identifier Null Check** | Primary Keys (`id`) & Foreign Keys (`order_id`) | Ensure critical operational IDs exist. | **PASS:** Zero NULL values in key fields.<br>**FAIL:** `id` or `order_id` is NULL. |
| **PK Uniqueness** | Extracted datasets | Detect duplicate operational records. | **PASS:** `count(id) == count(distinct id)`.<br>**FAIL:** Duplicate primary keys detected. |
| **Record Count Reconciliation** | Extraction Queries vs In-Memory DataFrames | Confirm no dropped rows during memory loading. | **PASS:** Query row count equals DataFrame length.<br>**FAIL:** Row counts do not match. |
| **Sales Join Integrity** | `order_items` joined to `orders` | Identify orphan line items missing parent order. | **PASS:** 0 orphaned line items.<br>**FAIL:** Orphaned items found (Hold for review). |
| **Watermark Boundary Check** | Extracted Timestamps | Confirm extracted records fall strictly within target date window. | **PASS:** All timestamps fit extraction window.<br>**FAIL:** Out-of-bounds timestamps detected. |

---