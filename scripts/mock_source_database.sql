-- =============================================================================
-- CakeLytics Operational Source Database — Realistic Mock Test Dataset
-- =============================================================================
-- Date 1 (HAPPY TESTING): 2026-11-10 (100+ realistic records, 100% PASS -> APPROVED)
-- Date 2 (SAD TESTING):   2026-11-11 (Contains defects -> MANUAL REVIEW)
-- Date 3 (FAILED TESTING): 2026-11-12 (Critical invalid status -> REJECTED)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- I-drop ang mga lumang tables para sa malinis na initialization
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS waste_logs CASCADE;
DROP TABLE IF EXISTS inventory_logs CASCADE;

-- =============================================================================
-- 1. TABLE DEFINITIONS (DDL)
-- =============================================================================
-- TANDAAN: Para sa mock test database, sinadyang hindi nilagyan ng mahigpit na
-- database-level PRIMARY KEY/NOT NULL constraints ang ilang columns.
-- Ito ay upang payagan ng PostgreSQL na maipasok ang dirty test data (gaya ng
-- NULL id o duplicate id) nang sa gayon ay ang ETL pipeline mismo ang sumuri at sumalo!

CREATE TABLE orders (
    id UUID,
    status VARCHAR(50),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
    id UUID,
    order_id UUID,
    quantity NUMERIC,
    total_price NUMERIC(12, 4)
);

CREATE TABLE waste_logs (
    id UUID,
    waste_type VARCHAR(50),
    item_name VARCHAR(255),
    quantity NUMERIC(10, 2),
    unit VARCHAR(20),
    cost NUMERIC(12, 4),
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    voided_at TIMESTAMPTZ NULL
);

CREATE TABLE inventory_logs (
    id UUID,
    item_type VARCHAR(50),
    item_name VARCHAR(255),
    transaction_type VARCHAR(50),
    cost NUMERIC(12, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    voided_at TIMESTAMPTZ NULL
);

-- =============================================================================
-- 2. HAPPY TESTING: BATCH 2026-11-10 (~101 REALISTIC RECORDS -> 100% PASSED)
-- Command: python -m pipeline.run_transform 2026-11-10
-- Inaasahang resulta: APPROVED (Exit Code: 0)
-- Lahat ng validation checks (V-01 hanggang V-11) ay PASS, 0 rejects, 0 excluded.
-- =============================================================================

-- 2.1 ORDERS (30 Realistic Orders spread across business hours 8:00 AM - 8:15 PM PHT)
INSERT INTO orders (id, status, updated_at) VALUES
('10000000-0000-0000-0000-000000000001', 'Completed', '2026-11-10 08:15:20+08'),
('10000000-0000-0000-0000-000000000002', 'Completed', '2026-11-10 08:35:10+08'),
('10000000-0000-0000-0000-000000000003', 'Completed', '2026-11-10 09:05:45+08'),
('10000000-0000-0000-0000-000000000004', 'Completed', '2026-11-10 09:40:00+08'),
('10000000-0000-0000-0000-000000000005', 'Completed', '2026-11-10 10:12:30+08'),
('10000000-0000-0000-0000-000000000006', 'Completed', '2026-11-10 10:45:15+08'),
('10000000-0000-0000-0000-000000000007', 'Completed', '2026-11-10 11:20:00+08'),
('10000000-0000-0000-0000-000000000008', 'Completed', '2026-11-10 11:55:40+08'),
('10000000-0000-0000-0000-000000000009', 'Completed', '2026-11-10 12:15:10+08'),
('10000000-0000-0000-0000-000000000010', 'Completed', '2026-11-10 12:48:50+08'),
('10000000-0000-0000-0000-000000000011', 'Completed', '2026-11-10 13:10:00+08'),
('10000000-0000-0000-0000-000000000012', 'Completed', '2026-11-10 13:42:15+08'),
('10000000-0000-0000-0000-000000000013', 'Completed', '2026-11-10 14:05:30+08'),
('10000000-0000-0000-0000-000000000014', 'Completed', '2026-11-10 14:28:40+08'),
('10000000-0000-0000-0000-000000000015', 'Completed', '2026-11-10 14:50:00+08'),
('10000000-0000-0000-0000-000000000016', 'Completed', '2026-11-10 15:15:20+08'),
('10000000-0000-0000-0000-000000000017', 'Completed', '2026-11-10 15:38:10+08'),
('10000000-0000-0000-0000-000000000018', 'Completed', '2026-11-10 16:02:45+08'),
('10000000-0000-0000-0000-000000000019', 'Completed', '2026-11-10 16:25:30+08'),
('10000000-0000-0000-0000-000000000020', 'Completed', '2026-11-10 16:50:00+08'),
('10000000-0000-0000-0000-000000000021', 'Completed', '2026-11-10 17:15:10+08'),
('10000000-0000-0000-0000-000000000022', 'Completed', '2026-11-10 17:35:40+08'),
('10000000-0000-0000-0000-000000000023', 'Completed', '2026-11-10 18:00:15+08'),
('10000000-0000-0000-0000-000000000024', 'Completed', '2026-11-10 18:22:30+08'),
('10000000-0000-0000-0000-000000000025', 'Completed', '2026-11-10 18:45:00+08'),
('10000000-0000-0000-0000-000000000026', 'Completed', '2026-11-10 19:05:20+08'),
('10000000-0000-0000-0000-000000000027', 'Completed', '2026-11-10 19:20:10+08'),
('10000000-0000-0000-0000-000000000028', 'Completed', '2026-11-10 19:35:45+08'),
('10000000-0000-0000-0000-000000000029', 'Completed', '2026-11-10 19:50:00+08'),
('10000000-0000-0000-0000-000000000030', 'Completed', '2026-11-10 20:10:30+08');

-- 2.2 ORDER ITEMS (41 realistic bakery line items: Cakes, Cupcakes, Brownies, Pastries)
INSERT INTO order_items (id, order_id, quantity, total_price) VALUES
-- Order 1
('11000000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000001', 1, 850.00), -- Ube Bloom Cake 8x8
('11000000-0000-0000-0000-000000000002', '10000000-0000-0000-0000-000000000001', 2, 360.00), -- Brownies Box
-- Order 2
('11000000-0000-0000-0000-000000000003', '10000000-0000-0000-0000-000000000002', 1, 950.00), -- Chocolate Truffle Cake
-- Order 3
('11000000-0000-0000-0000-000000000004', '10000000-0000-0000-0000-000000000003', 2, 480.00), -- Cupcake Pack of 6
('11000000-0000-0000-0000-000000000005', '10000000-0000-0000-0000-000000000003', 1, 150.00), -- Choco Chip Cookies
-- Order 4
('11000000-0000-0000-0000-000000000006', '10000000-0000-0000-0000-000000000004', 1, 780.00), -- Red Velvet Cake
-- Order 5
('11000000-0000-0000-0000-000000000007', '10000000-0000-0000-0000-000000000005', 3, 540.00), -- Mango Bravo Slice
('11000000-0000-0000-0000-000000000008', '10000000-0000-0000-0000-000000000005', 2, 130.00), -- Cheese Ensaymada
-- Order 6
('11000000-0000-0000-0000-000000000009', '10000000-0000-0000-0000-000000000006', 1, 650.00), -- Mocha Crunch Cake
-- Order 7
('11000000-0000-0000-0000-000000000010', '10000000-0000-0000-0000-000000000007', 2, 440.00), -- Banana Walnut Loaf
('11000000-0000-0000-0000-000000000011', '10000000-0000-0000-0000-000000000007', 1, 320.00), -- Custard Cake Roll
-- Order 8
('11000000-0000-0000-0000-000000000012', '10000000-0000-0000-0000-000000000008', 1, 820.00), -- Carrot Walnut Cake
-- Order 9
('11000000-0000-0000-0000-000000000013', '10000000-0000-0000-0000-000000000009', 4, 300.00), -- Blueberry Muffins
-- Order 10
('11000000-0000-0000-0000-000000000014', '10000000-0000-0000-0000-000000000010', 1, 900.00), -- Strawberry Shortcake
('11000000-0000-0000-0000-000000000015', '10000000-0000-0000-0000-000000000010', 2, 520.00), -- Red Velvet Cupcakes (6s)
-- Order 11
('11000000-0000-0000-0000-000000000016', '10000000-0000-0000-0000-000000000011', 1, 750.00), -- Classic Cheesecake
-- Order 12
('11000000-0000-0000-0000-000000000017', '10000000-0000-0000-0000-000000000012', 3, 450.00), -- Choco Chip Cookies
-- Order 13
('11000000-0000-0000-0000-000000000018', '10000000-0000-0000-0000-000000000013', 1, 850.00), -- Ube Bloom Cake 8x8
('11000000-0000-0000-0000-000000000019', '10000000-0000-0000-0000-000000000013', 1, 180.00), -- Brownies Box
-- Order 14
('11000000-0000-0000-0000-000000000020', '10000000-0000-0000-0000-000000000014', 1, 950.00), -- Chocolate Truffle Cake
-- Order 15
('11000000-0000-0000-0000-000000000021', '10000000-0000-0000-0000-000000000015', 2, 420.00), -- Vanilla Butter Cupcakes
-- Order 16
('11000000-0000-0000-0000-000000000022', '10000000-0000-0000-0000-000000000016', 1, 780.00), -- Red Velvet Cake
('11000000-0000-0000-0000-000000000023', '10000000-0000-0000-0000-000000000016', 2, 260.00), -- Cheese Ensaymada
-- Order 17
('11000000-0000-0000-0000-000000000024', '10000000-0000-0000-0000-000000000017', 2, 360.00), -- Mango Bravo Slices
-- Order 18
('11000000-0000-0000-0000-000000000025', '10000000-0000-0000-0000-000000000018', 1, 650.00), -- Mocha Crunch Cake
-- Order 19
('11000000-0000-0000-0000-000000000026', '10000000-0000-0000-0000-000000000019', 1, 320.00), -- Custard Cake Roll
('11000000-0000-0000-0000-000000000027', '10000000-0000-0000-0000-000000000019', 1, 220.00), -- Banana Walnut Loaf
-- Order 20
('11000000-0000-0000-0000-000000000028', '10000000-0000-0000-0000-000000000020', 1, 820.00), -- Carrot Walnut Cake
-- Order 21
('11000000-0000-0000-0000-000000000029', '10000000-0000-0000-0000-000000000021', 3, 225.00), -- Blueberry Muffins
-- Order 22
('11000000-0000-0000-0000-000000000030', '10000000-0000-0000-0000-000000000022', 1, 900.00), -- Strawberry Shortcake
-- Order 23
('11000000-0000-0000-0000-000000000031', '10000000-0000-0000-0000-000000000023', 2, 480.00), -- Cupcakes (Pack of 6)
('11000000-0000-0000-0000-000000000032', '10000000-0000-0000-0000-000000000023', 1, 180.00), -- Brownies Box
-- Order 24
('11000000-0000-0000-0000-000000000033', '10000000-0000-0000-0000-000000000024', 1, 750.00), -- Classic Cheesecake
-- Order 25
('11000000-0000-0000-0000-000000000034', '10000000-0000-0000-0000-000000000025', 2, 300.00), -- Choco Chip Cookies
-- Order 26
('11000000-0000-0000-0000-000000000035', '10000000-0000-0000-0000-000000000026', 1, 850.00), -- Ube Bloom Cake 8x8
-- Order 27
('11000000-0000-0000-0000-000000000036', '10000000-0000-0000-0000-000000000027', 1, 950.00), -- Chocolate Truffle Cake
('11000000-0000-0000-0000-000000000037', '10000000-0000-0000-0000-000000000027', 3, 195.00), -- Cheese Ensaymada
-- Order 28
('11000000-0000-0000-0000-000000000038', '10000000-0000-0000-0000-000000000028', 1, 780.00), -- Red Velvet Cake
-- Order 29
('11000000-0000-0000-0000-000000000039', '10000000-0000-0000-0000-000000000029', 2, 520.00), -- Red Velvet Cupcakes
('11000000-0000-0000-0000-000000000040', '10000000-0000-0000-0000-000000000029', 1, 180.00), -- Brownies Box
-- Order 30
('11000000-0000-0000-0000-000000000041', '10000000-0000-0000-0000-000000000030', 1, 650.00); -- Mocha Crunch Cake

-- 2.3 WASTE LOGS (12 realistic waste entries, walang voided)
INSERT INTO waste_logs (id, waste_type, item_name, quantity, unit, cost, logged_at, voided_at) VALUES
('12000000-0000-0000-0000-000000000001', 'ingredient', 'Fresh Milk 1L', 1.0, 'pcs', 95.00, '2026-11-10 09:30:00+08', NULL),
('12000000-0000-0000-0000-000000000002', 'ingredient', 'Fresh Eggs (Cracked)', 6.0, 'pcs', 48.00, '2026-11-10 10:15:00+08', NULL),
('12000000-0000-0000-0000-000000000003', 'ingredient', 'Chocolate Batter Residue', 0.5, 'kg', 140.00, '2026-11-10 11:45:00+08', NULL),
('12000000-0000-0000-0000-000000000004', 'product', 'Cracked Vanilla Cake Base', 1.0, 'pcs', 220.00, '2026-11-10 13:20:00+08', NULL),
('12000000-0000-0000-0000-000000000005', 'product', 'Overbaked Brownies', 4.0, 'pcs', 120.00, '2026-11-10 14:10:00+08', NULL),
('12000000-0000-0000-0000-000000000006', 'material', 'Torn Cake Box 8x8', 3.0, 'pcs', 45.00, '2026-11-10 15:00:00+08', NULL),
('12000000-0000-0000-0000-000000000007', 'ingredient', 'Spilled Whipping Cream', 0.5, 'L', 125.00, '2026-11-10 15:45:00+08', NULL),
('12000000-0000-0000-0000-000000000008', 'product', 'Stale Blueberry Muffin', 2.0, 'pcs', 110.00, '2026-11-10 16:30:00+08', NULL),
('12000000-0000-0000-0000-000000000009', 'material', 'Bent Cake Board 10in', 2.0, 'pcs', 30.00, '2026-11-10 17:15:00+08', NULL),
('12000000-0000-0000-0000-000000000010', 'ingredient', 'Expired Dry Yeast', 2.0, 'pcs', 60.00, '2026-11-10 18:00:00+08', NULL),
('12000000-0000-0000-0000-000000000011', 'ingredient', 'Fruit Puree Residue', 0.3, 'kg', 90.00, '2026-11-10 18:45:00+08', NULL),
('12000000-0000-0000-0000-000000000012', 'ingredient', 'Burnt Caramel Sauce', 0.2, 'kg', 45.00, '2026-11-10 19:30:00+08', NULL);

-- 2.4 INVENTORY LOGS (18 realistic restock & usage movements, walang voided)
INSERT INTO inventory_logs (id, item_type, item_name, transaction_type, cost, created_at, voided_at) VALUES
('13000000-0000-0000-0000-000000000001', 'raw', 'All Purpose Flour 25kg', 'IN', 950.00, '2026-11-10 08:00:00+08', NULL),
('13000000-0000-0000-0000-000000000002', 'raw', 'Refined White Sugar 25kg', 'IN', 1400.00, '2026-11-10 08:10:00+08', NULL),
('13000000-0000-0000-0000-000000000003', 'raw', 'Unsalted Butter 10kg', 'IN', 2800.00, '2026-11-10 08:20:00+08', NULL),
('13000000-0000-0000-0000-000000000004', 'raw', 'Fresh Eggs (10 Trays)', 'IN', 2400.00, '2026-11-10 08:30:00+08', NULL),
('13000000-0000-0000-0000-000000000005', 'raw', 'Dutch Cocoa Powder 5kg', 'IN', 1250.00, '2026-11-10 08:40:00+08', NULL),
('13000000-0000-0000-0000-000000000006', 'raw', 'Whole Milk (12x1L Case)', 'IN', 1080.00, '2026-11-10 08:50:00+08', NULL),
('13000000-0000-0000-0000-000000000007', 'raw', 'Pure Vanilla Extract 1L', 'IN', 850.00, '2026-11-10 09:00:00+08', NULL),
('13000000-0000-0000-0000-000000000008', 'raw', 'Cream Cheese 2kg Block', 'IN', 980.00, '2026-11-10 09:10:00+08', NULL),
('13000000-0000-0000-0000-000000000009', 'material', 'Cake Boxes 8x8 (Pack of 100)', 'IN', 850.00, '2026-11-10 09:20:00+08', NULL),
('13000000-0000-0000-0000-000000000010', 'material', 'Cake Boards 10in (Pack of 50)', 'IN', 420.00, '2026-11-10 09:30:00+08', NULL),
('13000000-0000-0000-0000-000000000011', 'material', 'Disposable Pastry Bags (100s)', 'IN', 220.00, '2026-11-10 09:40:00+08', NULL),
('13000000-0000-0000-0000-000000000012', 'raw', 'Flour Stock Movement', 'OUT', 0.00, '2026-11-10 12:00:00+08', NULL),
('13000000-0000-0000-0000-000000000013', 'raw', 'Sugar Stock Movement', 'OUT', 0.00, '2026-11-10 13:00:00+08', NULL),
('13000000-0000-0000-0000-000000000014', 'raw', 'Butter Stock Movement', 'OUT', 0.00, '2026-11-10 14:00:00+08', NULL),
('13000000-0000-0000-0000-000000000015', 'raw', 'Eggs Stock Movement', 'OUT', 0.00, '2026-11-10 15:00:00+08', NULL),
('13000000-0000-0000-0000-000000000016', 'raw', 'Milk Stock Movement', 'OUT', 0.00, '2026-11-10 16:00:00+08', NULL),
('13000000-0000-0000-0000-000000000017', 'material', 'Box Stock Movement', 'OUT', 0.00, '2026-11-10 17:00:00+08', NULL),
('13000000-0000-0000-0000-000000000018', 'material', 'Board Stock Movement', 'OUT', 0.00, '2026-11-10 18:00:00+08', NULL);


-- =============================================================================
-- 3. SAD TESTING: BATCH 2026-11-11 (COMPLETE DEFECT & FAILURE TEST DATASET)
-- Command: python -m pipeline.run_transform 2026-11-11
-- Sinusubok ang BUONG FAILURE & DEFECT MATRIX ng ETL Pipeline:
--   [Extract Check 3 / Rule C-01] NULL primary keys (id IS NULL)
--   [Extract Check 4 / Rule C-02] Duplicate primary keys (id naulit)
--   [Rule C-03] Missing Amount (total_price / cost IS NULL)
--   [Rule C-04] Negative Amount (total_price / cost < 0)
--   [Rule C-05] Invalid Quantity (quantity <= 0)
--   [Rule C-07] Leading/Trailing Whitespace (item_name may spaces na dapat i-trim)
--   [Rule S-03] Invalid Numeric Precision (may sobra sa 2 decimal places)
--   [Rule S-04] Casing mapping (transaction_type = 'in' o 'Out' na dapat maging 'IN'/'OUT')
--   [Rule B-01] Business Exclusion: Cancelled orders (status = 'Cancelled')
--   [Rule B-02] Business Exclusion: Voided waste logs (voided_at IS NOT NULL)
--   [Rule B-03] Business Exclusion: Voided inventory logs (voided_at IS NOT NULL)
--   [Rule V-10] Join Cardinality / Row Duplication Check
--   [Rule V-11] High Rejection Rate (>1% threshold -> WARNING / MANUAL REVIEW)
-- =============================================================================

-- 3.1 ORDERS (May Completed at Cancelled)
INSERT INTO orders (id, status, updated_at) VALUES
('20000000-0000-0000-0000-000000000001', 'Completed', '2026-11-11 09:00:00+08'),
('20000000-0000-0000-0000-000000000002', 'Completed', '2026-11-11 10:30:00+08'),
('20000000-0000-0000-0000-000000000003', 'Completed', '2026-11-11 11:15:00+08'),
('20000000-0000-0000-0000-000000000004', 'Cancelled', '2026-11-11 14:00:00+08'), -- B-01: Cancelled order (EXCLUDED)
('20000000-0000-0000-0000-000000000005', 'Completed', '2026-11-11 16:30:00+08');

-- 3.2 ORDER ITEMS (Naglalaman ng iba't ibang depekto)
INSERT INTO order_items (id, order_id, quantity, total_price) VALUES
-- Normal Valid Record para may baseline:
('21000000-0000-0000-0000-000000000001', '20000000-0000-0000-0000-000000000001', 1, 850.00),

-- DEFECT 1: Rule C-04 -> Negative amount (total_price < 0)
('21000000-0000-0000-0000-000000000002', '20000000-0000-0000-0000-000000000001', 1, -450.00),

-- DEFECT 2: Rule C-05 -> Invalid quantity (quantity = 0)
('21000000-0000-0000-0000-000000000003', '20000000-0000-0000-0000-000000000002', 0, 350.00),

-- DEFECT 3: Rule C-05 -> Invalid negative quantity (quantity = -2)
('21000000-0000-0000-0000-000000000004', '20000000-0000-0000-0000-000000000002', -2, 700.00),

-- DEFECT 4: Rule C-03 -> Missing amount (total_price IS NULL)
('21000000-0000-0000-0000-000000000005', '20000000-0000-0000-0000-000000000003', 1, NULL),

-- DEFECT 5: Rule S-03 -> Invalid precision (sobra sa 2 decimals, e.g. 150.9999)
('21000000-0000-0000-0000-000000000006', '20000000-0000-0000-0000-000000000003', 1, 150.9999),

-- DEFECT 6: Rule B-01 -> Cancelled order (mapupunta sa quarantine bilang EXCLUDED)
('21000000-0000-0000-0000-000000000007', '20000000-0000-0000-0000-000000000004', 2, 950.00),

-- DEFECT 7: Rule C-02 -> Duplicate ID (unang kopya tatanggapin, pangalawa ire-reject sa C-02 / V-10)
('21000000-0000-0000-0000-000000000008', '20000000-0000-0000-0000-000000000005', 1, 650.00),
('21000000-0000-0000-0000-000000000008', '20000000-0000-0000-0000-000000000005', 1, 650.00); -- Duplicate ID!

-- 3.3 WASTE LOGS (May voided, whitespace, missing/negative amounts, at NULL ID)
INSERT INTO waste_logs (id, waste_type, item_name, quantity, unit, cost, logged_at, voided_at) VALUES
-- Normal Valid Record
('22000000-0000-0000-0000-000000000001', 'spoilage', 'Fresh Eggs', 4.0, 'pcs', 32.00, '2026-11-11 09:30:00+08', NULL),

-- DEFECT 8: Rule C-07 -> Leading/Trailing Whitespace (ititrim ng pipeline: "  Chocolate Muffin  ")
('22000000-0000-0000-0000-000000000002', 'product', '   Chocolate Muffin   ', 2.0, 'pcs', 120.00, '2026-11-11 11:00:00+08', NULL),

-- DEFECT 9: Rule B-02 -> Voided Waste Log (EXCLUDED sa quarantine)
('22000000-0000-0000-0000-000000000003', 'ingredient', 'Fresh Milk 1L', 1.0, 'pcs', 95.00, '2026-11-11 13:00:00+08', '2026-11-11 13:30:00+08'),

-- DEFECT 10: Rule C-04 -> Negative cost
('22000000-0000-0000-0000-000000000004', 'ingredient', 'Spoiled Batter', 1.0, 'kg', -150.00, '2026-11-11 15:30:00+08', NULL),

-- DEFECT 11: Rule C-03 -> Missing cost (cost IS NULL)
('22000000-0000-0000-0000-000000000005', 'material', 'Damaged Cake Box', 1.0, 'pcs', NULL, '2026-11-11 17:00:00+08', NULL),

-- DEFECT 12: Rule C-01 / Check 3 -> NULL Primary Key (id IS NULL)
(NULL, 'ingredient', 'Spoiled Egg Pack', 1.0, 'pcs', 75.00, '2026-11-11 18:00:00+08', NULL);

-- 3.4 INVENTORY LOGS (May casing issues, voided, negative cost, at duplicate ID)
INSERT INTO inventory_logs (id, item_type, item_name, transaction_type, cost, created_at, voided_at) VALUES
-- Normal Valid Record
('23000000-0000-0000-0000-000000000001', 'raw', 'All Purpose Flour 25kg', 'IN', 950.00, '2026-11-11 08:30:00+08', NULL),

-- DEFECT 13: Rule S-04 -> Lowercase transaction_type ('in' -> iconvert sa 'IN')
('23000000-0000-0000-0000-000000000002', 'raw', 'Refined White Sugar 25kg', 'in', 1400.00, '2026-11-11 09:00:00+08', NULL),

-- DEFECT 14: Rule S-04 -> Mixed case transaction_type ('Out' -> iconvert sa 'OUT')
('23000000-0000-0000-0000-000000000003', 'raw', 'Butter Usage Movement', 'Out', 0.00, '2026-11-11 10:00:00+08', NULL),

-- DEFECT 15: Rule C-07 -> Leading/Trailing Whitespace ("   Cocoa Powder 5kg   ")
('23000000-0000-0000-0000-000000000004', 'raw', '   Cocoa Powder 5kg   ', 'IN', 1250.00, '2026-11-11 11:30:00+08', NULL),

-- DEFECT 16: Rule B-03 -> Voided Inventory Log (EXCLUDED sa quarantine)
('23000000-0000-0000-0000-000000000005', 'raw', 'Expired Vanilla Extract', 'IN', 850.00, '2026-11-11 13:00:00+08', '2026-11-11 14:00:00+08'),

-- DEFECT 17: Rule C-04 -> Negative cost
('23000000-0000-0000-0000-000000000006', 'material', 'Cake Boxes 8x8 (Pack 100)', 'IN', -850.00, '2026-11-11 15:00:00+08', NULL),

-- DEFECT 18: Rule C-02 / Check 4 -> Duplicate ID in inventory
('23000000-0000-0000-0000-000000000007', 'raw', 'Yeast Pack', 'IN', 120.00, '2026-11-11 18:30:00+08', NULL),
('23000000-0000-0000-0000-000000000007', 'raw', 'Yeast Pack', 'IN', 120.00, '2026-11-11 18:30:00+08', NULL);

-- =============================================================================
-- 4. FAILED TESTING: BATCH 2026-11-12 (CRITICAL VALIDATION FAILURE)
-- Command: python -m pipeline.run_transform 2026-11-12
-- Expected result: REJECTED / FAILED because "Pending" is not a recognized
-- order status for the transformation output.
-- =============================================================================

INSERT INTO orders (id, status, updated_at) VALUES
('30000000-0000-0000-0000-000000000001', 'Pending', '2026-11-12 09:00:00+08');

INSERT INTO order_items (id, order_id, quantity, total_price) VALUES
('31000000-0000-0000-0000-000000000001',
 '30000000-0000-0000-0000-000000000001', 1, 500.00);


-- =============================================================================
-- 5. EXTRA VOLUME: 100 VALID ROWS PER TABLE ON EACH EXISTING BUSINESS DATE
-- Adds 900 rows (300 sales, 300 waste, 300 inventory) without adding dates.
-- These rows use unique IDs and valid values. Existing defect examples above stay intact.
-- =============================================================================

WITH business_dates(business_date) AS (
    VALUES ('2026-11-10'::date), ('2026-11-11'::date), ('2026-11-12'::date)
), inserted_orders AS (
    INSERT INTO orders (id, status, updated_at)
    SELECT gen_random_uuid(), 'Completed',
           business_date + TIME '08:00' + (n * INTERVAL '6 minutes')
    FROM business_dates
    CROSS JOIN generate_series(1, 100) AS n
    RETURNING id, updated_at
), numbered_orders AS (
    SELECT id, updated_at,
           row_number() OVER (PARTITION BY updated_at::date ORDER BY updated_at) AS item_no
    FROM inserted_orders
)
INSERT INTO order_items (id, order_id, quantity, total_price)
SELECT gen_random_uuid(), id,
       1 + (item_no % 4),
       (250 + ((item_no * 137) % 1250))::numeric(12,2)
FROM numbered_orders;

WITH business_dates(business_date) AS (
    VALUES ('2026-11-10'::date), ('2026-11-11'::date), ('2026-11-12'::date)
)
INSERT INTO waste_logs (id, waste_type, item_name, quantity, unit, cost, logged_at, voided_at)
SELECT gen_random_uuid(),
       CASE WHEN n % 3 = 0 THEN 'product' ELSE 'ingredient' END,
       CASE n % 5
           WHEN 0 THEN 'Fresh Milk'
           WHEN 1 THEN 'Cake Batter'
           WHEN 2 THEN 'Chocolate Filling'
           WHEN 3 THEN 'Bread Dough'
           ELSE 'Fresh Eggs'
       END,
       (1 + (n % 5) * 0.5)::numeric(10,2), 'pcs',
       (30 + ((n * 17) % 220))::numeric(12,2),
       business_date + TIME '09:00' + (n * INTERVAL '5 minutes'),
       NULL
FROM business_dates
CROSS JOIN generate_series(1, 100) AS n;

WITH business_dates(business_date) AS (
    VALUES ('2026-11-10'::date), ('2026-11-11'::date), ('2026-11-12'::date)
)
INSERT INTO inventory_logs (id, item_type, item_name, transaction_type, cost, created_at, voided_at)
SELECT gen_random_uuid(),
       CASE WHEN n % 4 = 0 THEN 'material' ELSE 'raw' END,
       CASE n % 5
           WHEN 0 THEN 'All Purpose Flour'
           WHEN 1 THEN 'Unsalted Butter'
           WHEN 2 THEN 'Fresh Eggs'
           WHEN 3 THEN 'Cake Boxes'
           ELSE 'Chocolate Chips'
       END,
       'IN',
       (150 + ((n * 43) % 2500))::numeric(12,2),
       business_date + TIME '08:30' + (n * INTERVAL '5 minutes'),
       NULL
FROM business_dates
CROSS JOIN generate_series(1, 100) AS n;
