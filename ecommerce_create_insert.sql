-- ===========================================================
-- E-COMMERCE CORE SCHEMA + SYNTHETIC DATA (PostgreSQL 12+)
-- Safety: drop existing types/tables (idempotent-ish for dev)
BEGIN;
DROP TABLE IF EXISTS order_line_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS shops CASCADE;
DROP TABLE IF EXISTS skus CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS users_addresses CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ===========================================================
-- ENUM TYPES (for robust domain constraints)
-- ===========================================================
CREATE TYPE user_status         AS ENUM ('active','banned','inactive');
CREATE TYPE shop_status         AS ENUM ('active','suspended','closed');
CREATE TYPE product_condition   AS ENUM ('new','used','refurbished');
CREATE TYPE order_status        AS ENUM ('pending','paid','shipped','delivered','cancelled','refunded');
CREATE TYPE payment_provider    AS ENUM ('card','paypal','bank_transfer','cod');
CREATE TYPE payment_status      AS ENUM ('pending','authorized','captured','failed','refunded','cancelled');

-- ===========================================================
-- TABLES
-- ===========================================================

-- USERS
CREATE TABLE users (
  id              BIGSERIAL PRIMARY KEY,
  email           TEXT UNIQUE NOT NULL,
  full_name       TEXT NOT NULL,
  phone           TEXT,
  status          user_status NOT NULL DEFAULT 'active',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_users_status ON users(status);

-- ADDRESSES
CREATE TABLE addresses (
  id              BIGSERIAL PRIMARY KEY,
  country         TEXT NOT NULL DEFAULT 'GR',
  region          TEXT,
  city            TEXT NOT NULL,
  postal_code     TEXT NOT NULL,
  address_line1   TEXT NOT NULL,
  address_line2   TEXT,
  latitude        NUMERIC(9,6),
  longitude       NUMERIC(9,6),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- USERS_ADDRESSES (many-to-many, with flags)
CREATE TABLE users_addresses (
  user_id               BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  address_id            BIGINT NOT NULL REFERENCES addresses(id) ON DELETE CASCADE,
  label                 TEXT,
  is_default_shipping   BOOLEAN NOT NULL DEFAULT false,
  is_default_billing    BOOLEAN NOT NULL DEFAULT false,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, address_id)
);
CREATE INDEX idx_users_addresses_user ON users_addresses(user_id);
CREATE INDEX idx_users_addresses_address ON users_addresses(address_id);

-- CATEGORIES (simple tree)
CREATE TABLE categories (
  id              BIGSERIAL PRIMARY KEY,
  parent_id       BIGINT REFERENCES categories(id) ON DELETE SET NULL,
  name            TEXT NOT NULL,
  slug            TEXT NOT NULL UNIQUE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_categories_parent ON categories(parent_id);

-- SKUS (catalog-level items)
CREATE TABLE skus (
  id              BIGSERIAL PRIMARY KEY,
  sku_code        TEXT NOT NULL UNIQUE,
  title           TEXT NOT NULL,
  brand           TEXT,
  category_id     BIGINT NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
  attributes      JSONB NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_skus_category ON skus(category_id);
CREATE INDEX idx_skus_title_gin ON skus USING GIN (to_tsvector('simple', title));

-- SHOPS (merchants/retailers)
CREATE TABLE shops (
  id              BIGSERIAL PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  slug            TEXT NOT NULL UNIQUE,
  url             TEXT,
  rating          NUMERIC(3,2) NOT NULL DEFAULT 4.50 CHECK (rating >= 0 AND rating <= 5),
  status          shop_status NOT NULL DEFAULT 'active',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- PRODUCTS (a shop’s unique offering for a SKU)
CREATE TABLE products (
  id              BIGSERIAL PRIMARY KEY,
  shop_id         BIGINT NOT NULL REFERENCES shops(id) ON DELETE RESTRICT,
  sku_id          BIGINT NOT NULL REFERENCES skus(id) ON DELETE RESTRICT,
  price           NUMERIC(12,2) NOT NULL CHECK (price >= 0),
  currency        CHAR(3) NOT NULL DEFAULT 'EUR',
  stock           INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
  condition       product_condition NOT NULL DEFAULT 'new',
  is_active       BOOLEAN NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (shop_id, sku_id)
);
CREATE INDEX idx_products_sku ON products(sku_id);
CREATE INDEX idx_products_shop ON products(shop_id);
CREATE INDEX idx_products_active ON products(is_active);

-- PAYMENTS (created separate, linked from orders.payment_id)
CREATE TABLE payments (
  id              BIGSERIAL PRIMARY KEY,
  provider        payment_provider NOT NULL,
  status          payment_status NOT NULL,
  amount          NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  currency        CHAR(3) NOT NULL DEFAULT 'EUR',
  external_ref    TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_payments_status ON payments(status);

-- ORDERS (references payment_id)
CREATE TABLE orders (
  id                    BIGSERIAL PRIMARY KEY,
  user_id               BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  shipping_address_id   BIGINT REFERENCES addresses(id) ON DELETE SET NULL,
  billing_address_id    BIGINT REFERENCES addresses(id) ON DELETE SET NULL,
  status                order_status NOT NULL DEFAULT 'pending',
  total_amount          NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
  currency              CHAR(3) NOT NULL DEFAULT 'EUR',
  payment_id            BIGINT REFERENCES payments(id) ON DELETE SET NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_payment ON orders(payment_id);

-- ORDER LINE ITEMS
CREATE TABLE order_line_items (
  id              BIGSERIAL PRIMARY KEY,
  order_id        BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id      BIGINT NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
  sku_id          BIGINT NOT NULL REFERENCES skus(id) ON DELETE RESTRICT,
  quantity        INTEGER NOT NULL CHECK (quantity > 0),
  unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  -- Use a generated column for integrity:
  line_total      NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);
CREATE INDEX idx_oli_order ON order_line_items(order_id);
CREATE INDEX idx_oli_product ON order_line_items(product_id);

-- ===========================================================
-- SAMPLE DATA (deterministic randomness)
-- ===========================================================
SELECT setseed(0.421);  -- reproducible random()

-- ---------- Categories (a small tree)
WITH roots AS (
  INSERT INTO categories (name, slug)
  VALUES
    ('Electronics', 'electronics'),
    ('Home & Living', 'home-living'),
    ('Sports & Outdoors', 'sports-outdoors'),
    ('Health & Beauty', 'health-beauty'),
    ('Toys & Hobbies', 'toys-hobbies')
  RETURNING id, slug
),
subs AS (
  INSERT INTO categories (name, slug, parent_id)
  SELECT x.name, x.slug, r.id
  FROM (VALUES
    ('Mobiles', 'mobiles', 'electronics'),
    ('Laptops', 'laptops', 'electronics'),
    ('Headphones', 'headphones', 'electronics'),
    ('Furniture', 'furniture', 'home-living'),
    ('Kitchen', 'kitchen', 'home-living'),
    ('Fitness', 'fitness', 'sports-outdoors'),
    ('Cycling', 'cycling', 'sports-outdoors'),
    ('Supplements', 'supplements', 'health-beauty'),
    ('Skincare', 'skincare', 'health-beauty'),
    ('Board Games', 'board-games', 'toys-hobbies'),
    ('RC Models', 'rc-models', 'toys-hobbies')
  ) AS x(name, slug, root_slug)
  JOIN roots r ON r.slug = x.root_slug
  RETURNING id
)
SELECT 1;

-- ---------- SKUs (~600)
INSERT INTO skus (sku_code, title, brand, category_id, attributes, created_at)
SELECT
  'SKU-' || gs::TEXT AS sku_code,
  CASE (random()*10)::INT
    WHEN 0 THEN 'Smartphone ' || gs
    WHEN 1 THEN 'Laptop ' || gs
    WHEN 2 THEN 'Wireless Headphones ' || gs
    WHEN 3 THEN 'Office Chair ' || gs
    WHEN 4 THEN 'Blender ' || gs
    WHEN 5 THEN 'Treadmill ' || gs
    WHEN 6 THEN 'Mountain Bike ' || gs
    WHEN 7 THEN 'Protein Powder ' || gs
    WHEN 8 THEN 'Face Serum ' || gs
    ELSE 'Board Game ' || gs
  END AS title,
  (ARRAY['BrandA','BrandB','BrandC','BrandD','BrandE'])[1 + (random()*4)::INT] AS brand,
  (SELECT id FROM categories ORDER BY random() LIMIT 1) AS category_id,
  jsonb_build_object(
    'color', (ARRAY['black','white','silver','red','blue','green'])[1 + (random()*5)::INT],
    'weight_kg', round( (0.2 + random()*25)::numeric, 2 ),
    'warranty_months', 6 + (random()*24)::INT
  ) AS attributes,
  now() - ((random()*365)::INT || ' days')::interval
FROM generate_series(1, 600) AS gs;

-- ---------- Shops (50)
INSERT INTO shops (name, slug, url, rating, status, created_at)
SELECT
  'Shop ' || gs,
  'shop-' || gs,
  'https://shop' || gs || '.example.com',
  round( (3.5 + random()*1.5)::numeric, 2 ),
  'active'::shop_status,      -- cast here
  now() - ((random()*800)::INT || ' days')::interval
FROM generate_series(1, 50) gs;

-- ---------- Products (≈5,000 offers; each (shop, sku) unique)
-- Create a temp pool of (shop, sku) pairs, sampled.
WITH sku_pool AS (
  SELECT id AS sku_id FROM skus
),
shop_pool AS (
  SELECT id AS shop_id FROM shops
),
pairs AS (
  SELECT sp.shop_id, sk.sku_id
  FROM shop_pool sp
  JOIN sku_pool sk ON random() < 0.20  -- ~20% of SKUs per shop
)
INSERT INTO products (shop_id, sku_id, price, currency, stock, condition, is_active, created_at)
SELECT
  p.shop_id,
  p.sku_id,
  round( (5 + random()*1500)::numeric, 2 ) AS price,
  'EUR',
  (random()*100)::INT,
  -- cast the array elements to product_condition
  (ARRAY['new'::product_condition,'used'::product_condition,'refurbished'::product_condition])
    [1 + (random()*2)::INT],
  true,
  now() - ((random()*365)::INT || ' days')::interval
FROM pairs p;

-- ---------- Users (1,200)
INSERT INTO users (email, full_name, phone, status, created_at, updated_at)
SELECT
  'user' || gs || '@example.com',
  'User ' || gs,
  '+30-210-' || lpad(((random()*9999)::INT)::TEXT, 4, '0'),
  'active'::user_status,      -- cast here
  now() - ((random()*900)::INT || ' days')::interval,
  now()
FROM generate_series(1, 1200) gs;

-- ---------- Addresses (2 per user => 2,400)
WITH ua AS (
  SELECT id AS user_id FROM users
)
INSERT INTO addresses (country, region, city, postal_code, address_line1, address_line2, latitude, longitude, created_at)
SELECT
  'GR',
  (ARRAY['Attica','Central Macedonia','Thessaly','Crete','Epirus'])[1 + (random()*4)::INT],
  (ARRAY['Athens','Thessaloniki','Patras','Heraklion','Larissa','Volos','Chania'])[1 + (random()*6)::INT],
  lpad(((10000 + (random()*89999)::INT))::TEXT, 5, '0'),
  'Street ' || (1 + (random()*300)::INT) || ' ' || (ARRAY['A','B','C','D','E'])[1+(random()*4)::INT],
  CASE WHEN random() < 0.2 THEN 'Apt ' || (1 + (random()*50)::INT) ELSE NULL END,
  round( (34 + random()*6)::numeric, 6),
  round( (19 + random()*8)::numeric, 6),
  now() - ((random()*800)::INT || ' days')::interval
FROM ua, generate_series(1,2) s;  -- two addresses per user

-- ---------- Link users to their two addresses & set defaults
-- We assume the most recently created two addresses belong to the current user from the above INSERT order.
WITH numbered AS (
  SELECT
    a.id AS address_id,
    u.id AS user_id,
    ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY a.id) AS rn
  FROM users u
  JOIN addresses a ON a.id BETWEEN ( (u.id - 1) * 2 + 1 ) AND ( (u.id - 1) * 2 + 2 )
)
INSERT INTO users_addresses (user_id, address_id, label, is_default_shipping, is_default_billing)
SELECT
  user_id,
  address_id,
  CASE rn WHEN 1 THEN 'Home' ELSE 'Office' END,
  (rn = 1),
  (rn = 1)
FROM numbered;

-- ---------- Orders (create 1,500 orders)
-- Choose a user and their default addresses when available.
WITH udef AS (
  SELECT ua.user_id,
         MAX(a.id) FILTER (WHERE ua.is_default_shipping) AS ship_addr_id,
         MAX(a.id) FILTER (WHERE ua.is_default_billing)  AS bill_addr_id
  FROM users_addresses ua
  JOIN addresses a ON a.id = ua.address_id
  GROUP BY ua.user_id
)

INSERT INTO orders (user_id, shipping_address_id, billing_address_id, status, total_amount, currency, payment_id, created_at, updated_at)
SELECT
  u.id,
  COALESCE(ud.ship_addr_id, a1.id),
  COALESCE(ud.bill_addr_id,  a2.id),
  'pending'::order_status,    -- cast here
  0,
  'EUR',
  NULL,
  now() - ((random()*120)::INT || ' days')::interval,
  now()
FROM users u
LEFT JOIN udef ud ON ud.user_id = u.id
LEFT JOIN LATERAL (SELECT id FROM addresses ORDER BY random() LIMIT 1) a1 ON TRUE
LEFT JOIN LATERAL (SELECT id FROM addresses ORDER BY random() LIMIT 1) a2 ON TRUE
ORDER BY random()
LIMIT 1500;

-- ---------- Order Line Items (1–5 per order)
-- Sample existing products; reuse product price; keep sku consistency.
WITH ord AS (
  SELECT id FROM orders
),
pick AS (
  SELECT
    o.id AS order_id,
    p.id AS product_id,
    p.sku_id,
    p.price AS unit_price,
    1 + (random()*3)::INT AS quantity
  FROM ord o
  JOIN LATERAL (
    SELECT id, sku_id, price
    FROM products
    WHERE is_active
    ORDER BY random()
    LIMIT (1 + (random()*4)::INT) -- 1..5 items per order
  ) p ON TRUE
)
INSERT INTO order_line_items (order_id, product_id, sku_id, quantity, unit_price)
SELECT order_id, product_id, sku_id, quantity, unit_price
FROM pick;

-- ---------- Update Orders.total_amount from line items
UPDATE orders o
SET total_amount = src.sum_total
FROM (
  SELECT order_id, COALESCE(SUM(line_total),0) AS sum_total
  FROM order_line_items
  GROUP BY order_id
) src
WHERE o.id = src.order_id;

-- ---------- Payments and link back to orders.payment_id
-- Most captured, some failed/refunded/cancelled.
WITH pay AS (
  INSERT INTO payments (provider, status, amount, currency, external_ref, created_at)
  SELECT
    (ARRAY['card','paypal','bank_transfer','cod'])[1 + (random()*3)::INT]::payment_provider,
    CASE
      WHEN random() < 0.80 THEN 'captured'::payment_status
      WHEN random() < 0.90 THEN 'failed'::payment_status
      WHEN random() < 0.97 THEN 'refunded'::payment_status
      ELSE 'authorized'::payment_status
    END,
    o.total_amount,
    'EUR',
    'TX-' || o.id,
    o.created_at + ( (random()*3600)::INT || ' seconds')::interval
  FROM orders o
  RETURNING id, status, amount, created_at, external_ref
)
UPDATE orders o
SET payment_id = p.id,
    status = CASE
               WHEN p.status = 'captured'   THEN 'paid'
               WHEN p.status = 'authorized' THEN 'pending'
               WHEN p.status IN ('failed','cancelled') THEN 'cancelled'
               WHEN p.status = 'refunded'   THEN 'refunded'
               ELSE o.status
             END,
    updated_at = GREATEST(o.updated_at, p.created_at)
FROM pay p
WHERE p.external_ref = 'TX-' || o.id;

-- ===========================================================
-- Helpful Indexes for typical access paths
-- ===========================================================
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_oli_order_product ON order_line_items(order_id, product_id);

COMMIT;

-- ===========================================================
-- EXAMPLE QUERIES (optional; comment out in prod)
-- ===========================================================
-- -- Top 5 categories by GMV:
-- SELECT c.name, SUM(oli.line_total) AS gmv
-- FROM order_line_items oli
-- JOIN skus s ON s.id = oli.sku_id
-- JOIN categories c ON c.id = s.category_id
-- GROUP BY c.name
-- ORDER BY gmv DESC
-- LIMIT 5;

-- -- Orders with payment + items:
-- SELECT o.id, o.status, p.status AS payment_status, o.total_amount, COUNT(oli.id) AS items
-- FROM orders o
-- LEFT JOIN payments p ON p.id = o.payment_id
-- LEFT JOIN order_line_items oli ON oli.order_id = o.id
-- GROUP BY o.id, p.status
-- ORDER BY o.id
-- LIMIT 20;