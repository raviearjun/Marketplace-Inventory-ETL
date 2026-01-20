-- =====================================================
-- Marketplace Data Warehouse - DDL Schema
-- Technical Test: Data Engineer
-- =====================================================

-- Drop tables if exist (for development)
DROP TABLE IF EXISTS products CASCADE;

DROP TABLE IF EXISTS stores CASCADE;

-- =====================================================
-- TABLE: stores
-- Description: Denormalized store information
-- =====================================================
CREATE TABLE stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    store_url TEXT,
    store_description TEXT,
    store_active_product_count INTEGER DEFAULT 0,
    store_location_province VARCHAR(100),
    store_location_city VARCHAR(100),
    store_location_latitude DECIMAL(10, 8),
    store_location_longitude DECIMAL(11, 8),
    store_terms_condition TEXT,
    store_image_avatar TEXT,
    store_image_header TEXT,
    store_last_active TIMESTAMP,
    store_created_at TIMESTAMP, -- Fixed typo from source: "store_creted_at"
    store_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TABLE: products
-- Description: Product catalog with FK to stores
-- =====================================================
CREATE TABLE products (
    id VARCHAR(50) PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL,
    name VARCHAR(500) NOT NULL,
    description TEXT,
    category VARCHAR(200),
    main_category VARCHAR(200),
    original_price BIGINT,
    discounted_price BIGINT,
    discount_percentage INTEGER DEFAULT 0,
    url TEXT,
    image_large TEXT,
    image_small TEXT,
    rating DECIMAL(3, 2) DEFAULT 0.00,
    stocks INTEGER DEFAULT 0,
    sold_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    favorited_count INTEGER,
    total_review INTEGER DEFAULT 0,
    item_condition VARCHAR(50),
    source VARCHAR(50),
    product_created_at TIMESTAMP,
    product_updated_at TIMESTAMP,
    crawled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

-- Foreign Key Constraint
CONSTRAINT fk_store
        FOREIGN KEY (store_id)
        REFERENCES stores(store_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- =====================================================
-- INDEXES for Performance Optimization
-- =====================================================

-- Products indexes
CREATE INDEX idx_products_store_id ON products (store_id);

CREATE INDEX idx_products_category ON products (category);

CREATE INDEX idx_products_main_category ON products (main_category);

CREATE INDEX idx_products_price ON products (discounted_price);

CREATE INDEX idx_products_rating ON products (rating);

CREATE INDEX idx_products_stocks ON products (stocks);

-- Stores indexes
CREATE INDEX idx_stores_city ON stores (store_location_city);

CREATE INDEX idx_stores_province ON stores (store_location_province);

CREATE INDEX idx_stores_name ON stores (store_name);

-- Composite indexes for common queries
CREATE INDEX idx_products_category_price ON products (category, discounted_price);

CREATE INDEX idx_products_store_category ON products (store_id, category);

-- =====================================================
-- COMMENTS for Documentation
-- =====================================================
COMMENT ON
TABLE stores IS 'Normalized store/merchant information from marketplace';

COMMENT ON
TABLE products IS 'Product catalog with foreign key relationship to stores';

COMMENT ON COLUMN stores.store_id IS 'Unique identifier for store';

COMMENT ON COLUMN products.id IS 'Unique identifier for product (product_id from source)';

COMMENT ON COLUMN products.store_id IS 'Foreign key reference to stores table';

-- =====================================================
-- INITIAL DATA VALIDATION
-- =====================================================
-- After data load, run these queries to validate:
-- SELECT COUNT(*) FROM stores;
-- SELECT COUNT(*) FROM products;
-- SELECT COUNT(*) FROM products p LEFT JOIN stores s ON p.store_id = s.store_id WHERE s.store_id IS NULL;