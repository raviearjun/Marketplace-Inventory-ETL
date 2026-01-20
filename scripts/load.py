"""
Load Module: Load data into PostgreSQL Data Warehouse
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
import os


def get_db_connection(
    host: str = None,
    port: str = None,
    user: str = None,
    password: str = None,
    database: str = None
):
    """
    Create database connection using SQLAlchemy.
    
    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        user: Database user
        password: Database password
        database: Database name
        
    Returns:
        SQLAlchemy engine
    """
    # Get credentials from environment variables
    host = host or os.getenv('POSTGRES_HOST', 'localhost')
    port = port or os.getenv('POSTGRES_PORT', '5432')
    user = user or os.getenv('POSTGRES_USER', 'marketplace_user')
    password = password or os.getenv('POSTGRES_PASSWORD', 'marketplace_pass_2026')
    database = database or os.getenv('POSTGRES_DB', 'marketplace_dw')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    print(f"[LOAD] Connecting to database: {host}:{port}/{database}")
    
    engine = create_engine(connection_string)
    return engine


def load_stores(stores_df: pd.DataFrame, engine) -> int:
    """
    Load stores data into PostgreSQL with UPSERT logic.
    
    Args:
        stores_df: DataFrame with store data
        engine: SQLAlchemy engine
        
    Returns:
        Number of records loaded
    """
    print(f"[LOAD] Loading {len(stores_df)} stores into database...")
    
    # Load using pandas to_sql with if_exists='append' and handle duplicates
    try:
        # Use UPSERT logic: ON CONFLICT DO UPDATE
        stores_df.to_sql(
            'stores',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"[LOAD] Successfully loaded {len(stores_df)} stores")
        return len(stores_df)
        
    except Exception as e:
        print(f"[LOAD] Error loading stores: {e}")
        # Try alternative: truncate and reload
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE stores CASCADE"))
            conn.commit()
        
        stores_df.to_sql(
            'stores',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"[LOAD] Successfully loaded {len(stores_df)} stores (after truncate)")
        return len(stores_df)


def load_products(products_df: pd.DataFrame, engine) -> int:
    """
    Load products data into PostgreSQL with UPSERT logic.
    
    Args:
        products_df: DataFrame with product data
        engine: SQLAlchemy engine
        
    Returns:
        Number of records loaded
    """
    print(f"[LOAD] Loading {len(products_df)} products into database...")
    
    try:
        products_df.to_sql(
            'products',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"[LOAD] Successfully loaded {len(products_df)} products")
        return len(products_df)
        
    except Exception as e:
        print(f"[LOAD] Error loading products: {e}")
        # Try alternative: truncate and reload
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE products"))
            conn.commit()
        
        products_df.to_sql(
            'products',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"[LOAD] Successfully loaded {len(products_df)} products (after truncate)")
        return len(products_df)


def validate_data(engine) -> dict:
    """
    Validate data after loading.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        Dictionary with validation results
    """
    print("[LOAD] Validating data...")
    
    with engine.connect() as conn:
        # Count stores
        stores_count = conn.execute(text("SELECT COUNT(*) FROM stores")).scalar()
        
        # Count products
        products_count = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        
        # Check orphaned products (products without stores)
        orphaned = conn.execute(text("""
            SELECT COUNT(*) 
            FROM products p 
            LEFT JOIN stores s ON p.store_id = s.store_id 
            WHERE s.store_id IS NULL
        """)).scalar()
        
        results = {
            'stores_count': stores_count,
            'products_count': products_count,
            'orphaned_products': orphaned
        }
        
        print(f"[LOAD] Validation results:")
        print(f"  - Stores: {stores_count}")
        print(f"  - Products: {products_count}")
        print(f"  - Orphaned products: {orphaned}")
        
        return results


def load_data(stores_df: pd.DataFrame, products_df: pd.DataFrame) -> dict:
    """
    Main load function - loads both stores and products into database.
    
    Args:
        stores_df: DataFrame with store data
        products_df: DataFrame with product data
        
    Returns:
        Dictionary with load statistics
    """
    engine = get_db_connection()
    
    # Load stores first (parent table)
    stores_loaded = load_stores(stores_df, engine)
    
    # Load products (child table)
    products_loaded = load_products(products_df, engine)
    
    # Validate
    validation = validate_data(engine)
    
    stats = {
        'stores_loaded': stores_loaded,
        'products_loaded': products_loaded,
        'validation': validation
    }
    
    print(f"\n[LOAD] Load complete!")
    print(f"  - Stores loaded: {stores_loaded}")
    print(f"  - Products loaded: {products_loaded}")
    
    return stats


if __name__ == "__main__":
    # Test loading
    from extract import extract_data
    from transform import transform_data
    
    raw_df = extract_data(data_dir='../data', source_type='json')
    stores_df, products_df = transform_data(raw_df)
    
    stats = load_data(stores_df, products_df)
    print(f"\nLoad statistics: {stats}")
