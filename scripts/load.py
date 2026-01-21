"""
Load Module: Load data into PostgreSQL Data Warehouse using TRUE UPSERT

This module implements production-grade UPSERT (INSERT or UPDATE) logic:
- New records are inserted
- Existing records are updated
- No TRUNCATE operations (safe for production)
- Idempotent (re-runnable)
- Transaction-safe (atomic operations)

Implementation uses PostgreSQL's ON CONFLICT DO UPDATE feature with
staging table approach for optimal performance.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError
from datetime import datetime
from typing import Optional, Dict
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


def load_stores(stores_df: pd.DataFrame, engine) -> Dict:
    """
    Load stores data into PostgreSQL using TRUE UPSERT logic.
    
    Uses staging table approach:
    1. Load data to temporary staging table
    2. MERGE staging -> stores using INSERT ... ON CONFLICT DO UPDATE
    3. Updates existing records, inserts new ones
    4. Sets updated_at = NOW() on UPDATE operations
    
    Args:
        stores_df: DataFrame with store data
        engine: SQLAlchemy engine
        
    Returns:
        dict: {
            'total': int,       # Total records processed
            'inserted': int,    # New records inserted
            'updated': int,     # Existing records updated
            'execution_time_seconds': float
        }
        
    Raises:
        IntegrityError: On constraint violation
        OperationalError: On database connection issues
        
    Note:
        This function is idempotent - can be run multiple times safely.
    """
    print(f"[LOAD] Starting UPSERT for {len(stores_df)} stores...")
    start_time = datetime.now()
    staging_table = '_staging_stores'
    
    try:
        # Step 1: Load to staging table
        print(f"[LOAD] Creating staging table: {staging_table}")
        stores_df.to_sql(
            staging_table,
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Step 2: Count before UPSERT (for statistics)
        with engine.connect() as conn:
            count_before = conn.execute(text("SELECT COUNT(*) FROM stores")).scalar()
        
        # Step 3: Execute UPSERT with transaction
        print(f"[LOAD] Executing UPSERT (INSERT ... ON CONFLICT DO UPDATE)...")
        with engine.begin() as conn:
            # Build dynamic UPDATE SET clause (exclude PK and created_at)
            update_cols = [col for col in stores_df.columns 
                          if col not in ['store_id', 'created_at']]
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            set_clause += ", updated_at = NOW()"
            
            # Define timestamp columns that need casting
            timestamp_cols = ['store_last_active', 'store_created_at']
            
            # Build SELECT with explicit casting for timestamp columns
            select_cols = []
            for col in stores_df.columns:
                if col in timestamp_cols:
                    select_cols.append(f"CAST({col} AS TIMESTAMP) AS {col}")
                else:
                    select_cols.append(col)
            select_clause = ', '.join(select_cols)
            
            # Execute UPSERT with explicit column casting
            upsert_sql = f"""
                INSERT INTO stores 
                SELECT {select_clause} FROM {staging_table}
                ON CONFLICT (store_id) 
                DO UPDATE SET {set_clause}
            """
            conn.execute(text(upsert_sql))
            
            # Cleanup staging table
            conn.execute(text(f"DROP TABLE {staging_table}"))
        
        # Step 4: Count after and calculate statistics
        with engine.connect() as conn:
            count_after = conn.execute(text("SELECT COUNT(*) FROM stores")).scalar()
        
        inserted = max(0, count_after - count_before)
        updated = len(stores_df) - inserted
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print(f"[LOAD] UPSERT complete: {inserted} inserted, {updated} updated ({elapsed:.2f}s)")
        
        return {
            'total': len(stores_df),
            'inserted': inserted,
            'updated': updated,
            'execution_time_seconds': elapsed
        }
        
    except IntegrityError as e:
        print(f"[LOAD] Integrity constraint violation: {e}")
        raise
    except OperationalError as e:
        print(f"[LOAD] Database operational error: {e}")
        raise
    except Exception as e:
        print(f"[LOAD] Unexpected error during UPSERT: {e}")
        raise


def load_products(products_df: pd.DataFrame, engine) -> Dict:
    """
    Load products data into PostgreSQL using TRUE UPSERT logic.
    
    Uses staging table approach:
    1. Load data to temporary staging table
    2. MERGE staging -> products using INSERT ... ON CONFLICT DO UPDATE
    3. Updates existing records, inserts new ones
    4. Sets updated_at = NOW() on UPDATE operations
    
    Args:
        products_df: DataFrame with product data
        engine: SQLAlchemy engine
        
    Returns:
        dict: {
            'total': int,       # Total records processed
            'inserted': int,    # New records inserted
            'updated': int,     # Existing records updated
            'execution_time_seconds': float
        }
        
    Raises:
        IntegrityError: On constraint violation (e.g., FK violation)
        OperationalError: On database connection issues
        
    Note:
        This function is idempotent - can be run multiple times safely.
        Products must have valid store_id (FK constraint enforced).
    """
    print(f"[LOAD] Starting UPSERT for {len(products_df)} products...")
    start_time = datetime.now()
    staging_table = '_staging_products'
    
    try:
        # Step 1: Load to staging table
        print(f"[LOAD] Creating staging table: {staging_table}")
        products_df.to_sql(
            staging_table,
            engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Step 2: Count before UPSERT (for statistics)
        with engine.connect() as conn:
            count_before = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        
        # Step 3: Execute UPSERT with transaction
        print(f"[LOAD] Executing UPSERT (INSERT ... ON CONFLICT DO UPDATE)...")
        with engine.begin() as conn:
            # Build dynamic UPDATE SET clause (exclude PK and created_at)
            update_cols = [col for col in products_df.columns 
                          if col not in ['id', 'created_at']]
            set_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            set_clause += ", updated_at = NOW()"
            
            # Define timestamp columns that need casting
            timestamp_cols = ['product_created_at', 'product_updated_at', 'crawled_at', 'created_at']
            
            # Build SELECT with explicit casting for timestamp columns
            select_cols = []
            for col in products_df.columns:
                if col in timestamp_cols:
                    select_cols.append(f"CAST({col} AS TIMESTAMP) AS {col}")
                else:
                    select_cols.append(col)
            select_clause = ', '.join(select_cols)
            
            # Execute UPSERT with explicit column casting
            upsert_sql = f"""
                INSERT INTO products 
                SELECT {select_clause} FROM {staging_table}
                ON CONFLICT (id) 
                DO UPDATE SET {set_clause}
            """
            conn.execute(text(upsert_sql))
            
            # Cleanup staging table
            conn.execute(text(f"DROP TABLE {staging_table}"))
        
        # Step 4: Count after and calculate statistics
        with engine.connect() as conn:
            count_after = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        
        inserted = max(0, count_after - count_before)
        updated = len(products_df) - inserted
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print(f"[LOAD] UPSERT complete: {inserted} inserted, {updated} updated ({elapsed:.2f}s)")
        
        return {
            'total': len(products_df),
            'inserted': inserted,
            'updated': updated,
            'execution_time_seconds': elapsed
        }
        
    except IntegrityError as e:
        print(f"[LOAD] Integrity constraint violation: {e}")
        raise
    except OperationalError as e:
        print(f"[LOAD] Database operational error: {e}")
        raise
    except Exception as e:
        print(f"[LOAD] Unexpected error during UPSERT: {e}")
        raise


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
    Main load function - loads both stores and products using TRUE UPSERT.
    
    Loads stores first (parent table), then products (child table with FK).
    Both tables use staging table approach with ON CONFLICT DO UPDATE.
    
    Args:
        stores_df: DataFrame with store data
        products_df: DataFrame with product data
        
    Returns:
        dict: {
            'stores': {'total': int, 'inserted': int, 'updated': int, ...},
            'products': {'total': int, 'inserted': int, 'updated': int, ...},
            'validation': {...}
        }
    """
    engine = get_db_connection()
    
    # Load stores first (parent table)
    stores_result = load_stores(stores_df, engine)
    
    # Load products (child table with FK to stores)
    products_result = load_products(products_df, engine)
    
    # Validate data integrity
    validation = validate_data(engine)
    
    stats = {
        'stores': stores_result,
        'products': products_result,
        'validation': validation
    }
    
    print(f"\n[LOAD] UPSERT Complete!")
    print(f"  - Stores: {stores_result['inserted']} inserted, {stores_result['updated']} updated")
    print(f"  - Products: {products_result['inserted']} inserted, {products_result['updated']} updated")
    
    return stats


if __name__ == "__main__":
    # Test loading
    from extract import extract_data
    from transform import transform_data
    
    raw_df = extract_data(data_dir='../data', source_type='json')
    stores_df, products_df = transform_data(raw_df)
    
    stats = load_data(stores_df, products_df)
    print(f"\nLoad statistics: {stats}")
