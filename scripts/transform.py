"""
Transform Module: Data cleaning and transformation
"""

import pandas as pd
import re
from typing import Tuple


def clean_text(text: str) -> str:
    """
    Clean text by removing newlines, extra spaces, and special characters.
    
    Args:
        text: Input text string
        
    Returns:
        Cleaned text string
    """
    if pd.isna(text) or text is None:
        return ""
    
    # Convert to string
    text = str(text)
    
    # Remove newline characters
    text = text.replace('\\n', ' ').replace('\n', ' ')
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing spaces
    text = text.strip()
    
    return text


def transform_stores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean store data.
    
    Args:
        df: Raw DataFrame with store columns
        
    Returns:
        Cleaned DataFrame with unique stores
    """
    print("[TRANSFORM] Processing stores data...")
    
    # Select store columns
    store_cols = [
        'store_id', 'store_url', 'store_name', 'store_description',
        'store_active_product_count', 'store_location_province', 
        'store_location_city', 'store_location_latitude', 
        'store_location_longitude', 'store_terms_condition',
        'store_image_avatar', 'store_image_header', 
        'store_last_active', 'store_store_creted_at', 'store_count'
    ]
    
    stores_df = df[store_cols].copy()
    
    # Rename columns (fix typo: creted -> created)
    stores_df.rename(columns={'store_store_creted_at': 'store_created_at'}, inplace=True)
    
    # Deduplication: keep first occurrence of each store_id
    stores_df = stores_df.drop_duplicates(subset=['store_id'], keep='first')
    
    # Handle nulls
    stores_df['store_name'] = stores_df['store_name'].fillna('UNKNOWN')
    stores_df['store_location_city'] = stores_df['store_location_city'].fillna('UNKNOWN')
    stores_df['store_location_province'] = stores_df['store_location_province'].fillna('UNKNOWN')
    stores_df['store_active_product_count'] = stores_df['store_active_product_count'].fillna(0).astype(int)
    stores_df['store_count'] = stores_df['store_count'].fillna(0).astype(int)
    
    # Text cleaning
    stores_df['store_name'] = stores_df['store_name'].apply(clean_text).str.upper()
    stores_df['store_description'] = stores_df['store_description'].apply(clean_text)
    stores_df['store_terms_condition'] = stores_df['store_terms_condition'].apply(clean_text)
    
    # Convert timestamps
    timestamp_cols = ['store_last_active', 'store_created_at']
    for col in timestamp_cols:
        stores_df[col] = pd.to_datetime(stores_df[col], errors='coerce')
    
    print(f"[TRANSFORM] Stores: {len(stores_df)} unique stores after deduplication")
    
    return stores_df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean product data.
    
    Args:
        df: Raw DataFrame with product columns
        
    Returns:
        Cleaned DataFrame with products
    """
    print("[TRANSFORM] Processing products data...")
    
    # Select product columns
    product_cols = [
        'id', 'store_id', 'name', 'description', 'category', 'main_category',
        'original_price', 'discounted_price', 'discount_percentage',
        'url', 'image_large', 'image_small', 'rating', 'stocks',
        'sold_count', 'view_count', 'favorited_count', 'total_review',
        'item_condition', 'source', 'product_created_at', 
        'product_updated_at', 'crawled', 'created_at'
    ]
    
    products_df = df[product_cols].copy()
    
    # Rename columns
    products_df.rename(columns={'crawled': 'crawled_at'}, inplace=True)
    
    # Deduplication: remove duplicate product IDs
    products_df = products_df.drop_duplicates(subset=['id'], keep='first')
    
    # Handle nulls
    products_df['name'] = products_df['name'].fillna('UNKNOWN')
    products_df['category'] = products_df['category'].fillna('UNCATEGORIZED')
    products_df['main_category'] = products_df['main_category'].fillna('UNCATEGORIZED')
    products_df['url'] = products_df['url'].fillna('')
    products_df['rating'] = products_df['rating'].fillna(0.0)
    products_df['stocks'] = products_df['stocks'].fillna(0).astype(int)
    products_df['sold_count'] = products_df['sold_count'].fillna(0).astype(int)
    products_df['view_count'] = products_df['view_count'].fillna(0).astype(int)
    products_df['total_review'] = products_df['total_review'].fillna(0).astype(int)
    products_df['discount_percentage'] = products_df['discount_percentage'].fillna(0).astype(int)
    
    # Text cleaning
    products_df['name'] = products_df['name'].apply(clean_text)
    products_df['description'] = products_df['description'].apply(clean_text)
    
    # Standardization: Uppercase for categories
    products_df['category'] = products_df['category'].str.upper()
    products_df['main_category'] = products_df['main_category'].str.upper()
    products_df['item_condition'] = products_df['item_condition'].str.upper()
    
    # Data type casting
    products_df['original_price'] = pd.to_numeric(products_df['original_price'], errors='coerce').fillna(0).astype(int)
    products_df['discounted_price'] = pd.to_numeric(products_df['discounted_price'], errors='coerce').fillna(0).astype(int)
    
    # Convert timestamps
    timestamp_cols = ['product_created_at', 'product_updated_at', 'crawled_at', 'created_at']
    for col in timestamp_cols:
        products_df[col] = pd.to_datetime(products_df[col], errors='coerce')
    
    print(f"[TRANSFORM] Products: {len(products_df)} products after cleaning")
    
    return products_df


def transform_data(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main transformation function - splits and cleans data into stores and products.
    
    Args:
        raw_df: Raw DataFrame from extraction
        
    Returns:
        Tuple of (stores_df, products_df)
    """
    print(f"[TRANSFORM] Starting transformation of {len(raw_df)} raw records")
    
    stores_df = transform_stores(raw_df)
    products_df = transform_products(raw_df)
    
    print(f"[TRANSFORM] Transformation complete: {len(stores_df)} stores, {len(products_df)} products")
    
    return stores_df, products_df


if __name__ == "__main__":
    # Test transformation
    from extract import extract_data
    
    raw_df = extract_data(data_dir='../data', source_type='json')
    stores_df, products_df = transform_data(raw_df)
    
    print(f"\nStores shape: {stores_df.shape}")
    print(f"Products shape: {products_df.shape}")
