"""
Extract Module: Read data from JSON/CSV files
"""

import json
import pandas as pd
from typing import Dict, List
import os


def extract_from_json(file_path: str) -> pd.DataFrame:
    """
    Extract data from JSON file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        pandas DataFrame with extracted data
    """
    print(f"[EXTRACT] Reading JSON file: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # JSON structure: {"data": [...]}
    if isinstance(data, dict) and 'data' in data:
        df = pd.DataFrame(data['data'])
    else:
        df = pd.DataFrame(data)
    
    print(f"[EXTRACT] Loaded {len(df)} records from JSON")
    return df


def extract_from_csv(file_path: str) -> pd.DataFrame:
    """
    Extract data from CSV file.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        pandas DataFrame with extracted data
    """
    print(f"[EXTRACT] Reading CSV file: {file_path}")
    
    # CSV uses semicolon as delimiter
    df = pd.read_csv(file_path, delimiter=';', encoding='utf-8', low_memory=False)
    
    print(f"[EXTRACT] Loaded {len(df)} records from CSV")
    return df


def extract_data(data_dir: str = '/opt/airflow/data', source_type: str = 'json') -> pd.DataFrame:
    """
    Main extraction function - reads from JSON or CSV.
    
    Args:
        data_dir: Directory containing data files
        source_type: 'json' or 'csv'
        
    Returns:
        pandas DataFrame with raw data
    """
    if source_type == 'json':
        file_path = os.path.join(data_dir, 'Technical Test - Data Marketplace Stores and Product.json')
        return extract_from_json(file_path)
    elif source_type == 'csv':
        file_path = os.path.join(data_dir, 'Technical Test - Data Marketplace Stores and Product.csv')
        return extract_from_csv(file_path)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")


if __name__ == "__main__":
    # Test extraction
    df = extract_data(data_dir='../data', source_type='json')
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
