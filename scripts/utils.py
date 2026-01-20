"""
Utility Functions for ETL Pipeline
"""

import logging
from datetime import datetime
from typing import Any, Dict


def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger('marketplace_etl')
    return logger


def get_execution_stats(start_time: datetime, end_time: datetime, **kwargs) -> Dict[str, Any]:
    """
    Calculate execution statistics.
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        **kwargs: Additional statistics
        
    Returns:
        Dictionary with execution stats
    """
    duration = (end_time - start_time).total_seconds()
    
    stats = {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': duration,
        'duration_minutes': round(duration / 60, 2),
        'status': 'success',
        **kwargs
    }
    
    return stats


def format_number(num: int) -> str:
    """
    Format number with thousand separators.
    
    Args:
        num: Number to format
        
    Returns:
        Formatted string
    """
    return f"{num:,}"


if __name__ == "__main__":
    # Test utilities
    logger = setup_logging()
    logger.info("Logging setup complete")
    
    start = datetime.now()
    # Simulate work
    import time
    time.sleep(2)
    end = datetime.now()
    
    stats = get_execution_stats(start, end, records_processed=10000)
    print(stats)
