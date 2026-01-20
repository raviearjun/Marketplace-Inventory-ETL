#!/usr/bin/env python3
"""Test all required imports for ETL pipeline"""

def test_imports():
    results = []
    
    # Core ETL packages
    packages = [
        ('pandas', 'Data manipulation'),
        ('sqlalchemy', 'Database ORM'),
        ('psycopg2', 'PostgreSQL driver'),
        ('openpyxl', 'Excel export (optional)'),
    ]
    
    for package, description in packages:
        try:
            mod = __import__(package)
            version = getattr(mod, '__version__', 'OK')
            results.append(f"✅ {package:15} {version:15} - {description}")
        except ImportError:
            results.append(f"❌ {package:15} {'MISSING':15} - {description}")
    
    print("\n" + "="*70)
    print("ETL PIPELINE - DEPENDENCY CHECK")
    print("="*70)
    for result in results:
        print(result)
    print("="*70 + "\n")

if __name__ == "__main__":
    test_imports()
