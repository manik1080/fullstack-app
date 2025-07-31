import pandas as pd
import os

# List of CSV files in the archive directory
csv_files = [
    'archive/users.csv',
    'archive/products.csv', 
    'archive/orders.csv',
    'archive/order_items.csv',
    'archive/inventory_items.csv',
    'archive/distribution_centers.csv'
]

print("Analyzing data types in CSV files...")
print("=" * 50)

for file_path in csv_files:
    if os.path.exists(file_path):
        try:
            # Read first few rows to infer data types
            df = pd.read_csv(file_path, nrows=1000)
            print(f"\n{file_path}:")
            print(f"Columns: {list(df.columns)}")
            print(f"Data types:")
            for col, dtype in df.dtypes.items():
                print(f"  {col}: {dtype}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    else:
        print(f"File not found: {file_path}") 