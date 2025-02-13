import pandas as pd
import numpy as np

def generate_ecommerce_data(num_records=1_000_000, output_path='data/ecommerce_sales.csv'):
    """
    Generates a synthetic e-commerce dataset and saves it to a CSV file.

    :param num_records: Number of rows in the dataset.
    :param output_path: File path where the CSV will be saved.
    """
    # Create a DataFrame with random e-commerce data
    df = pd.DataFrame({
        'order_id': np.arange(num_records),
        'customer_id': np.random.randint(1000, 5000, size=num_records),
        'order_date': pd.to_datetime('2020-01-01') + pd.to_timedelta(
            np.random.randint(0, 365, size=num_records), unit='d'
        ),
        'product_id': np.random.randint(1, 500, size=num_records),
        'quantity': np.random.randint(1, 10, size=num_records),
        'unit_price': np.random.uniform(10, 500, size=num_records),
        'region': np.random.choice(['North', 'South', 'East', 'West'], size=num_records),
    })

    # Create a 'data' folder if it doesn't exist
    import os
    os.makedirs('data', exist_ok=True)

    # Save the DataFrame to CSV without an index
    df.to_csv(output_path, index=False)
    print(f"Data generated and saved to {output_path}")

if __name__ == "__main__":
    # Feel free to change the number of records if you want bigger/smaller data
    generate_ecommerce_data(num_records=1_000_000)
