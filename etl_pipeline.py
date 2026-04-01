"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    

    print ("stage 1: extract Data")
    tables = ["customers", "products", "orders", "order_items"] 
    data_dict = {}
    for table in tables:
        query = f"SELECT * FROM {table}"
        data_dict[table] = pd.read_sql(query, engine) 
        print(f"Extracted {len(data_dict[table])} rows from {table} table.")   
    return data_dict      


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    print("\n--- Stage 2: Transforming Data ---")
    
    
    customers = data_dict['customers']
    products = data_dict['products']
    orders = data_dict['orders']
    items = data_dict['order_items']

    merged_df = items.merge(products, on='product_id')
    merged_df = merged_df.merge(orders, on='order_id')
    merged_df = merged_df.merge(customers, on='customer_id')



    merged_df['line_total'] = merged_df['quantity'] * merged_df['unit_price']

   

    merged_df = merged_df[merged_df['status'] != 'cancelled']
    merged_df = merged_df[merged_df['quantity'] <= 100]


    customer_summary = merged_df.groupby(['customer_id', 'customer_name']).agg(total_orders=('order_id', 'nunique'),total_revenue=('line_total', 'sum')).reset_index()

    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']

    
    cat_rev = merged_df.groupby(['customer_id', 'category'])['line_total'].sum().reset_index()
    top_cats = cat_rev.sort_values('line_total', ascending=False).drop_duplicates('customer_id')
    
    
    customer_summary = customer_summary.merge(top_cats[['customer_id', 'category']], on='customer_id')
    customer_summary = customer_summary.rename(columns={'category': 'top_category'})

    print(f"Done! Created summary for {len(customer_summary)} customers.")
    return customer_summary




def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    
    print("\n--- Stage 3: Validating Data ---")
    
    results = {}

    results['no_nulls'] = df['customer_id'].notnull().all() and df['customer_name'].notnull().all()
    
    results['revenue_positive'] = (df['total_revenue'] > 0).all()
    
    results['no_duplicates'] = not df['customer_id'].duplicated().any()
    
    results['orders_positive'] = (df['total_orders'] > 0).all()

    for check, status in results.items():
        state = "PASS" if status else "FAIL"
        print(f"Check [{check}]: {state}")

    if not all(results.values()):
        raise ValueError("Data Validation Failed! Critical errors found in the report.")

    print("All validation checks passed successfully.")
    return results


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    print("\n--- Stage 4: Loading Data ---")

    df.to_sql('customer_analytics', engine, if_exists='replace', index=False)
    print("Successfully loaded data into 'customer_analytics' table.")

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    df.to_csv(csv_path, index=False)
    print(f"Successfully saved data to: {csv_path}")

    print(f"Total rows loaded: {len(df)}")
    
    print("\n>>> ETL Pipeline Completed Successfully! <<<")
   

def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""


    DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(DB_URL)

    print("Starting ETL Pipeline...")

    
    raw_data = extract(engine)
    
    transformed_data = transform(raw_data)
    validate(transformed_data)
    load(transformed_data, engine, "output/customer_analytics.csv")
    



if __name__ == "__main__":
   
    
    main()
