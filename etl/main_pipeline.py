import os
import pandas as pd
import hashlib
import logging
from time import time
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from dotenv import load_dotenv

# --- 1. SETUP AND CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def get_db_engine(db_type, user, password, host, port, db_name):
    """Creates and returns a SQLAlchemy engine."""
    try:
        if db_type == 'postgres':
            url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            return create_engine(url)
    except Exception as e:
        logging.error(f"Error creating engine for {db_type}: {e}")
        raise

def get_mongo_client(user, password, host, port):
    """Creates and returns a MongoClient."""
    try:
        url = f"mongodb://{user}:{password}@{host}:{port}/"
        return MongoClient(url)
    except Exception as e:
        logging.error(f"Error creating mongo client: {e}")
        raise

# --- 2. HELPER FUNCTIONS ---
def hash_pii(data):
    """Hashes a string using SHA-256 for PII protection."""
    return hashlib.sha256(str(data).encode()).hexdigest()

# --- 3. ETL STAGES ---
def extract(source_engine, mongo_client):
    """Extracts data from all source systems."""
    logging.info("Starting EXTRACTION phase...")
    try:
        customers_df = pd.read_sql("SELECT * FROM customers", source_engine)
        transactions_df = pd.read_sql("SELECT * FROM transactions", source_engine)
        logging.info(f"Extracted {len(customers_df)} customers and {len(transactions_df)} transactions from PostgreSQL.")

        # Seed and extract from MongoDB
        merchant_db = mongo_client[os.getenv('MONGO_DB_NAME')]
        merchant_collection = merchant_db[os.getenv('MONGO_COLLECTION')]
        if merchant_collection.count_documents({}) == 0:
            logging.info("Seeding merchant data into MongoDB...")
            merchant_data = [
                {'merchant_name': 'GreenLeaf Grocers', 'category': 'Groceries'},
                {'merchant_name': 'The Daily Grind Coffee', 'category': 'Food & Beverage'},
                {'merchant_name': 'TechSphere Electronics', 'category': 'Electronics'},
            ]
            merchant_collection.insert_many(merchant_data)
        
        merchants_list = list(merchant_collection.find({}, {'_id': 0}))
        merchants_df = pd.DataFrame(merchants_list)
        logging.info(f"Extracted {len(merchants_df)} merchants from MongoDB.")

        return customers_df, transactions_df, merchants_df
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def transform(customers_df, transactions_df, merchants_df):
    """Transforms extracted data, applies business logic and data quality checks."""
    logging.info("Starting TRANSFORM phase...")
    
    # Data Quality: Validate transaction amounts
    original_rows = len(transactions_df)
    transactions_df = transactions_df[transactions_df['amount'] > 0].copy()
    logging.info(f"Data Quality: Removed {original_rows - len(transactions_df)} transactions with non-positive amounts.")
    
    # PII Governance: Hash sensitive customer data
    customers_df['last_name_hash'] = customers_df['last_name'].apply(hash_pii)
    customers_df['email_hash'] = customers_df['email'].apply(hash_pii)

    # Create Dimension: dim_customer
    dim_customer = customers_df[['customer_id', 'first_name', 'last_name_hash', 'email_hash']].copy()

    # Create Dimension: dim_merchant
    dim_merchant = merchants_df[['merchant_name', 'category']].drop_duplicates().copy()
    
    # --- THE FIX ---
    # Create Dimension: dim_date
    # First, ensure the transaction_date is a datetime object
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])
    # Create a new column with just the date part (normalized)
    transactions_df['date_only'] = transactions_df['transaction_date'].dt.date
    
    # Now, create the date range from the normalized date column. This is robust.
    date_range = pd.date_range(start=transactions_df['date_only'].min(), end=transactions_df['date_only'].max(), freq='D')
    dim_date = pd.DataFrame({
        'date_key': [int(d.strftime('%Y%m%d')) for d in date_range],
        'full_date': date_range.date,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.strftime('%A'),
        'month': date_range.month,
        'month_name': date_range.strftime('%B'),
        'year': date_range.year
    })

    # Create Fact Table: fact_transactions
    fact_transactions = transactions_df.copy()
    # Use the normalized date to create the key, ensuring consistency
    fact_transactions['date_key'] = pd.to_datetime(fact_transactions['date_only']).dt.strftime('%Y%m%d').astype(int)
    
    logging.info("Transformation complete.")
    return dim_customer, dim_merchant, dim_date, fact_transactions

def load(dim_customer, dim_merchant, dim_date, fact_transactions, dwh_engine):
    """Loads transformed data into the data warehouse."""
    logging.info("Starting LOAD phase...")
    try:
        # --- Transaction 1: Setup schema and load dimensions ---
        logging.info("Setting up schema and loading dimensions...")
        with dwh_engine.connect() as conn:
            # Execute DDL to create schema. This drops all tables with CASCADE.
            with open('etl/create_dwh_schema.sql', 'r') as file:
                schema_sql = file.read()
                conn.execute(text(schema_sql))
            
            # Use if_exists='append' because the tables are now guaranteed to be empty.
            # Pass the connection `conn` directly to ensure it's part of this transaction.
            dim_customer.to_sql('dim_customer', conn, if_exists='append', index=False)
            dim_merchant.to_sql('dim_merchant', conn, if_exists='append', index=False)
            dim_date.to_sql('dim_date', conn, if_exists='append', index=False)
            
            # Commit the entire transaction for schema and dimensions.
            conn.commit()
        logging.info("Dimension tables loaded successfully.")

        # --- Transaction 2: Load fact table ---
        logging.info("Loading fact table...")
        with dwh_engine.connect() as conn:
            # Create surrogate key lookups from the now-committed dimension tables
            customer_keys = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", conn)
            merchant_keys = pd.read_sql("SELECT merchant_key, merchant_name FROM dim_merchant", conn)
            
            # Rename the column in the fact DataFrame to match the dimension before merging.
            fact_transactions.rename(columns={'merchant_details': 'merchant_name'}, inplace=True)
            fact_transactions = fact_transactions.merge(customer_keys, on='customer_id')
            fact_transactions = fact_transactions.merge(merchant_keys, on='merchant_name')
            
            # Select and rename final columns for fact table
            final_fact_df = fact_transactions[['transaction_id', 'customer_key', 'merchant_key', 'date_key', 'amount']]
            
            # Load fact table
            final_fact_df.to_sql('fact_transactions', conn, if_exists='append', index=False)
            
            # Commit the fact table transaction.
            conn.commit()
        logging.info("Fact table loaded successfully.")

    except Exception as e:
        logging.error(f"Error during load phase: {e}")
        raise

# --- 4. MAIN ORCHESTRATION ---
if __name__ == "__main__":
    start_time = time()
    logging.info("====== Starting ETL Job ======")
    try:
        # Get database connections
        source_engine = get_db_engine('postgres', os.getenv('SOURCE_DB_USER'), os.getenv('SOURCE_DB_PASSWORD'), os.getenv('SOURCE_DB_HOST'), os.getenv('SOURCE_DB_PORT'), os.getenv('SOURCE_DB_NAME'))
        dwh_engine = get_db_engine('postgres', os.getenv('DWH_DB_USER'), os.getenv('DWH_DB_PASSWORD'), os.getenv('DWH_DB_HOST'), os.getenv('DWH_DB_PORT'), os.getenv('DWH_DB_NAME'))
        mongo_client = get_mongo_client(os.getenv('MONGO_USER'), os.getenv('MONGO_PASSWORD'), os.getenv('MONGO_HOST'), os.getenv('MONGO_PORT'))
        
        # Run ETL
        customers, transactions, merchants = extract(source_engine, mongo_client)
        dim_cust, dim_merch, dim_dt, fact_trans = transform(customers, transactions, merchants)
        load(dim_cust, dim_merch, dim_dt, fact_trans, dwh_engine)
        
        logging.info("====== ETL Job Finished Successfully ======")
    except Exception as e:
        logging.critical(f"ETL Job failed: {e}")
    finally:
        end_time = time()
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds.")
