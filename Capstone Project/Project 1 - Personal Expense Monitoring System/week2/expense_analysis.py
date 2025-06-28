# expense_analysis.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine  # Changed from pymysql
from pymongo import MongoClient
import matplotlib.pyplot as plt


# ======================
# 1. LOAD DATA (FIXED)
# ======================
def load_mysql_data():
    """Load expense data from MySQL using SQLAlchemy"""
    try:
        engine = create_engine('mysql+pymysql://root:root@localhost/expense_tracker')
        query = """
        SELECT e.date, u.name AS user, c.name AS category, e.amount, e.description
        FROM expenses e
        JOIN users u ON e.user_id = u.user_id
        JOIN categories c ON e.category_id = c.category_id
        """
        df = pd.read_sql(query, engine)
        df['date'] = pd.to_datetime(df['date'])  # Convert date immediately
        return df
    except Exception as e:
        print(f"MySQL Error: {e}")
        return pd.DataFrame()


def load_mongo_data():
    """Load receipt data from MongoDB with proper field mapping"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        receipts = list(client['expense_tracker']['receipts'].find({}, {
            'date': 1,
            'user_id': 1,
            'amount': 1,
            'merchant': 1,
            'category': 1,
            '_id': 0
        }))

        if not receipts:
            return pd.DataFrame()

        df = pd.DataFrame(receipts)
        df['date'] = pd.to_datetime(df['date'])
        df['description'] = df.get('merchant', 'MongoDB Receipt')
        return df
    except Exception as e:
        print(f"MongoDB Error: {e}")
        return pd.DataFrame()


# ======================
# 2. CLEAN DATA (FIXED)
# ======================
def clean_data(df, source):
    """Standardize data with source-specific handling"""
    if df.empty:
        return df

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M')

    # MongoDB-specific cleaning
    if source == 'mongo':
        df['user'] = 'User ' + df['user_id'].astype(str)
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # MySQL-specific cleaning
    elif source == 'mysql':
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    return df.dropna(subset=['date', 'amount'])


# ======================
# 3. ANALYZE DATA (FIXED)
# ======================
def analyze_data(mysql_df, mongo_df):
    """Combine and analyze data with validation"""
    try:
        # Verify we have the required columns
        required_cols = ['date', 'user', 'category', 'amount']
        for col in required_cols:
            if col not in mysql_df.columns:
                mysql_df[col] = None
            if col not in mongo_df.columns:
                mongo_df[col] = None

        # Safe concatenation
        combined = pd.concat([
            mysql_df[required_cols],
            mongo_df[required_cols]
        ], ignore_index=True)

        # Ensure month exists
        if 'month' not in combined.columns:
            combined['month'] = combined['date'].dt.to_period('M')

        # Calculate metrics
        monthly_totals = combined.groupby(['month', 'category'])['amount'].sum().unstack(fill_value=0)
        user_spending = combined.groupby('user')['amount'].agg(['sum', 'count', 'mean'])

        return monthly_totals, user_spending, combined

    except Exception as e:
        print(f"Analysis Error: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# ======================
# 4. VISUALIZE RESULTS
# ======================
def generate_visuals(monthly_totals, user_spending):
    """Create plots with error handling"""
    try:
        if not monthly_totals.empty:
            plt.figure(figsize=(12, 6))
            monthly_totals.plot(kind='bar', stacked=True)
            plt.title('Monthly Spending by Category')
            plt.ylabel('Amount ($)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('monthly_spending.png')
            plt.close()

        if not user_spending.empty:
            plt.figure(figsize=(10, 5))
            user_spending['sum'].sort_values().plot(kind='barh')
            plt.title('Total Spending by User')
            plt.xlabel('Amount ($)')
            plt.tight_layout()
            plt.savefig('user_spending.png')
            plt.close()

    except Exception as e:
        print(f"Visualization Error: {e}")


# ======================
# MAIN EXECUTION (FIXED)
# ======================
if __name__ == "__main__":
    print("🚀 Loading data...")
    mysql_data = load_mysql_data()
    mongo_data = load_mongo_data()

    print("🧹 Cleaning data...")
    mysql_clean = clean_data(mysql_data, 'mysql')
    mongo_clean = clean_data(mongo_data, 'mongo')

    print("🔍 Analyzing...")
    monthly, users, combined = analyze_data(mysql_clean, mongo_clean)

    print("\n=== Monthly Spending ===")
    print(monthly.head())

    print("\n=== User Spending ===")
    print(users.head())

    print("\n💾 Saving results...")
    if not combined.empty:
        combined.to_csv('combined_expenses.csv', index=False)
        generate_visuals(monthly, users)
        print("""
✅ Done!
- Generated: monthly_spending.png, user_spending.png
- Exported: combined_expenses.csv
""")
    else:
        print("❌ No data to save. Check your database connections.")