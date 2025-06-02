import pandas as pd
import numpy as np


def load_and_clean_data(filepath):
    """Load and clean the expense data"""
    df = pd.read_csv(filepath)

    # Data cleaning
    df['amount'] = df['amount'].replace('[\₹,]', '', regex=True).astype(float)
    df['expense_date'] = pd.to_datetime(df['expense_date'])
    df['month'] = df['expense_date'].dt.to_period('M')

    # Map category IDs to names
    category_map = {
        1: 'Groceries', 2: 'Transport', 3: 'Dining', 4: 'Utilities',
        5: 'Entertainment', 6: 'Shopping', 7: 'Healthcare',
        8: 'Education', 9: 'Rent', 10: 'Others'
    }
    df['category'] = df['category_id'].map(category_map)

    return df


def analyze_expenses(df):
    """Perform various analyses on the expense data"""
    # Monthly totals using string method names to avoid FutureWarnings
    monthly_totals = df.groupby('month')['amount'].agg(['sum', 'mean', 'max', 'min'])

    # Category-wise breakdown
    monthly_expense = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)
    monthly_percentage = monthly_expense.div(monthly_expense.sum(axis=1), axis=0) * 100

    # Top expenses per user
    top_expenses = df.sort_values(['user_id', 'amount'], ascending=[True, False])
    top_expenses_per_user = top_expenses.groupby('user_id').head(3)

    return monthly_totals, monthly_expense, monthly_percentage, top_expenses_per_user


def save_results(*args):
    """Save analysis results to CSV files"""
    monthly_totals, monthly_expense, monthly_percentage, _ = args
    monthly_expense.to_csv('monthly_expense_summary.csv')
    monthly_percentage.round(2).to_csv('monthly_expense_percentage.csv')
    monthly_totals.to_csv('monthly_stats.csv')


def main():
    # File path - adjust as needed
    filepath = 'expenses.csv'

    # Load and clean data
    df = load_and_clean_data(filepath)
    df.to_csv('cleaned_expenses.csv', index=False)

    # Analyze data
    monthly_totals, monthly_expense, monthly_percentage, top_expenses = analyze_expenses(df)

    # Save results
    save_results(monthly_totals, monthly_expense, monthly_percentage, top_expenses)

    # Print results
    print("\nMonthly Expense Summary:")
    print(monthly_totals)

    print("\nMonthly Expenses by Category:")
    print(monthly_expense)

    print("\nMonthly Expense Percentage by Category:")
    print(monthly_percentage.round(2))

    print("\nTop Expenses per User:")
    print(top_expenses[['user_id', 'category', 'amount', 'expense_date']])

    print("\nData processing complete. Files saved:")
    print("- cleaned_expenses.csv")
    print("- monthly_expense_summary.csv")
    print("- monthly_expense_percentage.csv")
    print("- monthly_stats.csv")


if __name__ == "__main__":
    main()