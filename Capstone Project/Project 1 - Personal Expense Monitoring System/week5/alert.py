import pandas as pd
import os

# Step 1: Load your Week 1 expenses file
df = pd.read_csv('expenses.csv')

# Step 2: Clean and convert
df['amount'] = df['amount'].replace('[\$,]', '', regex=True).astype(float)
df['date'] = pd.to_datetime(df['date'])
df['month'] = df['date'].dt.to_period('M')

# Step 3: Monthly summary
summary = df.groupby(['month', 'category'])['amount'].sum().unstack().fillna(0)

# Step 4: Save summary
os.makedirs('output', exist_ok=True)
summary.to_csv('output/monthly_summary.csv')

# Step 5: Alert
total = df['amount'].sum()
if total > 10000:
    print("⚠️ Alert: Spending exceeded ₹10,000!")
else:
    print("✅ Spending is within the safe limit.")
