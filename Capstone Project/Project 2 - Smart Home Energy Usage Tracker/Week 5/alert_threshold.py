import pandas as pd
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--threshold', type=float, required=True)
args = parser.parse_args()

df = pd.read_csv(args.input)
overuse = df[df['daily_kwh'] > args.threshold]

if not overuse.empty:
    print("ALERT: Some devices exceeded the threshold:")
    print(overuse[['device_id', 'daily_kwh']])
else:
    print("All devices within usage limits.")



