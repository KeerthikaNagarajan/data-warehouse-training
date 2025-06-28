import pandas as pd
import numpy as np

# 1. Load CSV files
energy_logs = pd.read_csv("energy_logs.csv")
devices = pd.read_csv("devices.csv")
rooms = pd.read_csv("rooms.csv")

# 2. Clean the data
energy_logs['timestamp'] = pd.to_datetime(energy_logs['timestamp'], errors='coerce')
energy_logs['energy_kwh'] = pd.to_numeric(energy_logs['energy_kwh'], errors='coerce')
cleaned_logs = energy_logs.dropna(subset=['timestamp', 'energy_kwh', 'device_id'])

# Merge devices and rooms for full context
devices_full = pd.merge(devices, rooms, on='room_id', how='left')  # room_name, floor, area_sqft, purpose
logs_enriched = pd.merge(cleaned_logs, devices_full, on='device_id', how='left')

# 3. NumPy: Total and Average Energy per Device
grouped = logs_enriched.groupby(['device_id', 'device_name', 'room_name'])

# Use NumPy for total and average
device_ids = []
device_names = []
room_names = []
total_energy = []
average_energy = []

for key, group in grouped:
    device_ids.append(key[0])
    device_names.append(key[1])
    room_names.append(key[2])
    energy_values = group['energy_kwh'].values
    total_energy.append(np.sum(energy_values))
    average_energy.append(np.mean(energy_values))

# Create DataFrame with full names
energy_per_device = pd.DataFrame({
    'device_id': device_ids,
    'device_name': device_names,
    'room_name': room_names,
    'total_energy_kwh': total_energy,
    'average_energy_kwh': average_energy
})

# 4. Pandas: Room-level total energy summary
room_summary = logs_enriched.groupby(['room_id', 'room_name', 'floor', 'area_sqft', 'purpose'])['energy_kwh'].sum().reset_index()
room_summary.columns = ['room_id', 'room_name', 'floor', 'area_sqft', 'purpose', 'total_energy_kwh']

# Save outputs
energy_per_device.to_csv("energy_per_device_numpy.csv", index=False)
room_summary.to_csv("room_energy_summary_pandas.csv", index=False)

print("Results saved")
