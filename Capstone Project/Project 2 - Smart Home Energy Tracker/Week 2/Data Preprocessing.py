import pandas as pd
import numpy as np

#1.Load CSVs
energy_logs = pd.read_csv("energy_logs.csv")
devices = pd.read_csv("devices.csv")
rooms = pd.read_csv("rooms.csv")

#2:Clean the data
energy_logs['timestamp'] = pd.to_datetime(energy_logs['timestamp'], errors='coerce')
energy_logs['energy_kwh'] = pd.to_numeric(energy_logs['energy_kwh'], errors='coerce')
cleaned_logs = energy_logs.dropna(subset=['timestamp', 'energy_kwh', 'device_id'])

#3:Calculate total and average energy per device
energy_per_device = cleaned_logs.groupby('device_id')['energy_kwh'].agg([np.sum, np.mean]).reset_index()
energy_per_device.columns = ['device_id', 'total_energy_kwh', 'average_energy_kwh']

#Merge with room info
device_room_info = pd.merge(devices[['device_id', 'room_id']], rooms, on='room_id', how='left')
logs_with_room = pd.merge(cleaned_logs, device_room_info, on='device_id', how='left')

#4:Room-level summaries
room_summary = logs_with_room.groupby('room_name')['energy_kwh'].sum().reset_index()
room_summary.columns = ['room_name', 'total_energy_kwh']

# Save results
energy_per_device.to_csv("energy_per_device.csv", index=False)
room_summary.to_csv("room_energy_summary.csv", index=False)
