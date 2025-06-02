--4.Use MongoDB to store raw sensor logs (JSON format)

use SmartHome;
already on db SmartHome
db.SensorLogs.insertMany([
  {
    device_id: 1,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.52
  },
  {
    device_id: 2,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.61
  },
  {
    device_id: 3,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.47
  },
  {
    device_id: 4,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.75
  },
  {
    device_id: 5,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.39
  },
  {
    device_id: 6,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.58
  },
  {
    device_id: 7,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.44
  },
  {
    device_id: 8,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.66
  },
  {
    device_id: 9,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.79
  },
  {
    device_id: 10,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.33
  },
  {
    device_id: 11,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.42
  },
  {
    device_id: 12,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.68
  },
  {
    device_id: 13,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.56
  },
  {
    device_id: 14,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.51
  },
  {
    device_id: 15,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.60
  },
  {
    device_id: 16,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.29
  },
  {
    device_id: 17,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.72
  },
  {
    device_id: 18,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.36
  },
  {
    device_id: 19,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.69
  },
  {
    device_id: 20,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.43
  },
  {
    device_id: 21,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.59
  },
  {
    device_id: 22,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.38
  },
  {
    device_id: 23,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.64
  },
  {
    device_id: 24,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.48
  },
  {
    device_id: 25,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.57
  },
  {
    device_id: 26,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.50
  },
  {
    device_id: 27,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.73
  },
  {
    device_id: 28,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.62
  },
  {
    device_id: 29,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.49
  },
  {
    device_id: 30,
    timestamp: ISODate("2025-05-01T08:00:00Z"),
    energy_kwh: 0.65
  }
]);

--5.Add indexes for quick lookup by device ID or timestamp

db.SensorLogs.createIndex({ device_id: 1 });
db.SensorLogs.createIndex({ timestamp: 1 });


