--4.Use MongoDB to store raw sensor logs (JSON format)
use SmartHome
switched to db SmartHome
db["Sensor Logs"].find()
db.Sensor_Logs.insertMany([
  { device_id: 4, timestamp: new Date("2025-05-30T10:12:00Z"), energy_kwh: 3.24, voltage: 218.5, current: 4.8, room_id: 1 },
  { device_id: 7, timestamp: new Date("2025-05-30T11:15:00Z"), energy_kwh: 0.75, voltage: 221.3, current: 2.1, room_id: 2 },
  { device_id: 2, timestamp: new Date("2025-05-30T12:20:00Z"), energy_kwh: 1.12, voltage: 219.7, current: 3.3, room_id: 3 },
  { device_id: 5, timestamp: new Date("2025-05-30T13:25:00Z"), energy_kwh: 2.54, voltage: 217.9, current: 5.1, room_id: 4 },
  { device_id: 1, timestamp: new Date("2025-05-30T14:30:00Z"), energy_kwh: 4.10, voltage: 222.0, current: 7.0, room_id: 5 },
  { device_id: 6, timestamp: new Date("2025-05-30T15:35:00Z"), energy_kwh: 3.95, voltage: 218.0, current: 6.5, room_id: 1 },
  { device_id: 3, timestamp: new Date("2025-05-30T16:40:00Z"), energy_kwh: 1.75, voltage: 220.2, current: 3.8, room_id: 2 },
  { device_id: 8, timestamp: new Date("2025-05-30T17:45:00Z"), energy_kwh: 0.55, voltage: 219.0, current: 1.9, room_id: 3 },
  { device_id: 4, timestamp: new Date("2025-05-30T18:50:00Z"), energy_kwh: 2.20, voltage: 221.5, current: 4.3, room_id: 4 },
  { device_id: 7, timestamp: new Date("2025-05-30T19:55:00Z"), energy_kwh: 3.33, voltage: 217.6, current: 5.7, room_id: 5 },

  { device_id: 2, timestamp: new Date("2025-05-31T09:00:00Z"), energy_kwh: 1.80, voltage: 218.7, current: 3.9, room_id: 1 },
  { device_id: 5, timestamp: new Date("2025-05-31T10:05:00Z"), energy_kwh: 4.12, voltage: 220.1, current: 7.2, room_id: 2 },
  { device_id: 1, timestamp: new Date("2025-05-31T11:10:00Z"), energy_kwh: 2.45, voltage: 219.8, current: 5.0, room_id: 3 },
  { device_id: 6, timestamp: new Date("2025-05-31T12:15:00Z"), energy_kwh: 3.60, voltage: 221.7, current: 6.0, room_id: 4 },
  { device_id: 3, timestamp: new Date("2025-05-31T13:20:00Z"), energy_kwh: 0.90, voltage: 218.3, current: 2.4, room_id: 5 },
  { device_id: 8, timestamp: new Date("2025-05-31T14:25:00Z"), energy_kwh: 1.95, voltage: 220.6, current: 4.1, room_id: 1 },
  { device_id: 4, timestamp: new Date("2025-05-31T15:30:00Z"), energy_kwh: 3.10, voltage: 217.8, current: 5.5, room_id: 2 },
  { device_id: 7, timestamp: new Date("2025-05-31T16:35:00Z"), energy_kwh: 2.78, voltage: 219.9, current: 4.7, room_id: 3 },
  { device_id: 2, timestamp: new Date("2025-05-31T17:40:00Z"), energy_kwh: 4.00, voltage: 222.2, current: 7.3, room_id: 4 },
  { device_id: 5, timestamp: new Date("2025-05-31T18:45:00Z"), energy_kwh: 1.22, voltage: 220.4, current: 3.2, room_id: 5 },

  { device_id: 1, timestamp: new Date("2025-06-01T09:50:00Z"), energy_kwh: 3.85, voltage: 218.9, current: 6.8, room_id: 1 },
  { device_id: 6, timestamp: new Date("2025-06-01T10:55:00Z"), energy_kwh: 0.65, voltage: 217.2, current: 2.0, room_id: 2 },
  { device_id: 3, timestamp: new Date("2025-06-01T12:00:00Z"), energy_kwh: 2.40, voltage: 221.4, current: 5.1, room_id: 3 },
  { device_id: 8, timestamp: new Date("2025-06-01T13:05:00Z"), energy_kwh: 1.33, voltage: 219.3, current: 3.7, room_id: 4 },
  { device_id: 4, timestamp: new Date("2025-06-01T14:10:00Z"), energy_kwh: 2.90, voltage: 220.9, current: 5.8, room_id: 5 },
  { device_id: 7, timestamp: new Date("2025-06-01T15:15:00Z"), energy_kwh: 0.88, voltage: 218.6, current: 2.6, room_id: 1 },
  { device_id: 2, timestamp: new Date("2025-06-01T16:20:00Z"), energy_kwh: 3.70, voltage: 221.0, current: 6.2, room_id: 2 },
  { device_id: 5, timestamp: new Date("2025-06-01T17:25:00Z"), energy_kwh: 1.45, voltage: 219.1, current: 3.4, room_id: 3 },
  { device_id: 1, timestamp: new Date("2025-06-01T18:30:00Z"), energy_kwh: 4.25, voltage: 222.3, current: 7.4, room_id: 4 },
  { device_id: 6, timestamp: new Date("2025-06-01T19:35:00Z"), energy_kwh: 3.50, voltage: 217.4, current: 6.1, room_id: 5 },

  { device_id: 3, timestamp: new Date("2025-06-02T09:40:00Z"), energy_kwh: 2.05, voltage: 220.5, current: 4.5, room_id: 1 },
  { device_id: 8, timestamp: new Date("2025-06-02T10:45:00Z"), energy_kwh: 1.00, voltage: 219.2, current: 3.0, room_id: 2 },
  { device_id: 4, timestamp: new Date("2025-06-02T11:50:00Z"), energy_kwh: 3.40, voltage: 221.1, current: 5.9, room_id: 3 },
  { device_id: 7, timestamp: new Date("2025-06-02T12:55:00Z"), energy_kwh: 2.15, voltage: 218.4, current: 4.2, room_id: 4 },
  { device_id: 2, timestamp: new Date("2025-06-02T14:00:00Z"), energy_kwh: 3.95, voltage: 220.7, current: 6.7, room_id: 5 },
  { device_id: 5, timestamp: new Date("2025-06-02T15:05:00Z"), energy_kwh: 1.10, voltage: 217.7, current: 2.9, room_id: 1 },
  { device_id: 1, timestamp: new Date("2025-06-02T16:10:00Z"), energy_kwh: 4.40, voltage: 221.8, current: 7.5, room_id: 2 },
  { device_id: 6, timestamp: new Date("2025-06-02T17:15:00Z"), energy_kwh: 3.20, voltage: 219.5, current: 5.6, room_id: 3 },
  { device_id: 3, timestamp: new Date("2025-06-02T18:20:00Z"), energy_kwh: 2.75, voltage: 220.3, current: 4.9, room_id: 4 },
  { device_id: 8, timestamp: new Date("2025-06-02T19:25:00Z"), energy_kwh: 0.80, voltage: 218.8, current: 2.3, room_id: 5 }
]);
--Result
{
  acknowledged: true,
  insertedIds: {
    '0': ObjectId('683d7e2d5504739874e97b6d'),
    '1': ObjectId('683d7e2d5504739874e97b6e'),
    '2': ObjectId('683d7e2d5504739874e97b6f'),
    '3': ObjectId('683d7e2d5504739874e97b70'),
    '4': ObjectId('683d7e2d5504739874e97b71'),
    '5': ObjectId('683d7e2d5504739874e97b72'),
    '6': ObjectId('683d7e2d5504739874e97b73'),
    '7': ObjectId('683d7e2d5504739874e97b74'),
    '8': ObjectId('683d7e2d5504739874e97b75'),
    '9': ObjectId('683d7e2d5504739874e97b76'),
    '10': ObjectId('683d7e2d5504739874e97b77'),
    '11': ObjectId('683d7e2d5504739874e97b78'),
    '12': ObjectId('683d7e2d5504739874e97b79'),
    '13': ObjectId('683d7e2d5504739874e97b7a'),
    '14': ObjectId('683d7e2d5504739874e97b7b'),
    '15': ObjectId('683d7e2d5504739874e97b7c'),
    '16': ObjectId('683d7e2d5504739874e97b7d'),
    '17': ObjectId('683d7e2d5504739874e97b7e'),
    '18': ObjectId('683d7e2d5504739874e97b7f'),
    '19': ObjectId('683d7e2d5504739874e97b80'),
    '20': ObjectId('683d7e2d5504739874e97b81'),
    '21': ObjectId('683d7e2d5504739874e97b82'),
    '22': ObjectId('683d7e2d5504739874e97b83'),
    '23': ObjectId('683d7e2d5504739874e97b84'),
    '24': ObjectId('683d7e2d5504739874e97b85'),
    '25': ObjectId('683d7e2d5504739874e97b86'),
    '26': ObjectId('683d7e2d5504739874e97b87'),
    '27': ObjectId('683d7e2d5504739874e97b88'),
    '28': ObjectId('683d7e2d5504739874e97b89'),
    '29': ObjectId('683d7e2d5504739874e97b8a'),
    '30': ObjectId('683d7e2d5504739874e97b8b'),
    '31': ObjectId('683d7e2d5504739874e97b8c'),
    '32': ObjectId('683d7e2d5504739874e97b8d'),
    '33': ObjectId('683d7e2d5504739874e97b8e'),
    '34': ObjectId('683d7e2d5504739874e97b8f'),
    '35': ObjectId('683d7e2d5504739874e97b90'),
    '36': ObjectId('683d7e2d5504739874e97b91'),
    '37': ObjectId('683d7e2d5504739874e97b92'),
    '38': ObjectId('683d7e2d5504739874e97b93'),
    '39': ObjectId('683d7e2d5504739874e97b94')
  }
}

--5.Add indexes for quick lookup by device ID or timestamp
db.Sensor_Logs.createIndex({ device_id: 1 });
db.Sensor_Logs.createIndex({ timestamp: 1 });
timestamp_1