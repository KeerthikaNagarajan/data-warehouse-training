CREATE DATABASE EnergyTracker
USE EnergyTracker
--1.Create MySQL tables for devices, energy_logs, and rooms
-- Table: rooms
CREATE TABLE rooms (
    room_id INT PRIMARY KEY,
    room_name VARCHAR(100) NOT NULL,
    floor INT NOT NULL,
    area_sqft INT,
    purpose VARCHAR(50),
    occupancy_limit INT
);

-- Table: devices
CREATE TABLE devices (
    device_id INT PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    device_type VARCHAR(50),
    installation_date DATE,
    is_active BIT,
    room_id INT,
    FOREIGN KEY (room_id) REFERENCES rooms(room_id)
);

-- Table: energy_logs
CREATE TABLE energy_logs (
    log_id INT PRIMARY KEY,
    device_id INT,
    timestamp DATETIME,
    energy_kwh FLOAT,
    voltage FLOAT,
    [current] FLOAT,
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

-- Insert into rooms
BULK INSERT rooms
FROM 'C:\Data\SmartHome\rooms.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT = 'CSV',
    TABLOCK
);

-- Insert into devices
BULK INSERT devices
FROM 'C:\Data\SmartHome\devices.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT = 'CSV',
    TABLOCK
);

-- Insert into energy_logs
BULK INSERT energy_logs
FROM 'C:\Data\SmartHome\energy_logs.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FORMAT = 'CSV',
    TABLOCK
);

--2. CRUD Operations 
--Rooms Table
--Insert
INSERT INTO rooms (room_id, room_name, floor, area_sqft, purpose, occupancy_limit)
VALUES (1, 'Living Room', 1, 300, 'Leisure', 5);
--Read 
SELECT * FROM rooms;
--Update
UPDATE rooms
SET room_name = 'Main Living Room', occupancy_limit = 6 WHERE room_id = 1;
--Delete
DELETE FROM rooms WHERE room_id = 1;

--Devices Table
--Insert
INSERT INTO devices (device_id, device_name, device_type, installation_date, is_active, room_id)
VALUES (10, 'Air Conditioner', 'HVAC', '2024-01-01', 1, 1);
--Read
SELECT * FROM devices WHERE device_id = 10;
--Update
UPDATE devices SET is_active = 0 WHERE device_id = 10;
--Delet
DELETE FROM devices WHERE device_id = 10;

--Energy_Logs Table
--Insert
INSERT INTO energy_logs (log_id, device_id, timestamp, energy_kwh, voltage, [current])
VALUES (100, 10, '2025-06-01 10:00:00', 1.25, 220, 5);
--Read
SELECT * FROM energy_logs WHERE log_id = 100;
--Update
UPDATE energy_logs SET energy_kwh = 1.5 WHERE log_id = 100;
--Delete
DELETE FROM energy_logs WHERE log_id = 100;

--3.Stored Procedure: Calculate Total Energy Usage per Room per Day

CREATE PROCEDURE GetTotalEnergyUsagePerRoomPerDay
AS
BEGIN
    SELECT 
        r.room_id,
        r.room_name,
        CAST(el.timestamp AS DATE) AS usage_date,
        SUM(el.energy_kwh) AS total_energy_kwh
    FROM energy_logs el
    INNER JOIN devices d ON el.device_id = d.device_id
    INNER JOIN rooms r ON d.room_id = r.room_id
    GROUP BY r.room_id, r.room_name, CAST(el.timestamp AS DATE)
    ORDER BY r.room_id, usage_date;
END

EXEC GetTotalEnergyUsagePerRoomPerDay;
