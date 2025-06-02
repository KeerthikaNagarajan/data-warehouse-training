CREATE DATABASE SmartHome
USE SmartHome
--1.Create Tables
CREATE TABLE rooms (
    room_id INT PRIMARY KEY,
    room_name VARCHAR(100)
);

CREATE TABLE devices (
    device_id INT PRIMARY KEY,
    device_name VARCHAR(100),
    room_id INT FOREIGN KEY REFERENCES rooms(room_id),
    status VARCHAR(20) -- e.g., 'active', 'inactive'
);

CREATE TABLE energy_logs (
    log_id INT PRIMARY KEY IDENTITY(1,1),
    device_id INT FOREIGN KEY REFERENCES devices(device_id),
    timestamp DATETIME,
    energy_kwh FLOAT
);
--2.CRUD OPERATIONS
--Insert
INSERT INTO devices (device_id, device_name, room_id, status)
VALUES (41, 'Smart Heater', 1, 'active');
--Read
SELECT * FROM devices WHERE status = 'active';
--Update
UPDATE devices SET status = 'inactive' WHERE device_id = 1;
--Delete
DELETE FROM devices WHERE device_id = 41;

--3.Stored Procedure
CREATE PROCEDURE sp_TotalEnergyPerRoomPerDay
    @date DATE
AS
BEGIN
    SELECT r.room_name, SUM(e.energy_kwh) AS total_energy
    FROM energy_logs e
    JOIN devices d ON e.device_id = d.device_id
    JOIN rooms r ON d.room_id = r.room_id
    WHERE CAST(e.timestamp AS DATE) = @date
    GROUP BY r.room_name;
END