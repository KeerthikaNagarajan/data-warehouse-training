-- Employee Attendance Table 
CREATE TABLE EmployeeAttendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeName VARCHAR(50),
    Department VARCHAR(30),
    Date DATE,
    Status VARCHAR(20),
    HoursWorked DECIMAL(4,1)
);

-- Insert Data
INSERT INTO EmployeeAttendance VALUES
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);

-- 1. Add a new attendance record (Neha Sharma, Finance, Present, 8 hours)
INSERT INTO EmployeeAttendance 
VALUES (6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

-- 2. Update Riya Patel's status from Late to Present
UPDATE EmployeeAttendance
SET Status = 'Present'
WHERE EmployeeName = 'Riya Patel' AND Date = '2025-05-01';

-- 3. Delete the attendance entry for Priya Singh
DELETE FROM EmployeeAttendance
WHERE EmployeeName = 'Priya Singh';

-- 4. Display all attendance records sorted by EmployeeName
SELECT * FROM EmployeeAttendance
ORDER BY EmployeeName;

-- 5. List employees sorted by HoursWorked in descending order
SELECT * FROM EmployeeAttendance
ORDER BY HoursWorked DESC;

-- 6. Display all attendance records for the IT department
SELECT * FROM EmployeeAttendance
WHERE Department = 'IT';

-- 7. List all Present employees from the IT department
SELECT * FROM EmployeeAttendance
WHERE Status = 'Present' AND Department = 'IT';

-- 8. Retrieve all employees who are either Absent or Late
SELECT * FROM EmployeeAttendance
WHERE Status IN ('Absent', 'Late');

-- 9. Calculate total hours worked grouped by Department
SELECT Department, SUM(HoursWorked) AS TotalHours
FROM EmployeeAttendance
GROUP BY Department;

-- 10. Find the average hours worked per day
SELECT AVG(HoursWorked) AS AverageHours
FROM EmployeeAttendance;

-- 11. Count employees by attendance status
SELECT Status, COUNT(*) AS EmployeeCount
FROM EmployeeAttendance
GROUP BY Status;

-- 12. List employees whose names start with 'R'
SELECT * FROM EmployeeAttendance
WHERE EmployeeName LIKE 'R%';

-- 13. Display employees who worked >6 hours and are Present
SELECT * FROM EmployeeAttendance
WHERE HoursWorked > 6 AND Status = 'Present';

-- 14. List employees who worked between 6 and 8 hours
SELECT * FROM EmployeeAttendance
WHERE HoursWorked BETWEEN 6 AND 8;

-- 15. Display top 2 employees with highest hours worked
SELECT * FROM EmployeeAttendance
ORDER BY HoursWorked DESC
LIMIT 2;

-- 16. List employees who worked less than average hours
SELECT * FROM EmployeeAttendance
WHERE HoursWorked < (SELECT AVG(HoursWorked) FROM EmployeeAttendance);

-- 17. Calculate average hours worked for each status
SELECT Status, AVG(HoursWorked) AS AvgHours
FROM EmployeeAttendance
GROUP BY Status;

-- 18. Identify employees with multiple attendance records on same date
SELECT EmployeeName, Date, COUNT(*) AS Records
FROM EmployeeAttendance
GROUP BY EmployeeName, Date
HAVING COUNT(*) > 1;

-- 19. Find department with most Present employees
SELECT Department, COUNT(*) AS PresentCount
FROM EmployeeAttendance
WHERE Status = 'Present'
GROUP BY Department
ORDER BY PresentCount DESC
LIMIT 1;

-- 20. Find employee with most hours worked in each department
SELECT e.Department, e.EmployeeName, e.HoursWorked
FROM EmployeeAttendance e
INNER JOIN (
    SELECT Department, MAX(HoursWorked) AS MaxHours
    FROM EmployeeAttendance
    GROUP BY Department
) m ON e.Department = m.Department AND e.HoursWorked = m.MaxHours;
