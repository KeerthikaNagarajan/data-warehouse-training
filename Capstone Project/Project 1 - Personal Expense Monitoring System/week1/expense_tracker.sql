-- =============================================
-- DATABASE SETUP
-- =============================================
CREATE DATABASE IF NOT EXISTS expense_tracker;
USE expense_tracker;

-- =============================================
-- TABLE CREATION
-- =============================================
-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

-- Categories table
CREATE TABLE IF NOT EXISTS categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

-- Expenses table
CREATE TABLE IF NOT EXISTS expenses (
    expense_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    category_id INT,
    amount DECIMAL(10, 2) NOT NULL,
    date DATE NOT NULL,
    description VARCHAR(200),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- =============================================
-- SAMPLE DATA INSERTION
-- =============================================
-- Insert users
INSERT INTO users (name, email) VALUES 
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com'),
('Emma Johnson', 'emma@example.com'),
('Michael Chen', 'michael@example.com'),
('Sarah Williams', 'sarah@example.com'),
('David Kim', 'david@example.com'),
('Lisa Wong', 'lisa@example.com');

-- Insert categories
INSERT INTO categories (name) VALUES 
('Food'), 
('Transport'), 
('Entertainment'),
('Rent'), 
('Utilities'), 
('Healthcare'),
('Shopping'), 
('Travel'), 
('Education'),
('Gifts'), 
('Insurance');

-- Insert expenses (40+ transactions across 3 months)
INSERT INTO expenses (user_id, category_id, amount, date, description) VALUES
-- October 2023
(1, 1, 50.00, '2023-10-01', 'Groceries'),
(1, 2, 20.00, '2023-10-02', 'Bus fare'),
(2, 3, 15.99, '2023-10-03', 'Netflix'),
(1, 1, 75.30, '2023-10-05', 'Whole Foods'),
(1, 4, 1200.00, '2023-10-01', 'Apartment rent'),
(3, 5, 85.20, '2023-10-15', 'Electric bill'),
(2, 6, 150.00, '2023-10-12', 'Doctor visit'),
(4, 7, 89.99, '2023-10-18', 'Nike shoes'),
(2, 8, 300.00, '2023-10-25', 'Train to NYC'),
(3, 9, 200.00, '2023-10-22', 'Online course'),
(5, 10, 45.00, '2023-10-30', 'Birthday gift'),
(1, 11, 120.00, '2023-10-10', 'Car insurance'),

-- November 2023
(1, 1, 60.40, '2023-11-02', 'Grocery run'),
(2, 3, 10.99, '2023-11-15', 'Disney+'),
(3, 5, 90.00, '2023-11-20', 'Internet bill'),
(4, 2, 35.50, '2023-11-05', 'Taxi ride'),
(5, 6, 75.00, '2023-11-12', 'Dentist'),
(6, 7, 120.50, '2023-11-25', 'Christmas shopping'),
(7, 8, 450.00, '2023-11-18', 'Flight to Chicago'),
(1, 9, 65.00, '2023-11-30', 'Textbooks'),

-- December 2023
(2, 1, 42.75, '2023-12-03', 'Takeout'),
(3, 4, 1250.00, '2023-12-01', 'Rent payment'),
(4, 5, 95.60, '2023-12-15', 'Gas bill'),
(5, 10, 30.00, '2023-12-24', 'Holiday card'),
(6, 11, 85.00, '2023-12-05', 'Health insurance'),
(7, 2, 18.50, '2023-12-10', 'Subway pass'),
(1, 3, 8.99, '2023-12-20', 'Spotify'),
(2, 7, 65.00, '2023-12-22', 'Clothes');

-- =============================================
-- STORED PROCEDURE FOR MONTHLY REPORTS
-- =============================================
DELIMITER //
CREATE PROCEDURE GetMonthlySpending()
BEGIN
    SELECT 
        CONCAT(YEAR(date), '-', LPAD(MONTH(date), 2, '0')) AS month,
        c.name AS category,
        SUM(amount) AS total_amount,
        COUNT(*) AS transaction_count
    FROM expenses e
    JOIN categories c ON e.category_id = c.category_id
    GROUP BY month, c.name
    ORDER BY month, total_amount DESC;
END //
DELIMITER ;

-- =============================================
-- VERIFICATION QUERIES
-- =============================================
-- View all tables
SELECT * FROM users;
SELECT * FROM categories;
SELECT * FROM expenses LIMIT 10;

-- Test the stored procedure
CALL GetMonthlySpending();

-- Get total spending by user
SELECT 
    u.name AS user,
    SUM(e.amount) AS total_spent,
    COUNT(*) AS transactions
FROM expenses e
JOIN users u ON e.user_id = u.user_id
GROUP BY u.name
ORDER BY total_spent DESC;

-- Find largest single expenses
SELECT 
    u.name AS user,
    c.name AS category,
    e.amount,
    e.date,
    e.description
FROM expenses e
JOIN users u ON e.user_id = u.user_id
JOIN categories c ON e.category_id = c.category_id
ORDER BY e.amount DESC
LIMIT 5;