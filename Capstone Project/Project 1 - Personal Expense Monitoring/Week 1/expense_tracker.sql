-- Create the database
CREATE DATABASE expense_tracker;
USE expense_tracker;

-- Create users table
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create categories table
CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description VARCHAR(255)
);

-- Create expenses table
CREATE TABLE expenses (
    expense_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    category_id INT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    description VARCHAR(255),
    expense_date DATE NOT NULL,
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- Insert sample categories
INSERT INTO categories (category_name, description) VALUES
('Groceries', 'Daily household groceries'),
('Transport', 'Public transport, fuel, etc.'),
('Dining', 'Restaurants and eating out'),
('Utilities', 'Electricity, water, internet bills'),
('Entertainment', 'Movies, OTT subscriptions'),
('Shopping', 'Clothing, electronics, etc.'),
('Healthcare', 'Medicines, doctor visits'),
('Education', 'Books, courses, etc.'),
('Rent', 'House rent'),
('Others', 'Miscellaneous expenses');

-- Insert sample users
INSERT INTO users (username, email) VALUES
('Rahul_Sharma', 'rahul.sharma@example.com'),
('Priya_Patel', 'priya.patel@example.com'),
('Amit_Kumar', 'amit.kumar@example.com');

-- Stored procedure to calculate monthly expenses by category
DELIMITER //
CREATE PROCEDURE GetMonthlyExpensesByCategory(IN userID INT, IN yearMonth VARCHAR(7))
BEGIN
    SELECT 
        c.category_name,
        SUM(e.amount) AS total_amount
    FROM 
        expenses e
    JOIN 
        categories c ON e.category_id = c.category_id
    WHERE 
        e.user_id = userID
        AND DATE_FORMAT(e.expense_date, '%Y-%m') = yearMonth
    GROUP BY 
        c.category_name
    ORDER BY 
        total_amount DESC;
END //
DELIMITER ;

-- Basic CRUD operations examples
-- Create
INSERT INTO expenses (user_id, category_id, amount, description, expense_date, payment_method)
VALUES (1, 1, 1500.00, 'Monthly groceries from Big Bazaar', '2023-11-15', 'Credit Card');

-- Read
SELECT * FROM expenses WHERE user_id = 1;

-- Update
UPDATE expenses SET amount = 1600.00 WHERE expense_id = 1;

-- Delete
DELETE FROM expenses WHERE expense_id = 1;

-- load the data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/expenses.csv'
INTO TABLE expenses
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  -- Try \r\n for Windows line endings
IGNORE 1 LINES
(user_id, category_id, amount, description, expense_date, payment_method);
