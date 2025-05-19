-- Create the ProductInventory table
CREATE TABLE ProductInventory (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(50),
    Category VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Supplier VARCHAR(50),
    LastRestocked DATE
);

-- Insert initial data
INSERT INTO ProductInventory VALUES
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');

-- 1. CRUD Operations
-- 1.1 Add a new product
INSERT INTO ProductInventory VALUES
(6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');

-- 1.2 Update stock quantity
UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp';

-- 1.3 Delete a discontinued product
DELETE FROM ProductInventory
WHERE ProductID = 2;

-- 1.4 Read all products
SELECT * FROM ProductInventory
ORDER BY ProductName ASC;

-- 2. Sorting and Filtering
-- 2.1 Sort by Quantity
SELECT * FROM ProductInventory
ORDER BY Quantity DESC;

-- 2.2 Filter by Category
SELECT * FROM ProductInventory
WHERE Category = 'Electronics';

-- 2.3 Filter with AND condition
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20;

-- 2.4 Filter with OR condition
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000;

-- 3. Aggregation and Grouping
-- 3.1 Total stock value calculation
SELECT SUM(Quantity * UnitPrice) AS TotalStockValue
FROM ProductInventory;

-- 3.2 Average price by category
SELECT Category, AVG(UnitPrice) AS AveragePrice
FROM ProductInventory
GROUP BY Category;

-- 3.3 Count products by supplier
SELECT COUNT(*) AS GadgetHubProducts
FROM ProductInventory
WHERE Supplier = 'GadgetHub';

-- 4. Conditional and Pattern Matching
-- 4.1 Find products by name prefix
SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%';

-- 4.2 Filter by supplier and price
SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000;

-- 4.3 Filter using BETWEEN operator
SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000;

-- 5. Advanced Queries
-- 5.1 Top 3 most expensive products
SELECT * FROM ProductInventory
ORDER BY UnitPrice DESC
LIMIT 3;

-- 5.2 Products restocked recently
SELECT * FROM ProductInventory
WHERE LastRestocked >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY);

-- 5.3 Group by Supplier
SELECT Supplier, SUM(Quantity) AS TotalQuantity
FROM ProductInventory
GROUP BY Supplier;

-- 5.4 Check for low stock
SELECT * FROM ProductInventory
WHERE Quantity < 30;

-- 6. Join and Subqueries
-- 6.1 Supplier with most products
SELECT Supplier, COUNT(*) AS ProductCount
FROM ProductInventory
GROUP BY Supplier
ORDER BY ProductCount DESC
LIMIT 1;

-- 6.2 Product with highest stock value
SELECT ProductName, (Quantity * UnitPrice) AS StockValue
FROM ProductInventory
ORDER BY StockValue DESC
LIMIT 1;