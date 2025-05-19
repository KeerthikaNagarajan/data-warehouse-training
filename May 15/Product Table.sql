-- Create Product table
CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10,2),
    StockQuantity INT,
    Supplier VARCHAR(100)
);

-- Insert initial data
INSERT INTO Product VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

-- 1. CRUD Operations

-- Add new product
INSERT INTO Product VALUES
(6, 'Gaming Keyboard', 'Electronics', 3500, 150, 'TechMart');

-- Update product price (10% increase for Electronics)
UPDATE Product
SET Price = Price * 1.10
WHERE Category = 'Electronics';

-- Delete product (Desk Lamp)
DELETE FROM Product
WHERE ProductID = 4;

-- Read all products sorted by Price (descending)
SELECT * FROM Product
ORDER BY Price DESC;

-- 2. Sorting and Filtering

-- Sort by stock quantity (ascending)
SELECT * FROM Product
ORDER BY StockQuantity ASC;

-- Filter Electronics products
SELECT * FROM Product
WHERE Category = 'Electronics';

-- Electronics products priced above 5000
SELECT * FROM Product
WHERE Category = 'Electronics' AND Price > 5000;

-- Electronics OR price < 2000
SELECT * FROM Product
WHERE Category = 'Electronics' OR Price < 2000;

-- 3. Aggregation and Grouping

-- Total stock value
SELECT SUM(Price * StockQuantity) AS TotalStockValue
FROM Product;

-- Average price by category
SELECT Category, AVG(Price) AS AveragePrice
FROM Product
GROUP BY Category;

-- Count of GadgetHub products
SELECT COUNT(*) AS GadgetHubProducts
FROM Product
WHERE Supplier = 'GadgetHub';

-- 4. Conditional and Pattern Matching

-- Products containing "Wireless"
SELECT * FROM Product
WHERE ProductName LIKE '%Wireless%';

-- Products from TechMart or GadgetHub
SELECT * FROM Product
WHERE Supplier IN ('TechMart', 'GadgetHub');

-- Products priced between 1000 and 20000
SELECT * FROM Product
WHERE Price BETWEEN 1000 AND 20000;

-- 5. Advanced Queries

-- Products with above-average stock
SELECT * FROM Product
WHERE StockQuantity > (SELECT AVG(StockQuantity) FROM Product);

-- Top 3 most expensive products
SELECT * FROM Product
ORDER BY Price DESC
LIMIT 3;

-- Duplicate suppliers
SELECT Supplier, COUNT(*) AS Count
FROM Product
GROUP BY Supplier
HAVING COUNT(*) > 1;

-- Category summary
SELECT 
    Category,
    COUNT(*) AS NumberOfProducts,
    SUM(Price * StockQuantity) AS TotalStockValue
FROM Product
GROUP BY Category;

-- 6. Supplier and Category Analysis

-- Supplier with most products
SELECT Supplier, COUNT(*) AS ProductCount
FROM Product
GROUP BY Supplier
ORDER BY ProductCount DESC
LIMIT 1;

-- Most expensive product per category
SELECT p.Category, p.ProductName, p.Price
FROM Product p
INNER JOIN (
    SELECT Category, MAX(Price) AS MaxPrice
    FROM Product
    GROUP BY Category
) mp ON p.Category = mp.Category AND p.Price = mp.MaxPrice;
