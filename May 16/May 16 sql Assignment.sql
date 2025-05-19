-- Create the Book table
DROP TABLE IF EXISTS Book;
CREATE TABLE Book (
    BookID INT PRIMARY KEY,
    Title VARCHAR(100) NOT NULL,
    Author VARCHAR(100) NOT NULL,
    Genre VARCHAR(50),
    Price INT,
    PublishedYear INT,
    Stock INT
);

-- Insert data
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock) VALUES
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);

-- CRUD OPERATIONS
-- 1. Add a new book
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock)
VALUES (6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);

-- 2. Update book price (Self-Help +50)
UPDATE Book
SET Price = Price + 50
WHERE Genre = 'Self-Help';

-- 3. Delete a book (BookID = 4)
DELETE FROM Book
WHERE BookID = 4;

-- 4. Read all books (sorted by Title ASC)
SELECT * FROM Book
ORDER BY Title ASC;

-- SORTING AND FILTERING
-- 5. Sort by price (DESC)
SELECT * FROM Book
ORDER BY Price DESC;

-- 6. Filter by genre (Fiction)
SELECT * FROM Book
WHERE Genre = 'Fiction';

-- 7. Filter with AND (Self-Help AND price > 400)
SELECT * FROM Book
WHERE Genre = 'Self-Help' AND Price > 400;

-- 8. Filter with OR (Fiction OR published after 2000)
SELECT * FROM Book
WHERE Genre = 'Fiction' OR PublishedYear > 2000;

-- AGGREGATION AND GROUPING 
-- 9. Total stock value (Price * Stock)
SELECT 
    Title, 
    Price, 
    Stock, 
    (Price * Stock) AS TotalStockValue 
FROM Book;

-- 10. Average price by genre
SELECT 
    Genre, 
    AVG(Price) AS AveragePrice
FROM Book
GROUP BY Genre;

-- 11. Total books by author (Paulo Coelho)
SELECT 
    COUNT(*) AS BooksByPauloCoelho
FROM Book
WHERE Author = 'Paulo Coelho';

-- CONDITIONAL AND PATTERN MATCHING
-- 12. Find books with 'The' in title
SELECT * FROM Book
WHERE Title LIKE '%The%';

-- 13. Filter by multiple conditions (Yuval AND price < 600)
SELECT * FROM Book
WHERE Author = 'Yuval Noah Harari' AND Price < 600;

-- 14. Price range (300-500)
SELECT * FROM Book
WHERE Price BETWEEN 300 AND 500;

--  ADVANCED QUERIES 
-- 15. Top 3 most expensive books
SELECT * FROM Book
ORDER BY Price DESC
LIMIT 3;

-- 16. Books published before 2000
SELECT * FROM Book
WHERE PublishedYear < 2000;

-- 17. Count books by genre
SELECT 
    Genre, 
    COUNT(*) AS NumberOfBooks
FROM Book
GROUP BY Genre;

-- 18. Find duplicate titles
SELECT 
    Title, 
    COUNT(*) AS DuplicateCount
FROM Book
GROUP BY Title
HAVING COUNT(*) > 1;

-- 19. Author with most books
SELECT 
    Author, 
    COUNT(*) AS BookCount
FROM Book
GROUP BY Author
ORDER BY BookCount DESC
LIMIT 1;

-- 20. Oldest book by genre
SELECT 
    b.Genre, 
    b.Title, 
    b.PublishedYear
FROM Book b
INNER JOIN (
    SELECT 
        Genre, 
        MIN(PublishedYear) AS OldestYear
    FROM Book
    GROUP BY Genre
) oldest ON b.Genre = oldest.Genre AND b.PublishedYear = oldest.OldestYear;
