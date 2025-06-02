-- CREATE (Add new expense)
INSERT INTO expenses (user_id, category_id, amount, description, expense_date, payment_mode)
VALUES (4, 3, 1200.00, 'Lunch at Haldiram', '2024-06-22', 'UPI');

-- READ (Get all expenses for a user)
SELECT e.expense_id, c.name AS category, e.amount, e.description, 
       DATE_FORMAT(e.expense_date, '%d-%b-%Y') AS date,
       e.payment_mode
FROM expenses e
JOIN categories c ON e.category_id = c.category_id
WHERE e.user_id = 1
ORDER BY e.expense_date DESC;

-- UPDATE (Edit an expense)
UPDATE expenses
SET amount = 3800.00, 
    description = 'Big Bazaar groceries + household items'
WHERE expense_id = 1;

-- DELETE (Remove an expense)
DELETE FROM expenses WHERE expense_id = 25;