DELIMITER //
CREATE PROCEDURE GetMonthlyExpensesByCategory(
    IN user_id_param INT, 
    IN month_param INT, 
    IN year_param INT
)
BEGIN
    SELECT 
        c.name AS category,
        SUM(e.amount) AS total_amount,
        COUNT(e.expense_id) AS transaction_count,
        ROUND(SUM(e.amount) * 100 / (
            SELECT SUM(amount) 
            FROM expenses 
            WHERE user_id = user_id_param 
            AND MONTH(expense_date) = month_param 
            AND YEAR(expense_date) = year_param
        ), 2) AS percentage
    FROM expenses e
    JOIN categories c ON e.category_id = c.category_id
    WHERE e.user_id = user_id_param
    AND MONTH(e.expense_date) = month_param
    AND YEAR(expense_date) = year_param
    GROUP BY c.name
    ORDER BY total_amount DESC;
END //
DELIMITER ;

-- Example usage
CALL GetMonthlyExpensesByCategory(1, 6, 2024);