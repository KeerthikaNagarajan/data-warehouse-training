-- Insert users 
INSERT INTO users (name, email, phone) VALUES
('Aarav Patel', 'aarav.patel@example.com', '9876543210'),
('Priya Sharma', 'priya.sharma@example.com', '8765432109'),
('Rahul Gupta', 'rahul.gupta@example.com', '7654321098'),
('Ananya Singh', 'ananya.singh@example.com', '6543210987'),
('Vikram Joshi', 'vikram.joshi@example.com', '9432109876');

-- Insert categories
INSERT INTO categories (name, description) VALUES
('Groceries', 'Daily household groceries'),
('Transport', 'Fuel, public transport, cab services'),
('Dining', 'Restaurants, food delivery'),
('Utilities', 'Electricity, water, internet bills'),
('Shopping', 'Clothing, electronics, other purchases'),
('Entertainment', 'Movies, OTT subscriptions'),
('Health', 'Medicines, doctor visits'),
('Education', 'Books, courses, tuition fees'),
('Rent', 'House rent'),
('EMI', 'Loan payments'),
('Investments', 'SIP, stocks'),
('Others', 'Miscellaneous expenses');

-- Insert expenses 
INSERT INTO expenses (user_id, category_id, amount, description, expense_date, payment_mode) VALUES
-- Aarav's expenses
(1, 1, 3500.00, 'Big Bazaar monthly groceries', '2024-06-05', 'UPI'),
(1, 2, 2000.00, 'Petrol for car', '2024-06-07', 'Credit Card'),
(1, 3, 1500.00, 'Dinner at Mainland China', '2024-06-10', 'Debit Card'),
(1, 4, 1200.00, 'Airtel postpaid bill', '2024-06-12', 'Net Banking'),
(1, 5, 5000.00, 'New phone from Croma', '2024-06-15', 'Credit Card'),

-- Priya's expenses
(2, 1, 2500.00, 'D-Mart groceries', '2024-06-03', 'UPI'),
(2, 6, 1499.00, 'Annual Netflix subscription', '2024-06-05', 'Debit Card'),
(2, 7, 800.00, 'Apollo Pharmacy medicines', '2024-06-08', 'UPI'),
(2, 3, 750.00, 'Swiggy order', '2024-06-11', 'Wallet'),
(2, 8, 2500.00, 'Udemy course', '2024-06-18', 'Net Banking'),

-- Rahul's expenses
(3, 9, 18000.00, 'House rent', '2024-06-01', 'Net Banking'),
(3, 2, 1200.00, 'Ola rides to office', '2024-06-04', 'UPI'),
(3, 4, 1500.00, 'Electricity bill', '2024-06-10', 'Debit Card'),
(3, 10, 8500.00, 'Car EMI', '2024-06-05', 'Net Banking'),
(3, 11, 5000.00, 'Mutual fund SIP', '2024-06-10', 'Net Banking');