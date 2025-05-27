# ===== VARIABLES, DATA TYPES, OPERATORS =====

# 1. Digit Sum Calculator
print("\n1. Digit Sum Calculator")
number = input("Enter a number: ")
digit_sum = sum(int(digit) for digit in number)
print(f"Sum of digits: {digit_sum}")

# 2. Reverse a 3-digit Number
print("\n2. Reverse a 3-digit Number")
number = input("Enter a 3-digit number: ")
reversed_number = number[::-1]
print(f"Reversed number: {reversed_number}")

# 3. Unit Converter
print("\n3. Unit Converter")
meters = float(input("Enter length in meters: "))
cm = meters * 100
feet = meters * 3.28084
inches = meters * 39.3701
print(f"{meters}m = {cm}cm, {feet}ft, {inches}in")

# 4. Percentage Calculator
print("\n4. Percentage Calculator")
marks = [float(input(f"Enter marks for subject {i + 1}: ")) for i in range(5)]
total = sum(marks)
average = total / 5
percentage = (total / 500) * 100

grade = 'A' if percentage >= 90 else 'B' if percentage >= 80 else 'C' if percentage >= 70 else 'D' if percentage >= 60 else 'F'

print(f"Total: {total}, Average: {average}, Percentage: {percentage}%, Grade: {grade}")

# ===== CONDITIONALS =====

# 5. Leap Year Checker
print("\n5. Leap Year Checker")
year = int(input("Enter a year: "))
is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
print(f"{year} is {'a leap year' if is_leap else 'not a leap year'}")

# 6. Simple Calculator
print("\n6. Simple Calculator")
num1 = float(input("Enter first number: "))
num2 = float(input("Enter second number: "))
op = input("Enter operator (+, -, *, /): ")

if op == '+':
    result = num1 + num2
elif op == '-':
    result = num1 - num2
elif op == '*':
    result = num1 * num2
elif op == '/':
    result = num1 / num2 if num2 != 0 else "Undefined (division by zero)"
else:
    result = "Invalid operator"

print(f"Result: {result}")

# 7. Triangle Validator
print("\n7. Triangle Validator")
a, b, c = map(float, input("Enter three side lengths (space separated): ").split())
is_triangle = (a + b > c) and (a + c > b) and (b + c > a)
print(f"These sides {'can' if is_triangle else 'cannot'} form a valid triangle")

# 8. Bill Splitter with Tip
print("\n8. Bill Splitter with Tip")
bill = float(input("Enter total bill amount: "))
people = int(input("Enter number of people: "))
tip_percent = float(input("Enter tip percentage: "))

total_with_tip = bill * (1 + tip_percent / 100)
per_person = total_with_tip / people
print(f"Each person should pay: {per_person:.2f}")

# ===== LOOPS =====

# 9. Find All Prime Numbers Between 1 and 100
print("\n9. Find All Prime Numbers Between 1 and 100")
for num in range(2, 101):
    is_prime = True
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            is_prime = False
            break
    if is_prime:
        print(num, end=' ')
print()

# 10. Palindrome Checker
print("\n10. Palindrome Checker")
text = input("Enter a string: ").lower()
is_palindrome = text == text[::-1]
print(f"'{text}' is {'a palindrome' if is_palindrome else 'not a palindrome'}")

# 11. Fibonacci Series (First N Terms)
print("\n11. Fibonacci Series (First N Terms)")
n = int(input("Enter number of terms: "))
a, b = 0, 1
for _ in range(n):
    print(a, end=' ')
    a, b = b, a + b
print()

# 12. Multiplication Table
print("\n12. Multiplication Table")
num = int(input("Enter a number: "))
for i in range(1, 11):
    print(f"{num} × {i} = {num * i}")

# 13. Number Guessing Game
print("\n13. Number Guessing Game")
import random

target = random.randint(1, 100)
while True:
    guess = int(input("Guess the number (1-100): "))
    if guess == target:
        print("Correct!")
        break
    print("Too high" if guess > target else "Too low")

# 14. ATM Machine Simulation
print("\n14. ATM Machine Simulation")
balance = 110000
while True:
    print("\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Enter choice: ")

    if choice == '1':
        amount = float(input("Enter deposit amount: "))
        balance += amount
    elif choice == '2':
        amount = float(input("Enter withdrawal amount: "))
        if amount > balance:
            print("Insufficient funds")
        else:
            balance -= amount
    elif choice == '3':
        print(f"Current balance: {balance}")
    elif choice == '4':
        break
    else:
        print("Invalid choice")

# 15. Password Strength Checker
print("\n15. Password Strength Checker")
import re

password = input("Enter password: ")
has_length = len(password) >= 8
has_digit = bool(re.search(r'\d', password))
has_upper = bool(re.search(r'[A-Z]', password))
has_symbol = bool(re.search(r'[!@#$%^&*(),.?":{}|<>]', password))

if all([has_length, has_digit, has_upper, has_symbol]):
    print("Strong password")
else:
    print("Weak password. Must have:")
    if not has_length: print("- At least 8 characters")
    if not has_digit: print("- At least one digit")
    if not has_upper: print("- At least one uppercase letter")
    if not has_symbol: print("- At least one symbol")

# 16. Find GCD (Greatest Common Divisor)
print("\n16. Find GCD (Greatest Common Divisor)")

def gcd(a, b):
    while b:
        a, b = b, a % b
    return a

num1 = int(input("Enter first number: "))
num2 = int(input("Enter second number: "))
print(f"GCD of {num1} and {num2} is {gcd(num1, num2)}")
