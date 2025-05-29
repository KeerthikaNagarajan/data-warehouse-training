## Functions Exercises
### 1. Prime Number Checker
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

for num in range(1, 101):
    if is_prime(num):
        print(num, end=' ')

### 2. Temperature Converter
def convert_temp(value, unit):
    if unit.upper() == 'C':
        return (value * 9/5) + 32  
    elif unit.upper() == 'F':
        return (value - 32) * 5/9  
    else:
        return "Invalid unit"

print(convert_temp(100, 'C'))  
print(convert_temp(212, 'F'))  


### 3. Recursive Factorial Function
def factorial(n):
    if n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)

print(factorial(5))  

## Classes Exercises
### 4. Class: Rectangle
class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width
    
    def area(self):
        return self.length * self.width
    
    def perimeter(self):
        return 2 * (self.length + self.width)
    
    def is_square(self):
        return self.length == self.width

rect = Rectangle(5, 5)
print(rect.is_square())  

### 5. Class: BankAccount
class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance
    
    def deposit(self, amount):
        self.balance += amount
    
    def withdraw(self, amount):
        if amount > self.balance:
            print("Insufficient balance")
        else:
            self.balance -= amount
    
    def get_balance(self):
        return self.balance

account = BankAccount("John Doe", 1000)
account.withdraw(500)
print(account.get_balance())  

### 6. Class: Book
class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock
    
    def sell(self, quantity):
        if quantity > self.in_stock:
            raise ValueError("Not enough stock available")
        self.in_stock -= quantity
        return quantity * self.price

book = Book("Python 101", "A. Programmer", 29.99, 10)
try:
    book.sell(5)  
    book.sell(10)  
except ValueError as e:
    print(e)

### 7. Student Grade System
class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks
    
    def average(self):
        return sum(self.marks) / len(self.marks)
    
    def grade(self):
        avg = self.average()
        if avg >= 90:
            return 'A'
        elif avg >= 80:
            return 'B'
        elif avg >= 70:
            return 'C'
        else:
            return 'F'

student = Student("Alice", [85, 90, 78, 92])
print(student.grade())  

## Inheritance Exercises
### 8. Person → Employee
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

class Employee(Person):
    def __init__(self, name, age, emp_id, salary):
        super().__init__(name, age)
        self.emp_id = emp_id
        self.salary = salary
    
    def display_info(self):
        print(f"Name: {self.name}, Age: {self.age}, ID: {self.emp_id}, Salary: ${self.salary}")

emp = Employee("John Smith", 30, "E123", 75000)
emp.display_info()

### 9. Vehicle → Car, Bike
class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels
    
    def description(self):
        return f"{self.name} with {self.wheels} wheels"

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type
    
    def description(self):
        return f"{super().description()}, fuel type: {self.fuel_type}"

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared
    
    def description(self):
        return f"{super().description()}, {'geared' if self.is_geared else 'non-geared'}"

car = Car("Toyota", 4, "Petrol")
bike = Bike("Mountain Bike", 2, True)
print(car.description())
print(bike.description())

### 10. Polymorphism with Animals
class Animal:
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):
        return "Woof!"

class Cat(Animal):
    def speak(self):
        return "Meow!"

class Cow(Animal):
    def speak(self):
        return "Moo!"

animals = [Dog(), Cat(), Cow()]
for animal in animals:
    print(animal.speak())
