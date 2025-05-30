# Section 1: Python Basics & Control Flow
# Q1
for num in range(11, 50, 2):
    print(num)

# Q2
year = int(input("Enter a year: "))
if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
    print(f"{year} is a leap year")
else:
    print(f"{year} is not a leap year")

# Q3
text = input("Enter a string: ")
count = text.lower().count('a')
print(f"The letter 'a' appears {count} times")

# Section 2: Collections (Lists, Tuples, Sets, Dicts)
# Q4
keys = ['a', 'b', 'c']
values = [100, 200, 300]
result = dict(zip(keys, values))
print("Created dictionary:", result)

# Q5
salaries = [50000, 60000, 55000, 70000, 52000]
avg = sum(salaries)/len(salaries)
print(f"""
Max salary: {max(salaries)}
Salaries above average ({avg:.2f}): {[s for s in salaries if s > avg]}
Sorted descending: {sorted(salaries, reverse=True)}
""")

# Q6
a = [1, 2, 3, 4]
b = [3, 4, 5, 6]
print("Unique elements from list a:", set(a))
print("Difference a - b:", set(a) - set(b))

# Section 3: Functions & Classes
# Q7
class Employee:
    def __init__(self, name, salary):
        self.name = name
        self.salary = salary

    def display(self):
        print(f"Name: {self.name}, Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary > 60000

# Q8
class Project(Employee):
    def __init__(self, name, salary, project_name, hours_allocated):
        super().__init__(name, salary)
        self.project_name = project_name
        self.hours_allocated = hours_allocated

# Q9
e1 = Employee("Ali", 50000)
e2 = Employee("Sara", 70000)
e3 = Employee("Neha", 60000)

for emp in [e1, e2, e3]:
    print(f"{emp.name} is high earner: {emp.is_high_earner()}")

# Section 4: File Handling
# Q10
import pandas as pd
df = pd.read_csv("employee.csv")
it_employees = df[df['Department'] == 'IT']['Name']

with open("it_employees.txt", "w") as file:
    for name in it_employees:
        file.write(name + "\n")

# Q11
with open("it_employees.txt", "r") as file:
    content = file.read()
    word_count = len(content.split())
print("Word count:", word_count)

# Section 5: Exception Handling
# Q12
try:
    num = float(input("Enter a number: "))
    print("Square:", num ** 2)
except ValueError:
    print("Invalid input! Please enter a number.")

# Q13
def divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Cannot divide by zero"

print(divide(10, 0))

# Section 6: Pandas – Reading & Exploring CSVs
# Q14
employees = pd.read_csv("employee.csv")
projects = pd.read_csv("projects.csv")

# Q15
print(employees.head(2))
print(employees['Department'].unique())
print(employees.groupby('Department')['Salary'].mean())

# Q16
from datetime import datetime

employees['JoiningDate'] = pd.to_datetime(employees['JoiningDate'])
employees['TenureInYears'] = datetime.now().year - employees['JoiningDate'].dt.year
print(employees[['Name', 'TenureInYears']])

#  Section 7: Data Filtering, Aggregation, and Sorting
# Q17
filtered = employees[(employees['Department'] == 'IT') & (employees['Salary'] > 60000)]
print(filtered)

# Q18
dept_group = employees.groupby('Department').agg(
    EmployeeCount=('EmployeeID', 'count'),
    TotalSalary=('Salary', 'sum'),
    AverageSalary=('Salary', 'mean')
)
print(dept_group)

# Q19
sorted_employees = employees.sort_values(by='Salary', ascending=False)
print(sorted_employees)

# Section 8: Joins & Merging
# Q20
merged = pd.merge(employees, projects, on='EmployeeID')
print(merged)

# Q21
merged_all = pd.merge(employees, projects, on='EmployeeID', how='left')
no_projects = merged_all[merged_all['ProjectID'].isna()]
print(no_projects)

# Q22
merged_all = pd.merge(employees, projects, on='EmployeeID', how='inner')
merged_all['TotalCost'] = merged_all['HoursAllocated'] * (merged_all['Salary'] / 160)
print(merged_all[['EmployeeID', 'Name', 'ProjectName', 'TotalCost']])



