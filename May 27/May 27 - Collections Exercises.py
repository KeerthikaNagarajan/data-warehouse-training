# ===== LISTS =====

# 1. List of Squares
squares = [x**2 for x in range(1, 21)]
print("1. List of squares:", squares)

# 2. Second Largest Number
def second_largest(numbers):
    if len(numbers) < 2:
        return None
    largest = second = float('-inf')
    for num in numbers:
        if num > largest:
            second = largest
            largest = num
        elif num > second and num != largest:
            second = num
    return second if second != float('-inf') else None

numbers = [10, 20, 4, 45, 99]
print("\n2. Second largest number:", second_largest(numbers))

# 3. Remove Duplicates
def remove_duplicates(lst):
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

original_list = [1, 2, 2, 3, 4, 4, 5]
print("\n3. List after removing duplicates:", remove_duplicates(original_list))

# 4. Rotate List
def rotate_list(lst, k):
    if not lst:
        return lst
    k = k % len(lst)
    return lst[-k:] + lst[:-k]

original = [1, 2, 3, 4, 5]
print("\n4. Rotated list by 2 positions:", rotate_list(original, 2))

# 5. List Compression
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
doubled_evens = [x*2 for x in numbers if x % 2 == 0]
print("\n5. Doubled even numbers:", doubled_evens)

# ===== TUPLES =====

# 6. Swap Values
def swap_tuples(tuple1, tuple2):
    return tuple2, tuple1

tuple_a = (1, 2, 3)
tuple_b = ('a', 'b', 'c')
swapped = swap_tuples(tuple_a, tuple_b)
print("\n6. Swapped tuples:", swapped)

# 7. Unpack Tuples
student = ("John Doe", 20, "Computer Science", "A")
name, age, branch, grade = student
print("\n7. Unpacked tuple:", f"{name} is {age} years old, studying {branch} with grade {grade}.")

# 8. Tuple to Dictionary
def tuple_to_dict(tuple_pairs):
    return dict(tuple_pairs)

pairs = (("a", 1), ("b", 2), ("c", 3))
print("\n8. Dictionary from tuple:", tuple_to_dict(pairs))

# ===== SETS =====

# 9. Common Items
def common_elements(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    return set1 & set2

list_a = [1, 2, 3, 4, 5]
list_b = [4, 5, 6, 7, 8]
print("\n9. Common elements:", common_elements(list_a, list_b))

# 10. Unique Words in Sentence
sentence = "hello world hello python"
words = sentence.split()
unique_words = set(words)
print("\n10. Unique words:", unique_words)

# 11. Symmetric Difference
def symmetric_diff(set1, set2):
    return set1.symmetric_difference(set2)

set_a = {1, 2, 3, 4}
set_b = {3, 4, 5, 6}
print("\n11. Symmetric difference:", symmetric_diff(set_a, set_b))

# 12. Subset Checker
def is_subset(set1, set2):
    return set1.issubset(set2)

set_x = {1, 2}
set_y = {1, 2, 3, 4}
print("\n12. Is subset:", is_subset(set_x, set_y))

# ===== DICTIONARIES =====

# 13. Frequency Counter
def char_frequency(text):
    freq = {}
    for char in text:
        if char in freq:
            freq[char] += 1
        else:
            freq[char] = 1
    return freq

text = "hello world"
print("\n13. Character frequency:", char_frequency(text))

# 14. Student Grade Book
def student_grade_book():
    students = {}
    for i in range(3):
        name = input(f"\nEnter student {i+1} name: ")
        mark = float(input(f"Enter {name}'s mark: "))
        students[name] = mark

    query = input("\nEnter student name to check grade: ")
    if query in students:
        mark = students[query]
        if mark >= 90:
            grade = 'A'
        elif mark >= 75:
            grade = 'B'
        else:
            grade = 'C'
        print(f"{query}'s grade is {grade}")
    else:
        print("Student not found")

# 15. Merge Two Dictionaries
def merge_dicts(dict1, dict2):
    merged = dict1.copy()
    for key, value in dict2.items():
        if key in merged:
            merged[key] += value
        else:
            merged[key] = value
    return merged

dict_a = {'a': 1, 'b': 2}
dict_b = {'b': 3, 'c': 4}
print("\n15. Merged dictionaries:", merge_dicts(dict_a, dict_b))

# 16. Inverted Dictionary
def invert_dict(original):
    return {v: k for k, v in original.items()}

original_dict = {"a": 1, "b": 2, "c": 3}
print("\n16. Inverted dictionary:", invert_dict(original_dict))

# 17. Group Words by Length
def group_words_by_length(words):
    length_dict = {}
    for word in words:
        length = len(word)
        if length not in length_dict:
            length_dict[length] = []
        length_dict[length].append(word)
    return length_dict

word_list = ["apple", "banana", "cat", "dog", "elephant"]
print("\n17. Words grouped by length:", group_words_by_length(word_list))
