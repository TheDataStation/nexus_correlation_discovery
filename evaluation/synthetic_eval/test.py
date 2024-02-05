# import numpy as np
# from scipy.stats import pearsonr

# mean = 0
# std_dev = 1

# np.random.seed(0)
# x1 = np.random.normal(0, 1, 1000)
# x2 = np.random.normal(0, 1, 1000)
# x3 = np.random.normal(0, 1, 1000)
# print(x1[0], x2[0])
# y = x1 + -x2 + x3

# # Compute the Pearson correlation coefficient
# corr1, _ = pearsonr(x1, y)
# corr2, _ = pearsonr(x2, y)
# corr3, _ = pearsonr(x3, y)

# print(f'Pearson Correlation Coefficient: {corr1:.2f}, {corr2:.2f}, {corr3:.3f}')


import random

# Number of elements in the set
n = 4

# Generate n-1 small random numbers
numbers = [random.random() for _ in range(n-1)]

# Add a dominant number to the set
dominant_number = max(numbers) * 2
numbers.append(dominant_number)

# Sum of numbers
total = sum(numbers)

# Normalize the numbers
normalized_numbers = sorted([number / total for number in numbers], reverse=True)

print(normalized_numbers)