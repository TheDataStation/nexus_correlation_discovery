import numpy as np
from scipy.stats import pearsonr

mean = 0
std_dev = 1

np.random.seed(0)
x1 = np.random.normal(0, 1, 100)
x2 = np.random.normal(0, 1, 100)
x3 = np.random.normal(0, 1, 100)
print(x1[0], x2[0])
y = 0.6*x1 + 0.2*x2 + 0.2*x3

# Compute the Pearson correlation coefficient
corr1, _ = pearsonr(x1, y)
corr2, _ = pearsonr(x2, y)
corr3, _ = pearsonr(x3, y)

print(f'Pearson Correlation Coefficient: {corr1:.2f}, {corr2:.2f}, {corr3:.3f}')