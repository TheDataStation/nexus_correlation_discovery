# demonstrate the idea of saving correlation calculation
import pandas as pd
import random
import numpy as np
from scipy.stats import pearsonr, spearmanr

random.seed(124)

# Generate 20 random integers
random_integers1 = [random.randint(1, 100) for _ in range(20)]
random_integers2 = [random.randint(1, 100) for _ in range(20)]

table1_data = {"id1": range(1, 21), "col1": random_integers1}
table1 = pd.DataFrame(table1_data)

# Create the second table
table2_data = {"id2": range(11, 31), "col2": random_integers2}
table2 = pd.DataFrame(table2_data)

# Perform an outer join on the common numerical column
result = pd.merge(table1, table2, left_on="id1", right_on="id2", how="outer")
outer_df = result[["col1", "col2"]]
result = pd.merge(table1, table2, left_on="id1", right_on="id2", how="inner")
inner_df = result[["col1", "col2"]]


def impute_zero():
    df_filled = outer_df.fillna(0)
    print(f"ground truth corr of filling zero: {df_filled.corr().iloc[0, 1]}")
    # use inner join result and column stats to calculate corr
    sum_col1 = np.sum(table1["col1"])
    sum_col2 = np.sum(table2["col2"])
    sum_square_col1 = np.sum(table1["col1"] ** 2)
    sum_square_col2 = np.sum(table2["col2"] ** 2)
    x_values = inner_df["col1"]
    y_values = inner_df["col2"]
    result = np.sum((x_values) * (y_values))
    n = len(table1) + len(table2) - len(inner_df)
    print(n * sum_square_col1 - (sum_col1) ** 2)
    r = (n * result - sum_col1 * sum_col2) / (
        np.sqrt(n * sum_square_col1 - (sum_col1) ** 2)
        * np.sqrt(n * sum_square_col2 - (sum_col2) ** 2)
    )
    print(r)


def impute_avg():
    # calculate groundtruth corr of imputing average.
    df_filled = outer_df.fillna(outer_df.mean())
    print(f"ground truth corr of filling mean: {df_filled.corr().iloc[0, 1]}")

    # use inner join result and column stats to calculate corr
    column_avg1 = np.mean(table1["col1"])
    diff_sum1 = np.sum((table1["col1"] - column_avg1) ** 2)

    column_avg2 = table2["col2"].mean()
    diff_sum2 = ((table2["col2"] - column_avg2) ** 2).sum()

    x_values = inner_df["col1"]
    y_values = inner_df["col2"]
    avg_x = np.mean(x_values)
    avg_y = np.mean(y_values)
    result = np.sum((x_values - column_avg1) * (y_values - column_avg2))

    print(result / np.sqrt((diff_sum1 * diff_sum2)))

    # Create a sample row
    row = np.array([1, 2, 3])

    # Specify the number of repetitions
    repetitions = 5

    # Duplicate the row for the specified number of times
    matrix = np.repeat([row], repetitions, axis=0)
    matrix2 = np.zeros((5, 3))

    # Print the matrix
    print(matrix2 - matrix)

    data = {"A": [1, np.nan, 3], "B": [np.nan, 5, np.nan], "C": [7, np.nan, 9]}
    df = pd.DataFrame(data)
    print(df)
    # Create another matrix with values to fill NaN
    fill_values = np.array([10, 20, 30])

    # Fill NaN values in the DataFrame using values from the other matrix
    df_filled = df.fillna(fill_values)

    # Print the filled DataFrame
    print(df_filled)

def get_corr_numpy(df1, df2, corr_type='spearman'):
    df1, df2 = df1.fillna(0), df2.fillna(0)
    col_num1, col_num2 = len(df1.columns), len(df2.columns)
    mat1 = np.transpose(df1.to_numpy())
    mat2 = np.transpose(df2.to_numpy())
    for i in range(col_num1):
        for j in range(col_num2):
            col1, col2 = mat1[i], mat2[j]
            if corr_type == 'pearson':
                corr, p_val = pearsonr(col1, col2)
            elif corr_type == 'spearman':
                corr, p_val = spearmanr(col1, col2)
            print(df1.columns[i], df2.columns[j], round(corr, 5), p_val)

if __name__ == '__main__':
    df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    df2 = pd.DataFrame({'C': [2, 5, 7], 'D': [4, 5, 6]})
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [2, 5, 7], 'D': [4, 5, 6]})
    get_corr_numpy(df1, df2, 'pearson')
    print(df.corr(method='pearson'))
