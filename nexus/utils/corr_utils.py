import numpy as np
import pandas as pd
import scipy.special as special


def calculate_non_mask_overlaps(x_mask, y_mask):
    """for two mask arrays (x_mask, y_mask - boolean arrays) determine the number of entries in common there would be for each
    entry if their dot product were taken
    """
    x_is_not_nan = 1 * ~x_mask
    y_is_not_nan = 1 * ~y_mask

    r = np.dot(x_is_not_nan.T, y_is_not_nan)

    return r


def nan_dot_divide(x, y):
    """helper method for use within the _fast_cov method - carry out the dot product and subsequent
    division to generate the covariance values.  For use when there are missing values.
    """
    product = np.ma.dot(x.T, y)
    divisor = calculate_non_mask_overlaps(x.mask, y.mask) - 1

    return np.ma.divide(product, divisor)


def fast_cov(x, y):
    mean_x = np.nanmean(x, axis=0)
    mean_y = np.nanmean(y, axis=0)
    return nan_dot_divide(x - mean_x, y - mean_y)


def calculate_moments_with_additional_mask(x, mask):
    """calculate the moments (y, y^2, and variance) of the columns of x, excluding masked within x, for each of the masking columns in mask
    Number of rows in x and mask must be the same.
    Args:
        x (numpy.ma.array like)
        mask (numpy array-like boolean)
    """
    non_mask_overlaps = calculate_non_mask_overlaps(x.mask, mask)

    unmask = 1.0 * ~mask

    expect_x = np.ma.dot(x.T, unmask) / non_mask_overlaps
    expect_x = expect_x.T

    expect_x_squared = np.ma.dot(np.power(x, 2.0).T, unmask) / non_mask_overlaps
    expect_x_squared = expect_x_squared.T

    var_x = (
        (expect_x_squared - np.power(expect_x, 2.0))
        * non_mask_overlaps.T
        / (non_mask_overlaps.T - 1)
    )

    return expect_x, expect_x_squared, var_x


def get_pvals(num_samples, corrs, masked):
    # Compute significance values
    ab = num_samples / 2 - 1

    def beta(corr):
        return 2 * special.btdtr(ab, ab, 0.5 * (1 - abs(np.float64(corr))))

    def beta_2(corr, ab):
        if ab <= 0:
            return 1.0
        return 2 * special.btdtr(ab, ab, 0.5 * (1 - abs(np.float64(corr))))

    if not masked:
        beta = np.vectorize(beta)
        pvals = beta(corrs)
    else:
        beta_2 = np.vectorize(beta_2)
        pvals = beta_2(corrs, ab)
    # account for small p-values rounding to 0
    pvals[pvals == 0] = np.finfo(np.float64).tiny
    return pvals


def mat_corr(
    mat1,
    mat2,
    mat1_avg,
    mat2_avg,
    o_mean1,
    o_mean2,
    names1,
    names2,
    masked,
    r_methods,
    outer_join,
):
    to_return = {}
    if not masked:
        # Subtract column means
        res1, res2 = mat1 - np.mean(mat1, axis=0), mat2 - np.mean(mat2, axis=0)
        if not outer_join and mat1_avg:
            _res1, _res2 = mat1_avg - o_mean1, mat2_avg - o_mean2
        # Sum squares across columns
        sums1 = (res1**2).sum(axis=0)
        sums2 = (res2**2).sum(axis=0)

        # Compute correlations
        res_products = np.dot(res1.T, res2)
        if "impute_avg" in r_methods and not outer_join:
            _res_products = np.dot(_res1.T, _res2)
            to_return["res_sum"] = pd.DataFrame(
                _res_products, index=names1, columns=names2
            )
        if "impute_zero" in r_methods and not outer_join:
            inner_product = np.dot(mat1.T, mat2)
            to_return["inner_product"] = pd.DataFrame(
                inner_product, index=names1, columns=names2
            )
        sum_products = np.sqrt(np.dot(sums1[:, None], sums2[None]))

        # Account for cases when stardard deviation is 0
        sum_zeros = sum_products == 0
        sum_products[sum_zeros] = 1

        corrs = res_products / sum_products

        corrs[sum_zeros] = 0

        # Store correlations in DataFrames
        num_samples = mat1.shape[0]

        pvals = get_pvals(num_samples, corrs, masked)

    else:
        cov = fast_cov(mat1, mat2)
        cov[np.isinf(cov)] = 0
        _, _, var_x = calculate_moments_with_additional_mask(mat1, mat2.mask)

        std_x = np.sqrt(var_x)
        std_x_zeros = std_x == 0
        std_x_zeros_T = std_x.T == 0
        std_x[std_x_zeros] = 1

        _, _, var_y = calculate_moments_with_additional_mask(mat2, mat1.mask)

        std_y = np.sqrt(var_y)
        std_y_zeros = std_y == 0
        std_y[std_y_zeros] = 1

        corrs = np.ma.divide(cov, std_x.T)
        corrs = np.ma.divide(corrs, std_y)
        corrs[std_x_zeros_T] = 0
        corrs[std_y_zeros] = 0

        num_samples = calculate_non_mask_overlaps(mat1.mask, mat2.mask)

        pvals = get_pvals(
            num_samples, np.ma.getdata(corrs.filled(fill_value=0), subok=False), masked
        )

    corrs = pd.DataFrame(corrs, index=names1, columns=names2)
    to_return["corrs"] = corrs
    pvals = pd.DataFrame(pvals, index=names1, columns=names2)
    to_return["p_vals"] = pvals
    return to_return
    # return corrs, pvals
