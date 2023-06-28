def is_num_column_valid(col_name):
    stop_words_contain = [
        "id",
        "primary_key",
        "longitude",
        "latitude",
        "ward",
        "date",
        "zipcode",
        "zip_code",
        "_zip",
        "street_number",
        "street_address",
        "district",
        "coordinate",
        "community_area",
        "_no",
        "_year",
        "_day",
        "_month",
        "_hour",
        "_number",
        "_code",
        "census_tract",
        "address",
        "x_coord",
        "y_coord",
    ]
    stop_words_equal = [
        "census",
        "permit_",
        "beat",
        "zip",
        "year",
        "week_number",
        "ssa",
        "license_",
        "day_of_week",
        "police_sector",
        "police_beat",
        "license",
        "month",
        "hour",
        "day",
        "lat",
        "long",
        "mmwr_week",
        "zip4",
        "phone",
        "x",
        "y",
        "area",
    ]
    for stop_word in stop_words_contain:
        if stop_word in col_name:
            return False
    for stop_word in stop_words_equal:
        if stop_word == col_name:
            return False
    return True
