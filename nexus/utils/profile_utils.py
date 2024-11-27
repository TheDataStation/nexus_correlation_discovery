def is_num_column_valid(col_name: str):
    col_name = col_name.lower()
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
        "bbl",
        "borough",
        "block",
        "_lot",
        "interval",
        "accumulative",
        "cumulative"
    ]
    stop_words_equal = [
        "commarea",
        "commarea_n",
        "tract",
        "bbl",
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
        "wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
        "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
        "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
        "f12", "f13", "bin"
    ]
    for stop_word in stop_words_contain:
        if stop_word.lower() in col_name:
            return False
    for stop_word in stop_words_equal:
        if stop_word.lower() == col_name:
            return False
    return True
