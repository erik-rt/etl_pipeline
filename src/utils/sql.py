class sql():
    """Class that holds the SQL queries for Redshift."""

    drop_query = 'DROP TABLE IF EXISTS'
    create_query = 'CREATE TABLE IF NOT EXISTS'
    table_attributes = {
        'geonames': '''(
            geonameid int,
            asciiname varchar,
            latitude float,
            longitude float,
            feature_class varchar,
            feature_code varchar,
            country_code varchar,
            dem int,
            timezone varchar
        )''',
        'country_info': '''(
            ISO varchar,
            fips varchar,
            country varchar,
            capital varchar,
            area_sq_km int,
            population int,
            tld varchar,
            currency_name varchar,
            phone int,
            languages varchar,
            geonameid int,
            neighbours varchar
        )''',
        'geo_info': '''(
            geoname_id int,
            asciiname varchar,
            latitude float,
            longitude float,
            feature_class varchar,
            country varchar,
            country_code varchar,
            neighbours varchar
        )'''
    }
