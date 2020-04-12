# DROP TABLES

cities_table_drop = "drop table if exists dim_cities"
weather_table_drop = "drop table if exists fact_weather"
employment_table_drop = "drop table if exists fact_employment"

# CREATE TABLES

cities_table_create = """
    create table if not exists dim_cities (
        city_id int identity(1, 1),
        city varchar not null,
        state varchar not null,
        state_code varchar not null,
        country varchar not null,
        latitude double precision not null,
        longitude double precision not null,
        density double precision null,
        primary key (city_id)
    )
    diststyle all;
"""

weather_table_create = """
    create table if not exists fact_weather (
        avg_temp double precision not null,
        avg_temp_uncertainty double precision null,
        city varchar not null,
        state varchar not null,
        date date not null,
        primary key (date, state, city)
    )
    distkey (state)
    sortkey (date);
"""

employment_table_create = """
    create table if not exists fact_employment (
        people_employed int null,
        state varchar not null,
        year int not null,
        month int not null,
        primary key (state, year, month)
    )
    distkey (state)
    compound sortkey (year, month);
"""

# QUERY LISTS

create_table_queries = [
    cities_table_create,
    weather_table_create,
    employment_table_create,
]
drop_table_queries = [cities_table_drop, weather_table_drop, employment_table_drop]
