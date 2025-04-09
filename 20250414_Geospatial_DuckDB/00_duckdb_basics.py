# Desc:     duckdb basics
# Author:   claire
# Created:  04/14/2025
# env:      arc_dev_env
# notes:    https://duckdb.org/docs/stable/clients/python/overview.html
#           https://duckdb.org/docs/stable/extensions/spatial/functions.html
#           https://duckdb.org/community_extensions/list_of_extensions.html
#           https://duckdb.org/community_extensions/extensions/h3.html
#           https://duckdb.org/docs/stable/dev/release_calendar.html
#           https://duckdb.org/news/
#
# pip install duckdb
# pip install --upgrade duckdb
#
# VSCode:
# "jupyter.interactiveWindow.textEditor.executeSelection": true,
# Jupyter: Run Selection/Line in Interactive Window >> Shift + Enter

# =======================================
# ENVIRONMENT
import os
import pandas as pd
import duckdb

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

# =======================================
# MAIN
print(duckdb.__version__)       # 1.2.1

# create in-memory connection
con = duckdb.connect()

# create db file "work.db" if not exists, otherwise just connect to "work.db"
con = duckdb.connect("work.db")

# ----------------------------
# view built-in extensions
con.sql("select * from duckdb_extensions()").df()

# view schema
con.sql("describe from duckdb_extensions()").df()

con.sql("""
        select
            extension_name,
            loaded,
            installed,
            description,
            installed_from
        from duckdb_extensions()
        """).df()

# explicitly install & load core extensions
# install spatial
con.install_extension("spatial")
con.load_extension("spatial")

# install excel
con.install_extension("excel")
con.load_extension("excel")

# install json
con.install_extension("json")
con.load_extension("json")

# install DuckDB Local UI
con.install_extension("ui")
con.load_extension("ui")
con.execute("call start_ui()")

# ----------------------------
in_shp = r"data\calfire_fhsz\FHSZLRA25_Phase2_v1\Shapefile\FHSZLRA25_Phase2_v1.shp"

con.sql(f"select * from ST_Read('{in_shp}') limit 5").df()

con.sql("""
    SELECT *
    FROM 'https://raw.githubusercontent.com/duckdb/duckdb-web/main/data/weather.csv'
    limit 5
    """).df()


# ----------------------------
# install spatial
con.install_extension("spatial")
con.load_extension("spatial")

con.sql("""
    select ST_AsText(ST_Point(-122.708061, 38.365655)) as home_wkt
    """).df()                         # POINT (-122.708061 38.365655)

# distance from hotel to Transamerica Pyramid
# https://github.com/duckdb/duckdb_spatial/issues/16
# Additionally, while most functions in the spatial extension
# are ambivalent to the axis order or projection (as they almost all work in planar space)
# the _spheroid functions expect their input to be in lat/lon EPSG:4326 (WGS84).
con.sql("""
    select ST_Distance_Sphere(
            ST_FlipCoordinates(ST_Point(-122.708061, 38.365655)),
            ST_FlipCoordinates(ST_Point(-122.4027953, 37.795257))
        ) * 0.000621371 as dist_mi
    """).df()                           # 42.76 miles

con.sql("""
    select ST_Distance_Spheroid(
            ST_FlipCoordinates(ST_Point(-122.708061, 38.365655)),
            ST_FlipCoordinates(ST_Point(-122.4027953, 37.795257))
        ) * 0.000621371 as dist_mi
    """).df()                           # 42.72 miles







# close connection
con.close()
