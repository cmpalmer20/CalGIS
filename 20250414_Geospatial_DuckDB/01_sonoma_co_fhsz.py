# Desc:     get population in FHSZ areas for Sonoma Co
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

# =======================================
# ENVIRONMENT
import os
import pandas as pd
import duckdb
import arcpy
from functions.utils import get_bbox_coords, get_overture_bldgs
from functions.arcpy_utils import check_repair_fc

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)


# =======================================
# MAIN

print(duckdb.__version__)       # 1.2.1

con = duckdb.connect("work_1.db")
# install spatial extension
con.install_extension("spatial")
con.load_extension("spatial")
# install H3 extension
con.execute("INSTALL h3 FROM community")
con.execute("LOAD h3")
# install httpfs extension for reading remote files
con.install_extension("httpfs")
con.load_extension("httpfs")

# -----------------------
# get Sonoma Co boundary
sonoma_co_url = "https://services1.arcgis.com/P5Mv5GY5S66M8Z1Q/arcgis/rest/services/Sonoma_County/FeatureServer/0/query?where=1%3D1&outFields=*&f=GeoJSON"

con.sql(f"""
        create or replace table county_sonoma as
        select *
        from ST_Read('{sonoma_co_url}')
        """)        # 22s

con.sql("select ST_AsText(geom) as wkt from county_sonoma").df()    # 4326
con.sql("describe county_sonoma").df()

# polyfill county with H3 level 8
# and save to table for viz
# note: H3 hexagon geometry is in 4326
con.sql("""
        create or replace table h3_8_sonoma as
        with cte_0 as (
                select ST_AsText(geom) as wkt_poly
                from county_sonoma
        )
        , cte_1 as (
                select unnest(h3_polygon_wkt_to_cells_string(wkt_poly, 8)) as hexid_8
                from cte_0
        )
        select
                hexid_8,
                ST_GeomFromText(h3_cell_to_boundary_wkt(hexid_8)) as geom
        from cte_1
        """)        # .2s

# -----------------------
# get Kontur population by H3 level 8
# https://data.humdata.org/dataset/kontur-population-united-states-of-america
# https://geodata-eu-central-1-kontur-public.s3.amazonaws.com/kontur_datasets/kontur_population_US_20231101.gpkg.gz

kontur_pop = "data/kontur_pop/kontur_population_US_20231101.csv"

# review
con.sql(f"""
        select *
        from read_csv('{kontur_pop}')
        limit 10
        """).df()

# import
con.sql(f"""
        create or replace table kontur_pop_us as
        select *
        from read_csv('{kontur_pop}')
        """)

# attribute the sonoma hexes with pop data
con.sql("""
        alter table h3_8_sonoma
        add column pop int
        """)

con.sql("""
        update h3_8_sonoma t1
        set pop = t2.population
        from kontur_pop_us t2
        where t1.hexid_8 = t2.hexid_8
        """)

con.sql("select count(*) as total from h3_8_sonoma").df()

# -----------------------
# import FHSZ data
state_fhsz_gdb = r"data\calfire_fhsz\FHSZSRA_23_3\FHSZSRA_23_3.gdb"

# don't need to specify layer if there's only one layer
# otherwise, duckdb will show topmost layer
# to get all layers use:
arcpy.env.workspace = state_fhsz_gdb
feature_classes = arcpy.ListFeatureClasses()    # FHSZSRA_23_3

fhsz_sra_lyr = f"{state_fhsz_gdb}/FHSZSRA_23_3"

# get spatial reference
lyr_desc = arcpy.Describe(fhsz_sra_lyr)
srid = lyr_desc.spatialReference.factoryCode    # 3310

# repair FHSZ
check_repair_fc(fhsz_sra_lyr)

con.sql(f"""
        describe
        from ST_Read('{state_fhsz_gdb}')
        """).df()

# import SRAs:
# keep orig geom in 3310,
# add another geom transformed to 4326
con.sql(f"""
        create or replace table fhsz_sra as
        select
            SRA,
            FHSZ::int as FHSZ,
            FHSZ_Description,
            Shape as geom_3310,
            ST_Transform(Shape, 'EPSG:3310', 'EPSG:4326', always_xy := true) as geom_4326
        from ST_Read('{state_fhsz_gdb}')
        """)        # 4.8s

# import LRAs
fhsz_lra_shp = r"data\calfire_fhsz\FHSZLRA25_Phase2_v1\Shapefile\FHSZLRA25_Phase2_v1.shp"

con.sql(f"""
        describe
        from ST_Read('{fhsz_lra_shp}')
        """).df()

# import LRAs:
# keep orig geom in 3310,
# add another geom transformed to 4326
con.sql(f"""
        create or replace table fhsz_lra as
        select
            SRA,
            FHSZ,
            FHSZ_Descr,
            geom as geom_3310,
            ST_Transform(geom, 'EPSG:3310', 'EPSG:4326', always_xy := true) as geom_4326
        from ST_Read('{fhsz_lra_shp}')
        """)        # 8s

con.sql("select ST_AsText(geom_3310) as wkt from fhsz_lra limit 1").df()

# -----------------------
# update the H3 level 8 table, attribute with SRA & LRA rankings
con.sql("""
        alter table h3_8_sonoma
        add column sra text;

        alter table h3_8_sonoma
        add column lra text;
        """)

con.sql("""
        update h3_8_sonoma t1
        set sra = t2.FHSZ_Description
        from fhsz_sra t2
        where ST_Intersects(ST_Centroid(t1.geom), t2.geom_4326)
        """)        # 10s

con.sql("""
        update h3_8_sonoma t1
        set lra = t2.FHSZ_Descr
        from fhsz_lra t2
        where ST_Intersects(ST_Centroid(t1.geom), t2.geom_4326)
        """)        # 1.7s

# get total population by SRA, LRA
con.sql("""
        select sra, lra, sum(pop) as total_pop
        from h3_8_sonoma
        group by all
        order by total_pop desc
        """).df()

# -----------------------
# get distance from home point (One Doubletree Drive)
# to each of the closest LRA rankings (Moderate, High, Very High)
# also include a line representing this nearest distance

# note_1:
# DuckDB doesn't support the geography datatype,
# so current options are:
#   > geom_4326 with ST_Distance_Sphere(point, point)
#   > geom_projected with ST_Distance(geom, geom)
# Since we want to find distance between a point and a polygon,
# our only option is to use ST_Distance with projected geometries.

# note_2:
# "distinct on" in the final select statement limits the results to
# one row for each distinct value of FHSZ_Descr

con.sql("""
        create or replace table fhsz_near_dist as
        with cte0 as (
            select
                ST_AsText(ST_Point(-122.708061, 38.365655)) as home_wkt,
                ST_Transform(
                    ST_Point(-122.708061, 38.365655),
                    'EPSG:4326',
                    'EPSG:3310',
                    always_xy := true
                ) as home_pt_3310
        ),
        cte1 as (
            select
                home_wkt,
                FHSZ_Descr,
                round(ST_Distance(home_pt_3310, geom_3310)::numeric, 2) as dist_m,
                round(dist_m * 0.0006213712, 2) as dist_mi,
                ST_ShortestLine(home_pt_3310, geom_3310) as line_geom
            from fhsz_lra, cte0
            where 1=1
            and ST_DWithin(home_pt_3310, geom_3310, 15000) -- 10 miles
            and FHSZ_Descr in ('Moderate', 'High', 'Very High')
        )
        select
            distinct on (FHSZ_Descr)
            home_wkt,
            FHSZ_Descr,
            dist_m::float as dist_m,
            dist_mi::float as dist_mi,
            line_geom
        from cte1
        order by dist_m
""")        # 0s


# -----------------------
# get building footprints for Sonoma Co from Overture Maps
# Overture releases listed here: https://docs.overturemaps.org/release/latest/
# Overture building schema: https://docs.overturemaps.org/guides/buildings/

# install azure extension
con.install_extension("azure")
con.load_extension("azure")
# set azure connection string
con.execute("SET azure_storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=overturemapswestus2;AccountKey=;EndpointSuffix=core.windows.net';")

azure_overture_buildings = "azure://release/2025-03-19.0/theme=buildings/type=building/*"

# get bbox for Sonoma Co boundary
wkt_bbox = con.sql("""
                    select ST_AsText(ST_Envelope(geom)) as wkt_bbox
                    from county_sonoma
                    """).fetchone()[0]

# get bbox vars from wkt_poly
xmin, ymin, xmax, ymax = get_bbox_coords(con, wkt_bbox)

tbl_bldgs = "bldgs_sonoma"

# create duckdb table from Overture building footprints on Azure
get_overture_bldgs(con, tbl_bldgs, azure_overture_buildings, xmin, ymin, xmax, ymax) # 4m

# review
con.sql(f"select count(*) as total from {tbl_bldgs}").df()          # 346,306 rows
con.sql(f"select * exclude geom from {tbl_bldgs} limit 10").df()
con.sql(f"select ST_AsText(geom) from {tbl_bldgs} limit 1").df()    # 4326
con.sql(f"describe {tbl_bldgs}").df()


# --------------------------
# get building totals per level 8 hex

# first, generate hexid_8 for each building
con.sql("""
        alter table bldgs_sonoma
        add column hexid_8 text
        """)

con.sql("""
        update bldgs_sonoma
        set hexid_8 = h3_latlng_to_cell_string(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 8)
        """)    # 4.6s

# second, update county hexagon table with bldg totals
con.sql("""
        alter table h3_8_sonoma
        add column bldgs int
        """)

con.sql("""
        with cte as (
            select hexid_8, count(*) as total_bldgs
            from bldgs_sonoma
            group by all
        )
        update h3_8_sonoma t1
        set bldgs = total_bldgs
        from cte t2
        where t1.hexid_8 = t2.hexid_8
        """)        # 0.1s

# get total pop, bldgs by SRA, LRA
con.sql("""
        select sra, lra, sum(pop) as total_pop, sum(bldgs) as total_bldgs
        from h3_8_sonoma
        group by all
        order by total_bldgs desc
        """).df()

# --------------------------
# export data

out_dir = "data_out"
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

# totals report -> CSV
out_csv = "sra_lra_totals.csv"
con.sql(f"""
    copy (
        select sra, lra, sum(pop) as total_pop, sum(bldgs) as total_bldgs
        from h3_8_sonoma
        group by all
        order by total_bldgs desc
        )
    to '{out_dir}/{out_csv}'
    (header, delimiter ',')
        """)

# county hexagons -> SHP
out_shp = "county_sonoma_hex8.shp"
con.sql(f"""
        copy h3_8_sonoma
        to '{out_dir}/{out_shp}'
        with (FORMAT GDAL, DRIVER 'ESRI Shapefile', SRS)
        """)








# checkpoint the database
# i.e., commit the changes recorded in the wal file to the database file
con.close()

# EOF
