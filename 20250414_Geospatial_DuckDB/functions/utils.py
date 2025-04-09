import duckdb
from duckdb import DuckDBPyConnection


def connect_duckdb_work(db_name: str = "work.db") -> DuckDBPyConnection:
    """
    creates a connection to a local duckdb database
    will create a new database if none exists
    default database name is "work.db"
    """
    con = duckdb.connect(f"{db_name}")
    # spatial
    con.install_extension("spatial")
    con.load_extension("spatial")
    # json
    con.install_extension("json")
    con.load_extension("json")
    return con


def connect_duckdb_postgres(DB_URL) -> DuckDBPyConnection:
    con = duckdb.connect()
    con.install_extension("postgres_scanner")
    con.load_extension("postgres_scanner")
    con.execute(f"ATTACH '{DB_URL}' AS postgres_db (TYPE POSTGRES)")
    con.install_extension("spatial")
    con.load_extension("spatial")
    return con


def get_bbox_coords(con: DuckDBPyConnection, wkt_bbox: str) -> tuple:
    """
    Returns xmin, ymin, xmax, ymax coordinates from wkt bounding box.
    Used for feeding coordinates into Overture Maps searches.
    wkt_bbox is derived from:  ST_AsText(ST_Envelope(geom))
    """
    bbox = con.sql(f"""
                   SELECT
                    ST_XMin(ST_GeomFromText('{wkt_bbox}')) as xmin,
                    ST_YMin(ST_GeomFromText('{wkt_bbox}')) as ymin,
                    ST_XMax(ST_GeomFromText('{wkt_bbox}')) as xmax,
                    ST_YMax(ST_GeomFromText('{wkt_bbox}')) as ymax
                    """).fetchall()

    xmin = bbox[0][0]
    ymin = bbox[0][1]
    xmax = bbox[0][2]
    ymax = bbox[0][3]

    return xmin, ymin, xmax, ymax


def get_overture_bldgs(con: DuckDBPyConnection,
                       tbl_name: str,
                       azure_bldg_url: str,
                       xmin: float,
                       ymin: float,
                       xmax: float,
                       ymax: float
                       ) -> None:
    """
    creates duckdb table from Overture building footprints on Azure
    """
    con.sql(f"""
            create or replace table {tbl_name} as
            SELECT
                id,
                (sources::json)[0]->>'$.dataset' as source,
                subtype,
                class,
                level,
                has_parts,
                height,
                is_underground,
                num_floors,
                num_floors_underground,
                min_height,
                min_floor,
                facade_color,
                facade_material,
                roof_material,
                roof_shape,
                roof_direction,
                roof_orientation,
                roof_color,
                roof_height,
                geometry as geom
            FROM read_parquet('{azure_bldg_url}', filename=true, hive_partitioning=1)
            WHERE bbox.xmin > {xmin} AND bbox.xmax < {xmax}
            AND bbox.ymin > {ymin} AND bbox.ymax < {ymax}
            """)

    print(f"{tbl_name=} created.")
