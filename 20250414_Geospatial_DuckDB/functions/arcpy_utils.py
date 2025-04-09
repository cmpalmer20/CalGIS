import os
import time
import arcpy
from typing import List


def curve_checker(fc: str) -> List[int]:
    """
    Checks if the input feature class has curves in geometries.

    Parameters:
    fc (str): The path to the feature class to check.

    Returns:
    List[int]: A list of object IDs that have curves in their geometries.
    """
    curved_oids = []
    with arcpy.da.SearchCursor(fc, ["OID@", "SHAPE@JSON"]) as curs:
        for oid, json in curs:
            if "curve" in json:
                curved_oids.append(oid)
    curve_count = len(curved_oids)
    if curve_count > 0:
        print(f"{os.path.basename(fc)} has {curve_count} curves!")
        print("    ", curved_oids)
    return curved_oids


def densify_curve(fc: str, oids_list: List[int]) -> None:
    """
    Densifies the geometry for the specified object IDs in the feature class.

    Parameters:
    fc (str): The path to the feature class.
    oids_list (List[int]): A list of object IDs to densify.
    """
    oid = arcpy.Describe(fc).OIDFieldName
    list_str = ",".join([str(e) for e in oids_list])
    query = f"{oid} IN ({list_str})"
    with arcpy.da.UpdateCursor(fc, ["OID@", "SHAPE@"], query) as cur:
        for row in cur:
            if row[0] in oids_list:
                shape = row[1].densify("ANGLE", 10000, 0.174533)
                row[1] = shape
                cur.updateRow(row)
                print(f"{row[0]} has been densified")
    curved_oids = curve_checker(fc)
    if len(curved_oids) == 0:
        print(f"{os.path.basename(fc)} has no curves")


def check_repair_fc(fc: str) -> None:
    """
    Checks and repairs the feature class for geometry issues and curves.

    Parameters:
    fc (str): The path to the feature class to check and repair.
    """
    # check & repair geometry
    print(time.ctime(), " checking geometry...")
    try:
        arcpy.CheckGeometry_management(fc, f"{fc}_checkgeo", "OGC")
        # print(arcpy.GetMessages())
    except Exception as ex:
        print(time.ctime(), f" {os.path.basename(fc)} check failed")
        print(arcpy.GetMessages())

    print(time.ctime(), " repairing geometry...")
    try:
        arcpy.RepairGeometry_management(
            fc, delete_null=True, validation_method="OGC"
        )
        # print(arcpy.GetMessages())
    except Exception as ex:
        print(time.ctime(), f" {os.path.basename(fc)} repair failed")
        print(arcpy.GetMessages())

    print(time.ctime(), " checking for curves...")
    try:
        curved_oids = curve_checker(fc)
        if len(curved_oids) == 0:
            print(time.ctime(), f" {os.path.basename(fc)} has no curves")
        else:
            densify_curve(fc, curved_oids)
    except Exception as ex:
        print(time.ctime(), f" {os.path.basename(fc)} curve check failed")
