#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

from collections import defaultdict
import csv
import json
import numpy as np
import os
from pyproj import CRS, Transformer
import sqlite3
import sys
import time
import zmq
from scipy.interpolate import NearestNDInterpolator
import geopandas as gpd
import rasterio
from rasterio import features
import monica_run_lib as Mrunlib
from zalfmas_common import common
from zalfmas_common.model import monica_io
from zalfmas_common.soil import soil_io
from zalfmas_common import rect_ascii_grid_management as ragm

PATHS = {
    # adjust the local path to your environment
    "re-local-remote": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-local-remote": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-local-local": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "remoteProducer-remoteMonica": {
        # "include-file-base-path": "/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/data/",  # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "/out/debug-out/",
    }
}

# DATA_SOIL_DB = "germany/buek200.sqlite"
# DATA_GRID_HEIGHT = "germany/dem_100_25832_etrs89-utm32n.asc"
# DATA_GRID_SLOPE = "germany/slope_100_25832_etrs89-utm32n.asc"
# DATA_GRID_LAND_USE = "germany/landuse_1000_31469_gk5.asc"
# DATA_GRID_SOIL = "germany/buek200_100_25832_etrs89-utm32n.asc"
# DATA_GRID_SOIL_OW = "germany/buek200_1000_25832_etrs89-utm32n_OW.asc"
# DATA_GRID_CROPS = "germany/MOLLLL-crop-wr_100_25832_etrs89-utm32n.asc"

DATA_SOIL_DB = "germany/buek200.sqlite"
DATA_GRID_HEIGHT = "germany/Hermes-dem-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_SLOPE = "germany/Hermes-slope-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_LAND_USE = "germany/landuse_1000_31469_gk5.asc"
DATA_GRID_SOIL = "germany/Hermes-buek-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_SOIL_OW = "germany/buek200_1000_25832_etrs89-utm32n_OW.asc"
DATA_GRID_CROPS = "germany/Hermes-crop-wr_100_25832_etrs89-utm32n.asc"

#TEMPLATE_PATH_CLIMATE_CSV = "{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"
TEMPLATE_PATH_CLIMATE_CSV = "{crow}/daily_mean_RES1_C{ccol}R{crow}.csv.gz"
TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon_to_rowcol.json"

# Additional data for masking the regions
NUTS1_REGIONS = "data/germany/NUTS250_N1.shp"

TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/WW_WL_{crop_id}.csv"

gdf = gpd.read_file(NUTS1_REGIONS)

DEBUG_DONOT_SEND = False
DEBUG_WRITE = False
DEBUG_ROWS = 10
DEBUG_WRITE_FOLDER = "./debug_out"
DEBUG_WRITE_CLIMATE = False


## Add an argument in the run_producer function and make a loop with changing of the value of the additional parameter (sensitivity analysis)
## Make a list of the parameter values first

# commandline parameters e.g "server=localhost port=6666 shared_id=2"
def run_producer(server=None, port=None):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote",
        "server-port": port if port else "6666",
        "server": server if server else "login01.cluster.zalf.de",
        "start-row": "0",
        "end-row": "-1",
        "path_to_dem_grid": "",
        "sim.json": "sim_WL.json",
        "crop.json": "crop_WL.json",
        "site.json": "site_WL.json",
        "setups-file": "sim_setups_SG.csv",
        "run-setups": "[1]",
        "use_csv_soils": True,
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    # select paths 
    paths = PATHS[config["mode"]]
    # open soil db connection
    soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB)
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    rs_ranges = config["run-setups"][1:-1].split(",")
    run_setups = []
    for rsr in rs_ranges:
        rs_r = rsr.split("-")
        if 1 < len(rs_r) <= 2:
            run_setups.extend(range(int(rs_r[0]), int(rs_r[1])+1))
        elif len(rs_r) == 1:
            run_setups.append(int(rs_r[0]))
    #run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    # transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    soil_crs_to_x_transformers = {}
    wgs84_crs = CRS.from_epsg(4326)
    utm32_crs = CRS.from_epsg(25832)
    # transformers[wgs84] = Transformer.from_crs(wgs84_crs, gk5_crs, always_xy=True)

    ilr_seed_harvest_data = defaultdict(
        lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    # Load grids
    ## note numpy is able to load from a compressed file, ending with .gz or .bz2

    # soil data
    path_to_soil_grid = paths["path-to-data-dir"] + DATA_GRID_SOIL
    soil_epsg_code = int(path_to_soil_grid.split("/")[-1].split("_")[2])
    soil_crs = CRS.from_epsg(soil_epsg_code)
    if wgs84_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[wgs84_crs] = Transformer.from_crs(soil_crs, wgs84_crs)
    soil_metadata, _ = ragm.read_header(path_to_soil_grid)
    soil_grid = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)
    print("read: ", path_to_soil_grid)

    csv_soil_profiles = {}
    csv_soil_interpolate = None
    csv_soils_crs = CRS.from_epsg(25833)
    soil_crs_to_x_transformers[csv_soils_crs] = Transformer.from_crs(soil_crs, csv_soils_crs, always_xy=True)
    if config["use_csv_soils"]:
        with open(f"{paths['path-to-data-dir']}/germany/Final_Hermes_Soil.csv") as file:
            dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
            file.seek(0)
            reader = csv.reader(file, dialect)
            header_line = next(reader)
            header = {h: i for i, h in enumerate(header_line)}
            points = []
            values = []
            for line in reader:
                r = int(line[header["X"]])
                h = int(line[header["Y"]])
                points.append([r, h])
                values.append((r, h))
                profile = [
                    {
                        "Thickness": [0.3, "m"],
                        "SoilBulkDensity": [1500, "kg/m3"],
                        "SoilOrganicCarbon": [float(line[header["Corg_0"]]), "%"],
                        "Clay": [float(line[header["Clay_0"]]) / 100.0, "m3/m3"],
                        "Sand": [float(line[header["Sand_0"]]) / 100.0, "m3/m3"],
                        "Silt": [float(line[header["Silt_0"]]) / 100.0, "m3/m3"],
                    },
                    {
                        "Thickness": [0.3, "m"],
                        "SoilBulkDensity": [1700, "kg/m3"],
                        "SoilOrganicCarbon": [float(line[header["Corg_30"]]), "%"],
                        "Clay": [float(line[header["Clay_30"]]) / 100.0, "m3/m3"],
                        "Sand": [float(line[header["Sand_30"]]) / 100.0, "m3/m3"],
                        "Silt": [float(line[header["Silt_30"]]) / 100.0, "m3/m3"],
                    },
                    {
                        "Thickness": [0.3, "m"],
                        "SoilBulkDensity": [1700, "kg/m3"],
                        "SoilOrganicCarbon": [float(line[header["Corg_60"]]), "%"],
                        "Clay": [float(line[header["Clay_60"]]) / 100.0, "m3/m3"],
                        "Sand": [float(line[header["Sand_60"]]) / 100.0, "m3/m3"],
                        "Silt": [float(line[header["Silt_60"]]) / 100.0, "m3/m3"],
                    }
                ]
                csv_soil_profiles[(r, h)] = profile
            csv_soil_interpolate = NearestNDInterpolator(np.array(points), np.array(values))

    # height data for germany
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT
    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg_code)
    if dem_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[dem_crs] = Transformer.from_crs(soil_crs, dem_crs)
    dem_metadata, _ = ragm.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = ragm.create_interpolator_from_rect_grid(dem_grid, dem_metadata)
    print("read: ", path_to_dem_grid)

    # slope data
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_epsg_code = int(path_to_slope_grid.split("/")[-1].split("_")[2])
    slope_crs = CRS.from_epsg(slope_epsg_code)
    if slope_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[slope_crs] = Transformer.from_crs(soil_crs, slope_crs)
    slope_metadata, _ = ragm.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_interpolate = ragm.create_interpolator_from_rect_grid(slope_grid, slope_metadata)
    print("read: ", path_to_slope_grid)

    # land use data
    path_to_landuse_grid = paths["path-to-data-dir"] + DATA_GRID_LAND_USE
    landuse_epsg_code = int(path_to_landuse_grid.split("/")[-1].split("_")[2])
    landuse_crs = CRS.from_epsg(landuse_epsg_code)
    if landuse_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[landuse_crs] = Transformer.from_crs(soil_crs, landuse_crs)
    landuse_meta, _ = ragm.read_header(path_to_landuse_grid)
    landuse_grid = np.loadtxt(path_to_landuse_grid, dtype=int, skiprows=6)
    landuse_interpolate = ragm.create_interpolator_from_rect_grid(landuse_grid, landuse_meta)
    print("read: ", path_to_landuse_grid)

    # crop mask data
    path_to_crop_grid = paths["path-to-data-dir"] + DATA_GRID_CROPS
    crop_epsg_code = int(path_to_crop_grid.split("/")[-1].split("_")[2])
    crop_crs = CRS.from_epsg(crop_epsg_code)
    if crop_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[crop_crs] = Transformer.from_crs(soil_crs, crop_crs)
    crop_meta, _ = ragm.read_header(path_to_crop_grid)
    crop_grid = np.loadtxt(path_to_crop_grid, dtype=int, skiprows=6)
    crop_interpolate = ragm.create_interpolator_from_rect_grid(crop_grid, crop_meta, ignore_nodata=False)
    print("read: ", path_to_crop_grid)

    # Create the function for the mask. This function will later use the additional column in a setup file!

    def create_mask_from_shapefile(NUTS1_REGIONS, region_name, path_to_soil_grid):
        regions_df = gpd.read_file(NUTS1_REGIONS)
        region = regions_df[regions_df["NUTS_NAME"] == region_name]

        # This is needed to read the transformation data correctly from the file. With the original opening it does not work
        with rasterio.open(path_to_soil_grid) as dataset:
            soil_grid = dataset.read(1)
            transform = dataset.transform

        rows, cols = soil_grid.shape
        mask = rasterio.features.geometry_mask([region.geometry.values[0]], out_shape=(rows, cols), transform=transform,
                                               invert=True)
        return mask

    sent_env_count = 0
    start_time = time.perf_counter()

    listOfClimateFiles = set()

    # run calculations for each setup
    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.perf_counter()

        setup = setups[setup_id]
        gcm = setup["gcm"]
        rcm = setup["rcm"]
        scenario = setup["scenario"]
        ensmem = setup["ensmem"]
        version = setup["version"]
        crop_id = setup["crop-id"]
        region_name = setup["region_name"]

        ## extract crop_id from crop-id name that has possible an extension
        crop_id_short = crop_id.split('_')[0]

        if region_name and len(region_name) > 0:
            # Create the soil mask for the specific region
            path_to_soil_grid_ow = paths["path-to-data-dir"] + DATA_GRID_SOIL_OW
            mask = create_mask_from_shapefile(NUTS1_REGIONS, region_name, path_to_soil_grid_ow)

            # Apply the soil mask to the soil grid
            soil_grid_copy = soil_grid.copy()
            soil_grid[mask == False] = -8888
            soil_grid[soil_grid_copy == -9999] = -9999

        cdict = {}
        # path to latlon-to-rowcol.json
        # path = TEMPLATE_PATH_LATLON.format(path_to_climate_dir=paths["path-to-climate-dir"] + setup["climate_path_to_latlon_file"] + "/")
        path = TEMPLATE_PATH_LATLON.format(
            path_to_climate_dir=paths["path-to-climate-dir"] + setup["climate_path_to_latlon_file"] + "/")
        climate_data_interpolator = ragm.create_climate_geoGrid_interpolator_from_json_file(path, wgs84_crs,
                                                                                               soil_crs, cdict)
        print("created climate_data to gk5 interpolator: ", path)

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date according to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])
            # sim_json["include-file-base-path"] = paths["include-file-base-path"]

        # read template site.json
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        if len(scenario) > 0 and scenario[:3].lower() == "rcp":
            site_json["EnvironmentParameters"]["rcp"] = scenario

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup[
            "use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

        crop_json["cropRotation"][2] = crop_id

        # create environment template from json templates
        env_template = monica_io.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        scols = int(soil_metadata["ncols"])
        srows = int(soil_metadata["nrows"])
        scellsize = int(soil_metadata["cellsize"])
        xllcorner = int(soil_metadata["xllcorner"])
        yllcorner = int(soil_metadata["yllcorner"])
        nodata_value = int(soil_metadata["nodata_value"])

        # unknown_soil_ids = set()
        soil_id_cache = {}
        print("All Rows x Cols: " + str(srows) + "x" + str(scols))
        # cs__ = open("coord_mapping_etrs89-utm32n_to_wgs84-latlon.csv", "w")
        # cs__.write("row,col,center_25832_etrs89-utm32n_r,center_25832_etrs89-utm32n_h,center_lat,center_lon\n")

        for srow in range(0, srows):
            print(srow, end=", ")

            if srow < int(config["start-row"]):
                continue
            elif int(config["end-row"]) > 0 and srow > int(config["end-row"]):
                break

            for scol in range(0, scols):
                soil_id = int(soil_grid[srow, scol])
                if soil_id == nodata_value:
                    continue

                # get coordinate of clostest climate element of real soil-cell
                sh = yllcorner + (scellsize / 2) + (srows - srow - 1) * scellsize
                sr = xllcorner + (scellsize / 2) + scol * scellsize
                # inter = crow/ccol encoded into integer
                crow, ccol = map(int, climate_data_interpolator(sr, sh))

                tcoords = {}

                if crop_crs not in tcoords:
                    tcoords[crop_crs] = soil_crs_to_x_transformers[crop_crs].transform(sr, sh)
                cropr, croph = tcoords[crop_crs]
                crop_grid_id = int(crop_interpolate(cropr, croph))
                #crop_grid_id = int(crop_grid[srow, scol])
                # print(crop_grid_id)
                 # print(crop_grid_id)
                if crop_grid_id==1:
                    print("srow:", srow, "scol:", scol, "sh:", sh, "sr:", sr, "cropId:", crop_grid_id, flush=True)

                
                if crop_grid_id != 1 or soil_id == -8888:
                    # print("row/col:", srow, "/", scol, "is not a crop pixel.")
                    env_template["customId"] = {
                        "setup_id": setup_id,
                        "srow": srow, "scol": scol,
                        "crow": int(crow), "ccol": int(ccol),
                        "soil_id": soil_id,
                        "env_id": sent_env_count,
                        "nodata": True,
                    }
                    if not DEBUG_DONOT_SEND:
                        socket.send_json(env_template)
                        # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
                        sent_env_count += 1
                    continue

                if config["use_csv_soils"]:
                    csvs_r, csvs_h = soil_crs_to_x_transformers[csv_soils_crs].transform(sr, sh)
                    csvp_r, csvp_h = csv_soil_interpolate(csvs_r, csvs_h)
                    soil_profile = csv_soil_profiles.get((csvp_r, csvp_h), [])
                else:
                    if soil_id in soil_id_cache:
                        soil_profile = soil_id_cache[soil_id]
                    else:
                        soil_profile = soil_io.soil_parameters(soil_db_con, soil_id)
                        soil_id_cache[soil_id] = soil_profile

                if len(soil_profile) == 0:
                    env_template["customId"] = {
                        "setup_id": setup_id,
                        "srow": srow, "scol": scol,
                        "crow": int(crow), "ccol": int(ccol),
                        "soil_id": soil_id,
                        "env_id": sent_env_count,
                        "nodata": True,
                    }
                    if not DEBUG_DONOT_SEND:
                        socket.send_json(env_template)
                        # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
                        sent_env_count += 1
                    continue

                # check if current grid cell is used for agriculture                
                if setup["landcover"]:
                    if landuse_crs not in tcoords:
                        tcoords[landuse_crs] = soil_crs_to_x_transformers[landuse_crs].transform(sr, sh)
                    lur, luh = tcoords[landuse_crs]
                    landuse_id = landuse_interpolate(lur, luh)
                    if landuse_id not in [2, 3, 4]:
                        continue

                if dem_crs not in tcoords:
                    tcoords[dem_crs] = soil_crs_to_x_transformers[dem_crs].transform(sr, sh)
                demr, demh = tcoords[dem_crs]
                height_nn = dem_interpolate(demr, demh)

                if slope_crs not in tcoords:
                    tcoords[slope_crs] = soil_crs_to_x_transformers[slope_crs].transform(sr, sh)
                slr, slh = tcoords[slope_crs]
                slope = slope_interpolate(slr, slh)

                env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup[
                    "LeafExtensionModifier"]

                # print("soil:", soil_profile)
                env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                if setup["elevation"]:
                    env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

                if setup["slope"]:
                    env_template["params"]["siteParameters"]["slope"] = slope / 100.0

                if setup["latitude"]:
                    clat, _ = cdict[(crow, ccol)]
                    env_template["params"]["siteParameters"]["Latitude"] = clat

                if setup["CO2"]:
                    env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(setup["CO2"])

                if setup["O3"]:
                    env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(setup["O3"])

                if setup["FieldConditionModifier"]:
                    env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"][
                        "FieldConditionModifier"] = float(setup["FieldConditionModifier"])

                if setup["StageTemperatureSum"]:
                    stage_ts = setup["StageTemperatureSum"].split('_')
                    stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                    orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                        "StageTemperatureSum"][0]
                    if len(stage_ts) != len(orig_stage_ts):
                        stage_ts = orig_stage_ts
                        print('The provided StageTemperatureSum array is not '
                              'sufficiently long. Falling back to original StageTemperatureSum')

                    env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                        "StageTemperatureSum"][0] = stage_ts

                env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup[
                    "fertilization"]

                env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
                env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup[
                    "WaterDeficitResponseOn"]
                env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup[
                    "EmergenceMoistureControlOn"]
                env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup[
                    "EmergenceFloodingControlOn"]

                env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

                subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm=rcm, scenario=scenario, ensmem=ensmem,
                                                                  version=version, crow=str(crow), ccol=str(ccol))
                for _ in range(4):
                    subpath_to_csv = subpath_to_csv.replace("//", "/")
                env_template["pathToClimateCSV"] = [
                    paths["monica-path-to-climate-dir"] + setup["climate_path_to_csvs"] + "/" + subpath_to_csv]
                if setup["incl_hist"]:
                    hist_subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm=rcm, scenario="historical",
                                                                           ensmem=ensmem, version=version,
                                                                           crow=str(crow), ccol=str(ccol))
                    for _ in range(4):
                        hist_subpath_to_csv = hist_subpath_to_csv.replace("//", "/")
                    env_template["pathToClimateCSV"].insert(0, paths["monica-path-to-climate-dir"] + setup[
                        "climate_path_to_csvs"] + "/" + hist_subpath_to_csv)
                print("pathToClimateCSV:", env_template["pathToClimateCSV"])
                if DEBUG_WRITE_CLIMATE:
                    listOfClimateFiles.add(subpath_to_csv)

                env_template["customId"] = {
                    "setup_id": setup_id,
                    "srow": srow, "scol": scol,
                    "crow": int(crow), "ccol": int(ccol),
                    "soil_id": soil_id,
                    "env_id": sent_env_count,
                    "nodata": False
                }

                if not DEBUG_DONOT_SEND:
                    socket.send_json(env_template)
                    print("sent env ", sent_env_count, " customId: ", env_template["customId"])

                sent_env_count += 1

                # write debug output, as json file
                if DEBUG_WRITE:
                    debug_write_folder = paths["path-debug-write-folder"]
                    if not os.path.exists(debug_write_folder):
                        os.makedirs(debug_write_folder)
                    if sent_env_count < DEBUG_ROWS:

                        path_to_debug_file = debug_write_folder + "/row_" + str(sent_env_count - 1) + "_" + str(
                            setup_id) + ".json"

                        if not os.path.isfile(path_to_debug_file):
                            with open(path_to_debug_file, "w") as _:
                                _.write(json.dumps(env_template))
                        else:
                            print("WARNING: Row ", (sent_env_count - 1), " already exists")
            # print("unknown_soil_ids:", unknown_soil_ids)

        #if env_template:
        #    env_template["pathToClimateCSV"] = ""
        #    env_template["customId"] = {
        #        "setup_id": setup_id,
        #        "nodata": True,
        #        "no_of_sent_envs": sent_env_count,
        #    }
        #    socket.send_json(env_template)

            # print("crows/cols:", crows_cols)
        # cs__.close()
        stop_setup_time = time.perf_counter()
        print("\nSetup ", sent_env_count, " envs took ", (stop_setup_time - start_setup_time), " seconds")
        sent_env_count = 0

    stop_time = time.perf_counter()

    # write summary of used json files
    if DEBUG_WRITE_CLIMATE:
        debug_write_folder = paths["path-debug-write-folder"]
        if not os.path.exists(debug_write_folder):
            os.makedirs(debug_write_folder)

        path_to_climate_summary = debug_write_folder + "/climate_file_list" + ".csv"
        with open(path_to_climate_summary, "w") as _:
            _.write('\n'.join(listOfClimateFiles))

    try:
        print("sending ", (sent_env_count - 1), " envs took ", (stop_time - start_time), " seconds")
        # print("ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
        print("exiting run_producer()")
    except Exception:
        raise


if __name__ == "__main__":
    run_producer()
