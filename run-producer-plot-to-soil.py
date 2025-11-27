#!/usr/bin/python
# -*- coding: UTF-8 -*-

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
import geopandas as gpd
import rasterio
from rasterio import features
import monica_run_lib as Mrunlib
from zalfmas_common import common
from zalfmas_common.model import monica_io
from zalfmas_common.soil import soil_io
from zalfmas_common import rect_ascii_grid_management as ragm

# -----------------------------------------------------------------------------
# PATH CONFIGURATION
# -----------------------------------------------------------------------------

PATHS = {
    "re-local-remote": {
        "path-to-climate-dir": "data/germany/Weather_WL",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
        "path-debug-write-folder": "./debug-out/"
    }
}

DATA_SOIL_DB       = "germany/buek200.sqlite"
DATA_GRID_HEIGHT   = "germany/Hermes-dem-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_SLOPE    = "germany/Hermes-slope-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_LAND_USE = "germany/landuse_1000_31469_gk5.asc"
DATA_GRID_SOIL     = "germany/Hermes-buek-wr_100_25832_etrs89-utm32n.asc"
DATA_GRID_SOIL_OW  = "germany/buek200_1000_25832_etrs89-utm32n_OW.asc"
DATA_GRID_CROPS    = "germany/Hermes-crop-wr_100_25832_etrs89-utm32n.asc"

DEBUG_WRITE_CLIMATE = False

# -----------------------------------------------------------------------------
# MAIN PRODUCER FUNCTION
# -----------------------------------------------------------------------------

def run_producer(server=None, port=None):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    config = {
        "mode": "re-local-remote",
        "server-port": port if port else "6669",
        "server": server if server else "login01.cluster.zalf.de",
        "start-row": "0",
        "end-row": "-1",
        "sim.json": "sim_WL.json",
        "crop.json": "crop_WL.json",
        "site.json": "site_WL.json",
        "setups-file": "sim_setups_SG.csv",
        "run-setups": "[1]",
        "use_csv_soils": False
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]
    soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # Read simulation setups
    setups = Mrunlib.read_sim_setups(config["setups-file"])

    rs_ranges = config["run-setups"][1:-1].split(",")
    run_setups = []
    for rsr in rs_ranges:
        rs_r = rsr.split("-")
        if 1 < len(rs_r) <= 2:
            run_setups.extend(range(int(rs_r[0]), int(rs_r[1])+1))
        else:
            run_setups.append(int(rs_r[0]))

    soil_crs_to_x_transformers = {}
    wgs84_crs = CRS.from_epsg(4326)

    # -----------------------------------------------------------------------------
    # LOAD SOIL GRID
    # -----------------------------------------------------------------------------

    path_to_soil_grid = paths["path-to-data-dir"] + DATA_GRID_SOIL
    soil_epsg_code = int(path_to_soil_grid.split("/")[-1].split("_")[2])
    soil_crs = CRS.from_epsg(soil_epsg_code)

    soil_metadata, _ = ragm.read_header(path_to_soil_grid)
    soil_grid = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)

    soil_crs_to_x_transformers[wgs84_crs] = Transformer.from_crs(soil_crs, wgs84_crs)

    # -----------------------------------------------------------------------------
    # LOAD **ONLY** New_Meta.csv (ALL 3500 WEATHER POINTS)
    # -----------------------------------------------------------------------------

    plot_to_weather = {}
    with open(f"{paths['path-to-data-dir']}germany/New_weatherfile/New_Meta.csv", newline="") as mf:
        meta_reader = csv.DictReader(mf)
        for row in meta_reader:
            plot_no = int(row["Plot no"])
            plot_to_weather[plot_no] = row["Weather_file_no"]

    # -----------------------------------------------------------------------------
    # LOAD DEM, SLOPE, CROPS
    # -----------------------------------------------------------------------------

    # DEM
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT
    dem_metadata, _ = ragm.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = ragm.create_interpolator_from_rect_grid(dem_grid, dem_metadata)

    # Slope
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_metadata, _ = ragm.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_interpolate = ragm.create_interpolator_from_rect_grid(slope_grid, slope_metadata)

    # Crop mask
    path_to_crop_grid = paths["path-to-data-dir"] + DATA_GRID_CROPS
    crop_metadata, _ = ragm.read_header(path_to_crop_grid)
    crop_grid = np.loadtxt(path_to_crop_grid, dtype=int, skiprows=6)
    crop_interpolate = ragm.create_interpolator_from_rect_grid(crop_grid, crop_metadata)

    sent_env_count = 0
    start_time = time.perf_counter()

    # -----------------------------------------------------------------------------
    # MAIN LOOP
    # -----------------------------------------------------------------------------

    for _, setup_id in enumerate(run_setups):

        setup = setups[setup_id]

        # Load json templates
        with open(setup.get("sim.json", config["sim.json"])) as f:
            sim_json = json.load(f)
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])

        with open(setup.get("site.json", config["site.json"])) as f:
            site_json = json.load(f)

        if setup["scenario"].lower().startswith("rcp"):
            site_json["EnvironmentParameters"]["rcp"] = setup["scenario"]

        with open(setup.get("crop.json", config["crop.json"])) as f:
            crop_json = json.load(f)

        crop_json["cropRotation"][2] = setup["crop-id"]

        env_template = monica_io.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        srows = int(soil_metadata["nrows"])
        scols = int(soil_metadata["ncols"])
        cellsize = float(soil_metadata["cellsize"])
        x0 = float(soil_metadata["xllcorner"])
        y0 = float(soil_metadata["yllcorner"])
        nodata_value = int(soil_metadata["nodata_value"])

        soil_id_cache = {}

        for srow in range(srows):

            if srow < int(config["start-row"]):
                continue
            if int(config["end-row"]) > 0 and srow > int(config["end-row"]):
                break

            for scol in range(scols):

                soil_id = int(soil_grid[srow, scol])

                # nodata cell
                if soil_id == nodata_value:
                    env_template["customId"] = {
                        "setup_id": setup_id, "srow": srow, "scol": scol,
                        "soil_id": soil_id, "env_id": sent_env_count,
                        "nodata": True
                    }
                    socket.send_json(env_template)
                    sent_env_count += 1
                    continue

                # crop mask test
                sr = x0 + (scol + 0.5) * cellsize
                sh = y0 + (srows - srow - 0.5) * cellsize
                cropr, croph = soil_crs_to_x_transformers[crop_crs].transform(sr, sh)
                crop_grid_id = int(crop_interpolate(cropr, croph))
                if crop_grid_id != 1:
                    env_template["customId"] = {
                        "setup_id": setup_id, "srow": srow, "scol": scol,
                        "soil_id": soil_id, "env_id": sent_env_count,
                        "nodata": True
                    }
                    socket.send_json(env_template)
                    sent_env_count += 1
                    continue

                # Soil profile from DB
                if soil_id in soil_id_cache:
                    soil_profile = soil_id_cache[soil_id]
                else:
                    soil_profile = soil_io.soil_parameters(soil_db_con, soil_id)
                    soil_id_cache[soil_id] = soil_profile

                env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                # WEATHER: soil_id == plot_no
                weather_base = plot_to_weather.get(soil_id)

                if not weather_base:
                    # missing weather → skip
                    env_template["customId"] = {
                        "setup_id": setup_id, "srow": srow, "scol": scol,
                        "soil_id": soil_id, "env_id": sent_env_count,
                        "nodata": True
                    }
                    socket.send_json(env_template)
                    sent_env_count += 1
                    continue

                # Assign NEW weather file
                env_template["pathToClimateCSV"] = [
                    paths["monica-path-to-climate-dir"] +
                    f"/suren_WL/New_Weatherfile/{weather_base}.csv"
                ]

                env_template["customId"] = {
                    "setup_id": setup_id,
                    "srow": srow,
                    "scol": scol,
                    "soil_id": soil_id,
                    "env_id": sent_env_count,
                    "nodata": False
                }

                socket.send_json(env_template)
                sent_env_count += 1

    stop_time = time.perf_counter()

    if DEBUG_WRITE_CLIMATE:
        debug_write_folder = paths["path-debug-write-folder"]
        if not os.path.exists(debug_write_folder):
            os.makedirs(debug_write_folder)
        path_to_climate_summary = debug_write_folder + "/climate_file_list.csv"
        with open(path_to_climate_summary, "w") as f:
            f.write("\n".join(listOfClimateFiles))

    try:
        print("sending ", (sent_env_count - 1), " envs took ", (stop_time - start_time), " seconds")
        print("exiting run_producer()")
    except Exception:
        raise


# -----------------------------------------------------------------------------
# RUN MAIN
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    run_producer()
