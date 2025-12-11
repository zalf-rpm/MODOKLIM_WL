#!/usr/bin/python
# -*- coding: UTF-8 -*-

from collections import defaultdict
import csv
import json
import numpy as np
import os
import sys
import time
import zmq
import sqlite3
from copy import deepcopy

from pyproj import CRS, Transformer

import monica_run_lib as Mrunlib
from zalfmas_common import common
from zalfmas_common.model import monica_io
from zalfmas_common import rect_ascii_grid_management as ragm

# -------------------------------------------------------------------
# USER-ADDED WL-DEM LOGIC
# -------------------------------------------------------------------
DEM_THRESHOLD = 43.0  # Below this elevation → WL-stress applied

NORMAL_COC = [0.2, 0.2, 0.25, 0.25, 0.2, 0.2]   # Default
HIGH_WL_COC = [0.2, 0.2, 0.40, 0.40, 0.25, 0.2]  # Strong WL stress when low DEM

# -------------------------------------------------------------------
# PATH CONFIG
# -------------------------------------------------------------------
PATHS = {
    "re-local-remote": {
        "path-to-climate-dir": "data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
        "path-debug-write-folder": "./debug-out/",
    }
}

DATA_GRID_HEIGHT = "germany/Hermes-dem-wr_5_25832_etrs89-utm32n.asc"
FINAL_SOIL_CSV = "germany/Final_Hermes_Soil_25832.csv"
META_WEATHER_CSV = "germany/Weather_WL/Meta.csv"

# -------------------------------------------------------------------
def run_producer(server=None, port=None):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    config = {
        "mode": "re-local-remote",
        "server-port": port if port else "6667",
        "server": server if server else "login01.cluster.zalf.de",
        "sim.json": "sim_WL.json",
        "crop.json": "crop_WL.json",
        "site.json": "site_WL.json",
        "setups-file": "sim_setups_SG.csv",
        "run-setups": "[1]",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]
    socket.connect(f"tcp://{config['server']}:{config['server-port']}")

    # ---------------- READ SETUPS ----------------
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    rs_ranges = config["run-setups"][1:-1].split(",")
    run_setups = []
    for rsr in rs_ranges:
        parts = rsr.split("-")
        if len(parts) == 2:
            run_setups.extend(range(int(parts[0]), int(parts[1]) + 1))
        else:
            run_setups.append(int(parts[0]))

    print("read sim setups:", config["setups-file"], "→ run_setups:", run_setups)

    # ---------------- LOAD DEM GRID ----------------
    path_to_dem_grid = os.path.join(paths["path-to-data-dir"], DATA_GRID_HEIGHT)
    dem_metadata, _ = ragm.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = ragm.create_interpolator_from_rect_grid(dem_grid, dem_metadata)
    print("read DEM:", path_to_dem_grid)

    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg_code)

    csv_soils_crs = CRS.from_epsg(25832)
    csv_to_dem = Transformer.from_crs(csv_soils_crs, dem_crs, always_xy=True)

    # ---------------- LOAD 90 SOIL POINTS ----------------
    plots = {}
    soil_csv_path = os.path.join(paths["path-to-data-dir"], FINAL_SOIL_CSV)

    with open(soil_csv_path, newline="") as f:
        reader = csv.DictReader(f, delimiter=",")

        for row in reader:
            if not row["id"].strip():
                continue

            plot_no = int(row["id"])
            pr = float(row["X"])
            ph = float(row["Y"])

            profile = [
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [float(row["Bulk_Density_0"]) * 1000, "kg/m3"],
                    "SoilOrganicCarbon": [float(row["Corg_0"]), "%"],
                    "Clay": [float(row["Clay_0"]) / 100.0, "m3/m3"],
                    "Sand": [float(row["Sand_0"]) / 100.0, "m3/m3"],
                    "Silt": [float(row["Silt_0"]) / 100.0, "m3/m3"],
                },
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [float(row["Bulk_Density_30"]) * 1000, "kg/m3"],
                    "SoilOrganicCarbon": [float(row["Corg_30"]), "%"],
                    "Clay": [float(row["Clay_30"]) / 100.0, "m3/m3"],
                    "Sand": [float(row["Sand_30"]) / 100.0, "m3/m3"],
                    "Silt": [float(row["Silt_30"]) / 100.0, "m3/m3"],
                },
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [float(row["Bulk_Density_60"]) * 1000, "kg/m3"],
                    "SoilOrganicCarbon": [float(row["Corg_60"]), "%"],
                    "Clay": [float(row["Clay_60"]) / 100.0, "m3/m3"],
                    "Sand": [float(row["Sand_60"]) / 100.0, "m3/m3"],
                    "Silt": [float(row["Silt_60"]) / 100.0, "m3/m3"],
                },
            ]

            plots[plot_no] = {"pr": pr, "ph": ph, "profile": profile}

    print(f"Loaded {len(plots)} soil points from {FINAL_SOIL_CSV}")

    # ---------------- WEATHER MAPPING ----------------
    plot_to_weather = {}
    meta_weather_path = os.path.join(paths["path-to-data-dir"], META_WEATHER_CSV)
    with open(meta_weather_path, newline="") as mf:
        reader = csv.DictReader(mf)
        for row in reader:
            plot_to_weather[int(row["Plot no"])] = row["Weather_file_no"]

    print(f"Loaded weather mapping for {len(plot_to_weather)} plots.")

    # ---------------- RUN ----------------
    sent_env_count_total = 0
    start_time = time.perf_counter()

    for setup_id in run_setups:

        setup = setups[setup_id]
        print(f"\n=== RUNNING SETUP {setup_id}: {setup} ===")

        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        if setup.get("start_date"):
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup.get("end_date"):
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])

        crop_id = setup["crop-id"]
        crop_json["cropRotation"][2] = crop_id

        env_template_base = monica_io.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        sent_env_count = 0
        print("Sending environments for ONLY the 90 plots...\n")

        for plot_no, pdata in plots.items():

            pr = pdata["pr"]
            ph = pdata["ph"]

            sr_dem, sh_dem = csv_to_dem.transform(pr, ph)
            height_nn = float(dem_interpolate(sr_dem, sh_dem))

            env = deepcopy(env_template_base)

            env["params"]["siteParameters"]["HeightNN"] = height_nn
            env["params"]["siteParameters"]["SoilProfileParameters"] = pdata["profile"]

            # ======== DEM → WL AUTOMATIC CONTROL ==========
            if height_nn < DEM_THRESHOLD:
                env["params"]["cropParameters"]["CriticalOxygenContent"] = HIGH_WL_COC
            else:
                env["params"]["cropParameters"]["CriticalOxygenContent"] = NORMAL_COC
            # =================================================

            weather_file = plot_to_weather.get(plot_no)
            if not weather_file:
                print(f"⚠ No weather file for plot {plot_no}, skipping.")
                continue

            env["pathToClimateCSV"] = [
                paths["monica-path-to-climate-dir"]
                + f"/suren_WL/daily_mean_RES1_C181R0.csv/{weather_file}.csv"
            ]

            env["customId"] = {
                "setup_id": setup_id,
                "plot_no": plot_no,
                "env_id": sent_env_count,
                "nodata": False,
            }

            socket.send_json(env)
            print(f"Sent env {sent_env_count} for plot {plot_no}")
            sent_env_count += 1
            sent_env_count_total += 1

        print(f"Finished setup {setup_id}: sent {sent_env_count} environments.")

    stop_time = time.perf_counter()
    print(f"\nTotal environments sent: {sent_env_count_total}")
    print(f"Total time: {stop_time - start_time:.1f} s")
    print("Exiting run_producer().")

# -------------------------------------------------------------------
if __name__ == "__main__":
    run_producer()
