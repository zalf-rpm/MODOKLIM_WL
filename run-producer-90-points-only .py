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
# PATH CONFIG
# -------------------------------------------------------------------
PATHS = {
    "re-local-remote": {
        "path-to-climate-dir": "data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
        "path-debug-write-folder": "./debug-out/",
    },
    "remoteProducer-remoteMonica": {
        "path-to-climate-dir": "/data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
        "path-debug-write-folder": "/out/debug-out/",
    },
}

# DEM only (no soil ASC)
DATA_GRID_HEIGHT = "germany/Hermes-dem-wr_5_25832_etrs89-utm32n.asc"

FINAL_SOIL_CSV = "germany/Final_Hermes_Soil.csv"
META_WEATHER_CSV = "germany/Weather_WL/Meta.csv"  # mapping Plot no -> Weather_file_no

# -------------------------------------------------------------------
def run_producer(server=None, port=None):

    # ---------------- BASIC CONFIG ----------------
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
        "run-setups": "[1]",       # adjust if you want multiple setups
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]
    socket.connect(f"tcp://{config['server']}:{config['server-port']}")

    # ---------------- READ SETUPS ----------------
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    rs_ranges = config["run-setups"][1:-1].split(",")
    run_setups = []
    for rsr in rs_ranges:
        rs_r = rsr.split("-")
        if 1 < len(rs_r) <= 2:
            run_setups.extend(range(int(rs_r[0]), int(rs_r[1]) + 1))
        elif len(rs_r) == 1:
            run_setups.append(int(rs_r[0]))

    print("read sim setups:", config["setups-file"], "→ run_setups:", run_setups)

    # ---------------- LOAD DEM GRID ----------------
    path_to_dem_grid = os.path.join(paths["path-to-data-dir"], DATA_GRID_HEIGHT)
    dem_metadata, _ = ragm.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = ragm.create_interpolator_from_rect_grid(dem_grid, dem_metadata)
    print("read DEM:", path_to_dem_grid)

    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg_code)

    # CSV coords are in EPSG:25833
    csv_soils_crs = CRS.from_epsg(25833)
    csv_to_dem = Transformer.from_crs(csv_soils_crs, dem_crs, always_xy=True)

    # ---------------- LOAD 90 SOIL POINTS ----------------
    plots = {}
    soil_csv_path = os.path.join(paths["path-to-data-dir"], FINAL_SOIL_CSV)
    with open(soil_csv_path) as f:
        # Detect delimiter if needed
        sample = f.read(2048)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=";, \t")
        reader = csv.DictReader(f, dialect=dialect)

        for row in reader:
            plot_no = int(row["id"])
            pr = float(row["X"])   # EPSG:25833
            ph = float(row["Y"])

            profile = [
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [1500, "kg/m3"],
                    "SoilOrganicCarbon": [float(row["Corg_0"]), "%"],
                    "Clay": [float(row["Clay_0"]) / 100.0, "m3/m3"],
                    "Sand": [float(row["Sand_0"]) / 100.0, "m3/m3"],
                    "Silt": [float(row["Silt_0"]) / 100.0, "m3/m3"],
                },
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [1500, "kg/m3"],
                    "SoilOrganicCarbon": [float(row["Corg_30"]), "%"],
                    "Clay": [float(row["Clay_30"]) / 100.0, "m3/m3"],
                    "Sand": [float(row["Sand_30"]) / 100.0, "m3/m3"],
                    "Silt": [float(row["Silt_30"]) / 100.0, "m3/m3"],
                },
                {
                    "Thickness": [0.3, "m"],
                    "SoilBulkDensity": [1700, "kg/m3"],
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

    # ---------------- RUN FOR EACH SETUP ----------------
    sent_env_count_total = 0
    start_time = time.perf_counter()

    for setup_id in run_setups:
        if setup_id not in setups:
            continue

        setup = setups[setup_id]
        print(f"\n=== RUNNING SETUP {setup_id}: {setup} ===")

        # read templates
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        # dates from setup
        if setup.get("start_date"):
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup.get("end_date"):
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])

        # ensure crop-id is set correctly
        crop_id = setup["crop-id"]
        crop_json["cropRotation"][2] = crop_id

        # build base env template
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

            # transform CSV coords -> DEM CRS
            sr_dem, sh_dem = csv_to_dem.transform(pr, ph)

            # get elevation from DEM
            height_nn = float(dem_interpolate(sr_dem, sh_dem))

            env = deepcopy(env_template_base)

            # set site parameters
            env["params"]["siteParameters"]["HeightNN"] = height_nn
            env["params"]["siteParameters"]["SoilProfileParameters"] = pdata["profile"]

            # weather file for this plot
            weather_file = plot_to_weather.get(plot_no)
            if not weather_file:
                print(f"⚠ No weather file for plot {plot_no}, skipping.")
                continue

            env["pathToClimateCSV"] = [
                paths["monica-path-to-climate-dir"] +
                f"/suren_WL/daily_mean_RES1_C181R0.csv/{weather_file}.csv"
            ]

            # custom ID for consumer (we use plot_no instead of srow/scol)
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


if __name__ == "__main__":
    run_producer()
