#!/usr/bin/python
# -*- coding: UTF-8 -*-

from collections import defaultdict
import csv, json, numpy as np, os, sys, time, zmq
from copy import deepcopy
from pyproj import CRS, Transformer

import monica_run_lib as Mrunlib
from zalfmas_common import common
from zalfmas_common.model import monica_io
from zalfmas_common import rect_ascii_grid_management as ragm


PATHS = {
    "remoteProducer-remoteMonica": {
        "path-to-climate-dir": "/data/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        "path-to-data-dir": "./data/",
        "path-debug-write-folder": "/out/debug-out/",
    }
}

DATA_GRID_HEIGHT = "germany/Hermes-dem-wr_5_25832_etrs89-utm32n.asc"
FINAL_SOIL_CSV  = "germany/Final_Hermes_Soil_25832.csv"
META_WEATHER_CSV = "germany/Weather_WL/Meta.csv"


def run_producer(server=None, port=None):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    config = {
        "mode": "remoteProducer-remoteMonica",
        "server-port": port if port else "6666",
        "server": server if server else "node120",
        "sim.json": "sim_WL.json",
        "crop.json": "crop_WL.json",
        "site.json": "site_WL.json",
        "setups-file": "sim_setups_SG.csv",
        "run-setups": "[1]",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)
    paths = PATHS[config["mode"]]
    socket.connect(f"tcp://{config['server']}:{config['server-port']}")

    # ------------ Load setups ------------
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = [int(s) for s in config["run-setups"][1:-1].split(",")]

    # ------------ Load DEM ------------
    path_to_dem = os.path.join(paths["path-to-data-dir"], DATA_GRID_HEIGHT)
    dem_metadata, _ = ragm.read_header(path_to_dem)
    dem_grid = np.loadtxt(path_to_dem, dtype=float, skiprows=6)

    nodata = dem_metadata["nodata_value"]
    valid_dem = dem_grid[dem_grid != nodata]

    # COMPUTE WL threshold (bottom 20% elevation)
    DEM_THRESHOLD = np.percentile(valid_dem, 20)
    print(f"\nDEM threshold for WL = {DEM_THRESHOLD:.2f} m\n")

    dem_epsg = int(path_to_dem.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg)
    csv_crs = CRS.from_epsg(25832)
    csv_to_dem = Transformer.from_crs(csv_crs, dem_crs, always_xy=True)

    dem_interp = ragm.create_interpolator_from_rect_grid(dem_grid, dem_metadata)

    # ------------ Load soil points ------------
    plots = {}
    with open(os.path.join(paths["path-to-data-dir"], FINAL_SOIL_CSV)) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row["id"]: continue
            pid = int(row["id"])
            plots[pid] = {
                "pr": float(row["X"]),
                "ph": float(row["Y"]),
                "profile": [
                    {"Thickness":[0.3,"m"],"SoilBulkDensity":[1500,"kg/m3"],
                     "SoilOrganicCarbon":[float(row["Corg_0"]),"%"],
                     "Clay":[float(row["Clay_0"])/100,"m3/m3"],
                     "Sand":[float(row["Sand_0"])/100,"m3/m3"],
                     "Silt":[float(row["Silt_0"])/100,"m3/m3"]},

                    {"Thickness":[0.3,"m"],"SoilBulkDensity":[1500,"kg/m3"],
                     "SoilOrganicCarbon":[float(row["Corg_30"]),"%"],
                     "Clay":[float(row["Clay_30"])/100,"m3/m3"],
                     "Sand":[float(row["Sand_30"])/100,"m3/m3"],
                     "Silt":[float(row["Silt_30"])/100,"m3/m3"]},

                    {"Thickness":[0.3,"m"],"SoilBulkDensity":[1700,"kg/m3"],
                     "SoilOrganicCarbon":[float(row["Corg_60"]),"%"],
                     "Clay":[float(row["Clay_60"])/100,"m3/m3"],
                     "Sand":[float(row["Sand_60"])/100,"m3/m3"],
                     "Silt":[float(row["Silt_60"])/100,"m3/m3"]},
                ]
            }

    # ------------ Weather mapping ------------
    plot_to_weather = {}
    with open(os.path.join(paths["path-to-data-dir"], META_WEATHER_CSV)) as f:
        for r in csv.DictReader(f):
            plot_to_weather[int(r["Plot no"])] = r["Weather_file_no"]

    # ------------ Run setups ------------
    sent_total = 0
    for setup_id in run_setups:

        setup = setups[setup_id]

        sim_json = json.load(open(setup["sim.json"]))
        site_json = json.load(open(setup["site.json"]))
        crop_json = json.load(open(setup["crop.json"]))

        # Build base env template
        base_env = monica_io.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        print("\nSending environments...\n")
        count = 0

        for pid, pdata in plots.items():

            # --- Get DEM elevation ---
            x, y = csv_to_dem.transform(pdata["pr"], pdata["ph"])
            height = float(dem_interp(x, y))

            env = deepcopy(base_env)

            env["params"]["siteParameters"]["HeightNN"] = height
            env["params"]["siteParameters"]["SoilProfileParameters"] = pdata["profile"]

            # --- Assign weather file ---
            wf = plot_to_weather.get(pid)
            if not wf:
                print(f"No weather for {pid}, skipping")
                continue

            env["pathToClimateCSV"] = [
                paths["monica-path-to-climate-dir"] +
                f"/suren_WL/daily_mean_RES1_C181R0.csv/{wf}.csv"
            ]

            # --- Apply WL stress if low DEM ---
            species = env["params"]["crop"]["cropParams"]["species"]

            if height < DEM_THRESHOLD:
                print(f"Plot {pid}: LOW DEM {height:.2f} -> WL stress applied")
                species["DefaultRadiationUseEfficiency"] *= 0.70   # ↓ 30%
            else:
                print(f"Plot {pid}: Normal DEM {height:.2f}")

            # --- Custom ID ---
            env["customId"] = {
                "setup_id": setup_id,
                "plot_no": pid,
                "env_id": count,
            }

            socket.send_json(env)
            count += 1

        print(f"Setup {setup_id}: sent {count} envs")
        sent_total += count

    print(f"\nTotal environments sent = {sent_total}")


if __name__ == "__main__":
    run_producer()
