#!/usr/bin/python
# -*- coding: UTF-8 -*-

from collections import defaultdict
import csv
import os
import sys
import timeit

import zmq

import monica_run_lib as Mrunlib
from zalfmas_common import common
from zalfmas_common.model import monica_io

PATHS = {
    "re-local-remote": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "D:/monica_modoklim_wl/out/out/",
        "path-to-csv-output-dir": "D:/monica_modoklim_wl/out/csv-out/",
    },
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/",
    },
}

def run_consumer(leave_after_finished_run=True, server=None, port=None):

    config = {
        "mode": "re-local-remote",
        "port": port if port else "7780",
        "server": server if server else "login01.cluster.zalf.de",
        "timeout": 600000,  # 10 min
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)
    paths = PATHS[config["mode"]]

    if "out" not in config:
        config["out"] = paths["path-to-output-dir"]
    if "csv-out" not in config:
        config["csv-out"] = paths["path-to-csv-output-dir"]

    print("consumer config:", config)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(f"tcp://{config['server']}:{config['port']}")
    socket.RCVTIMEO = config["timeout"]

    leave = False

    def process_message(msg):
        if len(msg.get("errors", [])) > 0:
            print("There were errors in message, skipping:", msg["errors"])
            return False

        if msg.get("type", "") in ["jobs-per-cell", "no-data", "setup_data"]:
            # Control messages – ignore
            return False

        custom_id = msg.get("customId", {})
        nodata = custom_id.get("nodata", False)
        if nodata:
            return False

        setup_id = custom_id.get("setup_id", 0)
        plot_no = custom_id.get("plot_no", -1)
        env_id = custom_id.get("env_id", 0)

        print(f"Received result for setup {setup_id}, plot {plot_no}, env_id {env_id}")

        # Output folder: per setup, one CSV per plot
        path_to_out_dir = os.path.join(config["csv-out"], str(setup_id))
        if not os.path.exists(path_to_out_dir):
            try:
                os.makedirs(path_to_out_dir)
            except OSError:
                print("Could not create dir:", path_to_out_dir)
                return False

        # file name
        path_to_file = os.path.join(path_to_out_dir, f"plot-{plot_no}.csv")

        # For simplicity: overwrite file if multiple envs; if you want to append, change mode to "a"
        with open(path_to_file, "w", newline="") as _:
            writer = csv.writer(_, delimiter=",")

            for data_ in msg.get("data", []):
                results = data_.get("results", [])
                orig_spec = data_.get("origSpec", "")
                output_ids = data_.get("outputIds", [])

                if len(results) > 0:
                    writer.writerow([orig_spec.replace('"', "")])

                    for row in monica_io.write_output_header_rows(
                        output_ids,
                        include_header_row=True,
                        include_units_row=True,
                        include_time_agg=False,
                    ):
                        writer.writerow(row)

                    for row in monica_io.write_output_obj(output_ids, results):
                        writer.writerow(row)

                    writer.writerow([])

        return False  # never auto-leave here; you can control by timeout

    # main loop
    while not leave:
        try:
            msg = socket.recv_json()
            leave = process_message(msg)
        except zmq.error.Again:
            print('No response from server within timeout = %d ms' % socket.RCVTIMEO)
            break
        except Exception as e:
            print("Exception:", e)
            # continue or break as you like

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
