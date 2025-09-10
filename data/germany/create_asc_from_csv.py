import os
import numpy as np
from collections import defaultdict

"""This script is used to create ASCII files from the CSV results of MONICA simulations."""

def csv_to_asc(csv_output_dir, asc_output_path, soil_grid_path, mode="crop", variables=None, target_year=None, target_date=None):
    # Read soil grid metadata
    with open(soil_grid_path, "r") as f:
        header_lines = [next(f) for _ in range(6)]
    soil_grid = np.loadtxt(soil_grid_path, skiprows=6)

    # Get dimensions and initialize a dictionary for grids by cm_count
    nrows, ncols = soil_grid.shape
    grids = defaultdict(lambda: np.full((nrows, ncols), -9999, dtype=float))

    # Iterate through the row folders in the CSV output directory
    for row_folder in os.listdir(csv_output_dir):
        row_path = os.path.join(csv_output_dir, row_folder)
        if not os.path.isdir(row_path):
            continue
        row = int(row_folder)

        for csv_file in os.listdir(row_path):
            csv_path = os.path.join(row_path, csv_file)
            col = int(csv_file.split('-')[1].split('.')[0])

            with open(csv_path, "r") as f:
                lines = f.readlines()

            if mode == "daily":
                if not any(l.strip().startswith("Year,Date,Crop,Stage,TimeUnderAnoxia") for l in lines):
                    print(f"Skipping {csv_path}: no daily section")
                    continue

                for i, line in enumerate(lines):
                    if line.strip().startswith("Year,Date,Crop,Stage"):
                        header = [h.strip() for h in line.strip().split(",")]
                        data_start = i + 2
                        break

                for line in lines[data_start:]:
                    parts = line.strip().split(",")
                    if len(parts) != len(header):
                        continue

                    record = dict(zip(header, parts))
                    year = int(record["Year"])
                    date = record["Date"]

                    if target_year and year != target_year:
                        continue
                    if target_date and date != target_date:
                        continue

                    for var in (variables or ["TimeUnderAnoxia"]):
                        try:
                            value = float(record[var]) if record[var] else -9999
                        except ValueError:
                            value = -9999
                        grids[(year, var, date)][row, col] = value

            elif mode == "crop":
                crop_section_start = None
                for i, line in enumerate(lines):
                    if line.strip().startswith("CM-count,Year,Crop,TimeUnderAnoxia"):
                        crop_section_start = i
                        break

                if crop_section_start is None:
                    print(f"Skipping {csv_path}: no crop section")
                    continue

                crop_data_lines = lines[crop_section_start + 2:]
                for line in crop_data_lines:
                    parts = line.strip().split(",")
                    if len(parts) < 4 or not parts[0].isdigit():
                        continue
                    cm_count = int(parts[0])
                    year = int(parts[1])
                    crop = parts[2]

                    if target_year and year != target_year:
                        continue

                    val = float(parts[3]) if parts[3] else -9999
                    grids[(year, "TimeUnderAnoxia", crop)][row, col] = val

    for key, grid in grids.items():
        if mode == "daily":
            year, var, date = key
            asc_file = os.path.join(asc_output_path, f"{year}_{date}_{var}.asc")
        else:
            year, var, crop = key
            safe_crop = crop.replace("/", "_").replace(" ", "_")  # sanitize filename
            asc_file = os.path.join(asc_output_path, f"{year}_{safe_crop}_{var}.asc")

        with open(asc_file, "w") as asc_file_obj:
            asc_file_obj.writelines(header_lines)
            for row_vals in grid:
                asc_file_obj.write(" ".join([f"{-9999}" if v == -9999 else f"{v:.2f}" for v in row_vals]) + "\n")

        print(f"Written {asc_file}")

    print("ASC files created successfully.")


csv_output_dir = r"C:\Users\giri\Documents\sentinel1data\Marlene_crop\1\MONICA\TUA"
soil_grid_path = "data/germany/Hermes-buek-wr_100_25832_etrs89-utm32n.asc"
asc_output_path = r"C:\Users\giri\Documents\sentinel1data\Marlene_crop\1\MONICA\TUA"

# Make sure the output directory exists
os.makedirs(asc_output_path, exist_ok=True)

# For crop yield
# csv_to_asc(csv_output_dir, asc_output_path, soil_grid_path, mode="crop", variables=["Yield"])

# For daily values
csv_to_asc(csv_output_dir, asc_output_path, soil_grid_path, mode="daily", variables=["TimeUnderAnoxia"])
