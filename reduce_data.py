import os
from pathlib import Path
import subprocess
import shutil

def create_filelist(folder_path, data_type="CRDS", extension="dat"):
    filepaths = [f for f in Path(folder_path).glob(f'**/*.{extension}')]

    oversize = []
    small = []
    for f in filepaths:
        size_in_MB = (f.stat().st_size)/(1024*1024)
        if size_in_MB > 10:
            oversize.append(f)
        else:
            small.append(f)

    resized_folder = Path(f"resized_{data_type}")
    resized_folder.mkdir(exist_ok=True)

    if data_type == "CRDS":
        for f in oversize:
            input_path = str(f)
            output_path = str(resized_folder.joinpath(f.name))
            # subprocess.call([f"awk 'NR == 1 || NR == 2 || NR == 3 || NR % 15 == 0' {input_path} > {output_path}"])
            os.system(f"awk 'NR == 1 || NR == 2 || NR == 3 || NR % 20 == 0' {input_path} > {output_path}")

        # Copy the small files for easy uploading
        for f in small:
            shutil.copy(f, resized_folder.joinpath(f.name))
    elif data_type == "GC":
        filepaths = []
        eight_meg = 10*1024*1024
        # Find all files in
        for f in Path(folder_path).glob(f"**/*.C"):
            if "precisions" in f.name:
                # Remove precisions section and ensure file exists
                data_filename = str(f).replace(".precisions", "")
                data_path = Path(data_filename)
                if data_path.exists() and data_path.stat().st_size < eight_meg:
                    filepaths.append((Path(data_filename), f))

        # Copy precision and data files over
        for data, precision in filepaths:
            shutil.copy(data, resized_folder.joinpath(data.name))
            shutil.copy(precision, resized_folder.joinpath(precision.name))


# def process_files(file_list, data_type="CRDS"):
#     resized_folder = Path(f"resized_{data_type}")
#     resized_folder.mkdir(exist_ok=True)

#     for f in file_list:
#         input_path = str(f)
#         output_path = str(resized_folder.joinpath(f.name))
#         # subprocess.call([f"awk 'NR == 1 || NR == 2 || NR == 3 || NR % 15 == 0' {input_path} > {output_path}"])
#         os.system(f"awk 'NR == 1 || NR == 2 || NR == 3 || NR % 15 == 0' {input_path} > {output_path}")

if __name__ == "__main__":
    # create_filelist("/home/gar/Documents/Devel/hugs/raw_data/CRDS_picarro", data_type="CRDS")
    create_filelist("/home/gar/Documents/Devel/hugs/raw_data/GC_GCMD/data", data_type="GC")




