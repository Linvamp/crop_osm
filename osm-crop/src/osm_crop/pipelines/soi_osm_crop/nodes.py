"""
This is a boilerplate pipeline 'soi_osm_crop'
generated using Kedro 0.19.2
"""
import os
import re
import csv
import boto3
import shutil
import zipfile
import subprocess
import pandas as pd
from osgeo import gdal

def is_anomaly_file(file_name):
    VALID_FILE_NAME_PATTERN = r'([A-Z])(\d+)([A-Z])(\d+)'
    return '&' in file_name or '_' in file_name or not re.match(VALID_FILE_NAME_PATTERN, file_name)

def get_file_from_zip(zip_file_path):
    valid_extensions = ('.tif', '.tiff')
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.lower().endswith(valid_extensions):
                return zip_ref.open(file_info.filename)

        for file_info in zip_ref.infolist():
            if '/' in file_info.filename:
                folder_name = os.path.dirname(file_info.filename)
                for inner_file in zip_ref.namelist():
                    if inner_file.startswith(folder_name) and inner_file.endswith(('.tif', '.tiff')):
                        return zip_ref.open(inner_file)

def process_extracted_files(directory):
    print(f"directory: {directory}")
    proj_files = os.listdir(directory)
    
    tiff_files = [f for f in proj_files if f.endswith('.tif') or f.endswith('.tiff')]
    if not tiff_files:
        print(f"no tiff files in {directory}")
        for folder_name in proj_files:
            folder_path = os.path.join(directory, folder_name)
            print(f"folder_path: {folder_path}")
            if os.path.isdir(folder_path):
                tiff_files_in_folder = [f for f in os.listdir(folder_path) if f.endswith('.tif') or f.endswith('.tiff')]
                print(f"{len(tiff_files_in_folder)} in {folder_path}")
                if tiff_files_in_folder:
                    tiff_file_path = os.path.join(folder_path, tiff_files_in_folder[0])
                    print(f"tiff file path: {tiff_file_path}")
                    return tiff_file_path
    else:
        latest_tiff_file = sorted(tiff_files, key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)[0]
        print(f"latest_tiff_file: {latest_tiff_file}")
        return os.path.join(directory, latest_tiff_file)

def extract_tiff_node(zip_file_path, Limits_csv):
    CURRENT_DIR = os.getcwd()
    MANUAL = os.path.relpath('data/02_intermediate/MANUAL/', CURRENT_DIR)
    ERROR_FILES_PATH = os.path.relpath('data/02_intermediate/ERROR_FILES_PATH/', CURRENT_DIR)
    VALID_TIF_PATH = os.path.relpath('data/02_intermediate/VALID_TIF_PATH/', CURRENT_DIR)

    for folder in [MANUAL, ERROR_FILES_PATH, VALID_TIF_PATH]:
        if not os.path.exists(folder):
            os.makedirs(folder)
        else:
            print("folders exist")
    
    anamoly_file_name = os.path.basename(zip_file_path)

    if is_anomaly_file(anamoly_file_name):
        destination_path = os.path.join(MANUAL, anamoly_file_name)
        shutil.copy2(zip_file_path, destination_path)
    else:
        file_object = get_file_from_zip(zip_file_path)

        tif_file_name = os.path.basename(file_object.name)
        if anamoly_file_name.split(".")[0] != tif_file_name.split("_")[0]:
            print(f"Tif file name {tif_file_name} does not match with {anamoly_file_name}")
            shutil.copy2(zip_file_path, os.path.join(ERROR_FILES_PATH, anamoly_file_name))
        else:
            if os.path.isfile(zip_file_path) and zipfile.is_zipfile(zip_file_path):
                zip_file_name = os.path.basename(zip_file_path).split(".")[0]
                extract_path = os.path.join(VALID_TIF_PATH,zip_file_name)
                print(f"extract path: {extract_path}")
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    print(f"zip_file_path: {zip_file_path}")
                    # os.path.basename(zip_file_path)
                    zip_ref.extractall(extract_path)
                
                last_tiff_file_path = process_extracted_files(extract_path)
                if last_tiff_file_path:
                    proj_tiff(last_tiff_file_path, Limits_csv)

def proj_tiff(tiff_to_proj, Limits_csv):
    CURRENT_DIR = os.getcwd()
    projected_folder = os.path.relpath('data/03_primary/projected/', CURRENT_DIR)
    os.makedirs(projected_folder, exist_ok=True)
    projected_tif_path = project(tiff_to_proj, projected_folder)
    clip_tiff(projected_tif_path, Limits_csv)

def project(input_path, output_folder):
    output_path = os.path.join(output_folder, f'proj_{os.path.basename(input_path)}')
    gdalwarp_command = [
        'gdalwarp',
        '-t_srs', 'EPSG:4326',
        input_path,
        output_path
    ]
    subprocess.run(gdalwarp_command)
    return output_path

def clip(proj_file_path, limits, clipped_folder):
    clipped_tiff_path = os.path.join(clipped_folder, f'clip_{os.path.basename(proj_file_path)[5:]}')

    gdalwarp_command = [
        'gdalwarp',
        #'--config', 'CHECK_DISK_FREE_SPACE', 'FALSE',
        '-te', str(limits[3]), str(limits[1]), str(limits[2]), str(limits[0]),
        #'-overwrite',  # Add this line to force overwriting the output file if it already exists
        #'-co', 'COMPRESS=DEFLATE',  # You can experiment with different compression methods
        proj_file_path,
        clipped_tiff_path
    ]
    # Run the gdalwarp command
    subprocess.run(gdalwarp_command)

    return clipped_tiff_path

def get_input_key(proj_file_path):
    return os.path.basename(proj_file_path).split("_")[1]

def get_limits(input_text_key, Limits_csv):
    for index, row in Limits_csv.iterrows():  # Use iterrows to iterate over DataFrame rows
        if row['input_text'] == input_text_key:
            upper = float(row['upper_limit'])
            lower = float(row['lower_limit'])
            right = float(row['right_limit'])
            left = float(row['left_limit'])
            return upper, lower, right, left
    return None

def clip_tiff(projected_tif_path, Limits_csv):
    input_text_key = get_input_key(projected_tif_path)
    # print("input_text_key:", input_text_key)
    limits = get_limits(input_text_key, Limits_csv)
    CURRENT_DIR = os.getcwd()
    clipped_folder = os.path.relpath('data/08_reporting/clipped/', CURRENT_DIR)
    # Create 'clipped' folder if it doesn't exist
    os.makedirs(clipped_folder, exist_ok=True)
    clipped_path = clip(projected_tif_path, limits, clipped_folder)
    return (clipped_path)

def download_s3_object(s3, bucket_name, file_key, zip_file_folder, Limits_csv):
    key_value = file_key.replace('Georeferenced_Colour_Raster_Map/TIFF/', '')
    zip_file_path = os.path.join(zip_file_folder, key_value)
    print(f"Destination path: {zip_file_path}")
    # Download the file from S3
    s3.Bucket(bucket_name).download_file(file_key, zip_file_path)
    print(f"Downloaded: {file_key} to {zip_file_path}")
    # Call your extraction function
    extract_tiff_node(zip_file_path, Limits_csv)

def soi_osm_crop(Limits_csv: pd.DataFrame, s3_credentials: pd.DataFrame):
    clipped_paths = []
    CURRENT_DIR = os.getcwd()
    zip_file_folder = os.path.relpath('data/01_raw/zip_files/', CURRENT_DIR)
    os.makedirs(zip_file_folder, exist_ok=True)
    # AWS credentials
    for index, row in s3_credentials.iterrows():  # Use iterrows to iterate over DataFrame rows
        aws_access_key_id = row['Id']
        aws_secret_access_key = row['key']
    aws_region = 'ap-south-1'
    bucket_name = 'ugixsoi'
    prefix = 'Georeferenced_Colour_Raster_Map/TIFF/'

    # Using resource version
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket = s3.Bucket(bucket_name)
    # Use the objects.filter() method directly
    objects = bucket.objects.filter(Prefix=prefix)
    for index, obj in enumerate(objects):
        file_key = obj.key
        clipped_path = download_s3_object(s3, bucket_name, file_key, zip_file_folder, Limits_csv)
        clipped_paths.append(clipped_path)
        print(f"FILE {index+1} PROCESSED")
    #Create a csv with clipped file paths               
    clipped_files = {
        'Clipped File Paths' : clipped_paths
    }
    
    clipped_file_csv = pd.DataFrame(clipped_files)
    return clipped_file_csv

