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
from uuid import uuid4
from osgeo import gdal

bucket_name_dest = 'ugixsoiprocessed'
UNIQUE_IDENTIFIER = str(uuid4())

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

def extract_tiff_node(zip_file_path, Limits_csv, s3_resource_dest):
    CURRENT_DIR = os.getcwd()
    MANUAL = os.path.relpath('data/processed/MANUAL/', CURRENT_DIR)
    ERROR_FILES_PATH = os.path.relpath('data/processed/ERROR_FILES_PATH/', CURRENT_DIR)
    VALID_TIF_PATH = os.path.relpath('data/processed/VALID_TIF_PATH/', CURRENT_DIR)

    for folder in [MANUAL, ERROR_FILES_PATH, VALID_TIF_PATH]:
        if not os.path.exists(folder):
            os.makedirs(folder)
        else:
            print("folders exist")
    
    anamoly_file_name = os.path.basename(zip_file_path)
    zip_byte_data = None
    with open(zip_file_path, "rb") as file_obj:
        zip_byte_data = file_obj.read()

    if is_anomaly_file(anamoly_file_name):
        destination_path = os.path.join(MANUAL, anamoly_file_name)
        shutil.copy2(zip_file_path, destination_path)
        #copy the file to  ugixsoiprocessed bucket
        s3_resource_dest.Bucket(bucket_name_dest).put_object(Key=f'{UNIQUE_IDENTIFIER}/MANUAL/{anamoly_file_name}', Body=zip_byte_data)


    else:
        file_object = get_file_from_zip(zip_file_path)

        tif_file_name = os.path.basename(file_object.name)
        if anamoly_file_name.split(".")[0] != tif_file_name.split("_")[0]:
            print(f"Tif file name {tif_file_name} does not match with {anamoly_file_name}")
            shutil.copy2(zip_file_path, os.path.join(ERROR_FILES_PATH, anamoly_file_name))
            #copy the file to  ugixsoiprocessed bucket
            s3_resource_dest.Bucket(bucket_name_dest).put_object(Key=f'{UNIQUE_IDENTIFIER}/ERROR/{anamoly_file_name}', Body=zip_byte_data)
            


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
                    return proj_tiff(last_tiff_file_path, Limits_csv)

def proj_tiff(tiff_to_proj, Limits_csv):
    CURRENT_DIR = os.getcwd()
    projected_folder = os.path.relpath('data/processed/projected/', CURRENT_DIR)
    os.makedirs(projected_folder, exist_ok=True)
    projected_tif_path = project(tiff_to_proj, projected_folder)
    return clip_tiff(projected_tif_path, Limits_csv)

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
    clipped_folder = os.path.relpath('data/processed/clipped/', CURRENT_DIR)
    # Create 'clipped' folder if it doesn't exist
    os.makedirs(clipped_folder, exist_ok=True)
    clipped_path = clip(projected_tif_path, limits, clipped_folder)
    return compress_geotiff(clipped_path)

def compress_geotiff(clipped_file):
    CURRENT_DIR = os.getcwd()
    compressed_file_folder = os.path.relpath('data/processed/compressed/', CURRENT_DIR)
    os.makedirs(compressed_file_folder, exist_ok=True)
    compressed_file = os.path.join(compressed_file_folder, f'comp_{os.path.basename(clipped_file)}')
    # Define the command
    command = ['gdal_translate', '-co', 'COMPRESS=LZW', clipped_file, compressed_file]
    subprocess.run(command, check=True)
    print("Compression successful.")
    return compressed_file


def download_s3_object(s3, bucket_name, file_key, zip_file_folder, Limits_csv, s3_resource_dest):
    key_value = file_key.replace('Georeferenced_Colour_Raster_Map/TIFF/', '')
    zip_file_path = os.path.join(zip_file_folder, key_value)
    print(f"Download file: {file_key}")
    print(f"Destination path: {zip_file_path}")
    # Download the file from S3
    s3.Bucket(bucket_name).download_file(file_key, zip_file_path)
    print(f"Downloaded: {file_key} to {zip_file_path}")
    # Call your extraction function
    return extract_tiff_node(zip_file_path, Limits_csv, s3_resource_dest)

def soi_osm_crop(Limits_csv: pd.DataFrame, ugixsoi_s3_cred: pd.DataFrame, ugixsoiprocessed_s3_cred: pd.DataFrame):

    # AWS credentials
    for index, row in ugixsoi_s3_cred.iterrows():
        aws_access_key_id_source = row['Access key ID']
        aws_secret_access_key_source = row['Secret access key']
    
    for index, row in ugixsoiprocessed_s3_cred.iterrows():
        aws_access_key_id_dest = row['aws_access_key_id']
        aws_secret_access_key_dest = row['aws_secret_access_key']
    
    aws_region = 'ap-south-1'
    bucket_name_source = 'ugixsoi'
    prefix = 'Georeferenced_Colour_Raster_Map/TIFF/'

    # Using resource version
    s3_resource_source = boto3.resource('s3', aws_access_key_id=aws_access_key_id_source, aws_secret_access_key=aws_secret_access_key_source, region_name=aws_region)
    s3_resource_dest = boto3.resource('s3', aws_access_key_id=aws_access_key_id_dest, aws_secret_access_key=aws_secret_access_key_dest, region_name=aws_region)
    print("connected to both resources")
    bucket_source = s3_resource_source.Bucket(bucket_name_source)
    clipped_paths = []

    # Use the objects.filter() method directly
    objects = bucket_source.objects.filter(Prefix=prefix)
    for index, obj in enumerate(objects):
        CURRENT_DIR = os.getcwd()
        zip_file_folder = os.path.relpath('data/processed/zip_files/', CURRENT_DIR)
        os.makedirs(zip_file_folder, exist_ok=True)
   
        file_key = obj.key
        clipped_path = download_s3_object(s3_resource_source, bucket_name_source, file_key, zip_file_folder, Limits_csv, s3_resource_dest)
        if clipped_path:
            print(f"clipped_path ==> {clipped_path}")
            clipped_file_name = clipped_path.split("\\")[-1]
            with open(clipped_path, "rb") as clipped_file_obj:
                s3_resource_dest.Bucket(bucket_name_dest).put_object(Key=f'{UNIQUE_IDENTIFIER}/COMPRESSED_CLIP/{clipped_file_name}', Body=clipped_file_obj.read())
            clipped_paths.append(clipped_path)
        process_folder = os.path.relpath('data/processed', CURRENT_DIR)
        shutil.rmtree(process_folder)
        print(f"FILE {index+1} PROCESSED")

    #Create a csv with clipped file paths               
    clipped_files = {
        'Clipped File Paths' : clipped_paths
    }
    
    clipped_file_csv = pd.DataFrame(clipped_files)
    print(f"Uploaded files to S3 under unique path id - {UNIQUE_IDENTIFIER}")
    return clipped_file_csv

