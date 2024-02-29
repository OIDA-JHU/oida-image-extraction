import argparse
import logging
import os
import hashlib
import sys
import time
import uuid
from pathlib import Path

import yaml
import zipfile
import pandas as pd
from PIL import Image
import imagehash


def load_config(config_path):
    with open(config_path, 'r') as file:
        loaded_data = yaml.safe_load(file)

    return loaded_data


def init_file_structure(file_path_config):
    os.makedirs(file_path_config['data_output']['process_images_csv_filename'], exist_ok=True)
    os.makedirs(file_path_config['data_output']['tmp_working_dir'], exist_ok=True)
    os.makedirs(file_path_config['data_output']['image_output_dir'], exist_ok=True)
    os.makedirs(file_path_config['data_output']['dedup_log_file_dir'], exist_ok=True)


def remove_files_from_dir(dir_path):
    for file_name in os.listdir(dir_path):
        file_path = os.path.join(dir_path, file_name)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error: {file_path} : {e.strerror}")


def format_duration(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


def output_files(tmp_wrk_path, output_path, df):
    file_error_cnt = 0
    for row in df.itertuples(index=True, name='Pandas'):
        try:
            with zipfile.ZipFile(output_path, 'a') as zip_output_unique_ref:
                file_name_ext = row.image_id + row.file_ext
                zip_output_unique_ref.write(os.path.join(tmp_wrk_path, file_name_ext), arcname=file_name_ext)
                logging.info("added file = %s (image_id= %s), to output", row.original_file_name, row.image_id)
        except Exception as ex:
            logging.info("Unable to open %s during processing of %s (image_id= %s), error: %s",
                         output_path, row.original_file_name, row.image_id, str(ex))
            file_error_cnt = file_error_cnt + 1
    return file_error_cnt


if __name__ == "__main__":
    start_time = time.time()
    print("Python version:", sys.version)
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_type", dest="output_type", help="'unique': Zip archive to store the unique images "
                                                                  "or `all': Zip archive that includes a folder for "
                                                                  "each unique image, and a subfolder containing all "
                                                                  "duplicates. This is useful when using similarity "
                                                                  "of images", required=True)
    parser.add_argument("--inputs", dest="inputs", nargs="+", help="Zip archives of images to deduplicate",
                        required=True)

    # TODO: Need ability to do partial data deduping. New data to be compared against what already has been processed.
    '''
    parser.add_argument("--total_image_corpus", dest="image_corpus",
                        help="The entire set of images to deduplicate. This is in contrast to the input, where you may "
                             "only want to process a subset of images, but against the whole corpus of images. If not "
                             "specified, the input will be treated as the entire image corpus.")
    '''
    parser.add_argument("--config_file", dest="config_file_loc", help="Override the default location of the config "
                                                                      "file. Include the file name in the path.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    image_df = pd.DataFrame(columns=['original_file_name', 'image_id', 'file_ext', 'hash'])
    image_unique_df = pd.DataFrame(columns=['original_file_name', 'image_id', 'file_ext', 'hash'])
    image_dup_df = pd.DataFrame(columns=['original_file_name', 'image_id', 'file_ext', 'hash'])

    DEFAULT_CONFIG_PATH = ""
    if args.config_file_loc:
        DEFAULT_CONFIG_PATH = args.config_file_loc
    else:
        DEFAULT_CONFIG_PATH = os.path.join('..', 'config', 'dedup_config.yaml')
    config = load_config(DEFAULT_CONFIG_PATH)

    PROCESS_IMAGE_FULL_PATH = os.path.join(config['data_output']['output_image_csv_dir'],
                                           config['data_output']['process_images_csv_filename'])

    UNIQUE_IMAGE_FULL_PATH = os.path.join(config['data_output']['output_image_csv_dir'],
                                          config['data_output']['unique_images_csv_filename'])

    DUPLICATE_IMAGE_FULL_PATH = os.path.join(config['data_output']['output_image_csv_dir'],
                                             config['data_output']['duplicate_images_csv_filename'])

    LOG_FILE = os.path.join(config['data_output']['dedup_log_file_dir'],
                            config['data_output']['dedup_log_file_name'])

    ZIP_UNIQUE_IMAGE_OUTPUT = os.path.join(config['data_output']['image_output_dir'],
                                           config['data_output']['unique_image_output_filename'])

    ZIP_DUPLICATE_IMAGE_OUTPUT = os.path.join(config['data_output']['image_output_dir'],
                                              config['data_output']['duplicate_image_output_filename'])

    IMAGE_OUTPUT_DIR = os.path.join(config['data_output']['image_output_dir'])

    TMP_WRK = os.path.join(config['data_output']['tmp_working_dir'])

    # count all errors
    error_cnt = 0

    # init file structure
    init_file_structure(config)

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(file_handler)
    logging.info("Application Logging Initialized")
    logging.info("Saving processed image list here: %s", PROCESS_IMAGE_FULL_PATH)
    logging.info("Saving unique image list here: %s", UNIQUE_IMAGE_FULL_PATH)
    logging.info("Saving duplicate image list here: %s", DUPLICATE_IMAGE_FULL_PATH)

    matched_hashes_pd = pd.DataFrame()
    for zip_path in args.inputs:

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Iterate over each item
            for entry in zip_ref.infolist():
                name = entry.filename
                image_id = str(uuid.uuid4())
                ext = Path(name).suffix
                image_id_ext = image_id + ext

                if not name.endswith('/'):
                    logging.info("Processing image: '%s', image_id: %s", name, image_id)

                    try:
                        zip_ref.extract(name, TMP_WRK)
                        tmp_path_name = str(os.path.join(TMP_WRK, image_id_ext))
                        os.rename(str(os.path.join(TMP_WRK, name)), tmp_path_name)

                        hash_md5 = hashlib.md5()
                        with open(tmp_path_name, 'rb') as f:
                            for chunk in iter(lambda: f.read(4096), b""):
                                hash_md5.update(chunk)

                        hash = str(hash_md5.hexdigest())

                        new_record = pd.Series([name,
                                                image_id,
                                                ext,
                                                hash
                                                ],
                                               index=image_df.columns)

                        image_df = pd.concat([image_df, pd.DataFrame([new_record])], ignore_index=True)
                    except Exception as ex:
                        logging.info("Error processing image. Name = %s, ImageID = %s ", name, image_id)
                        error_cnt = error_cnt + 1

    # get all the unique images into a DF
    image_unique_df = image_df.drop_duplicates(subset=['hash'])

    # get all the duplicated images into a DF
    image_dup_mask = ~image_df['image_id'].isin(image_unique_df['image_id'])
    image_dup_df = image_df[image_dup_mask]

    error_cnt = error_cnt + output_files(TMP_WRK, ZIP_UNIQUE_IMAGE_OUTPUT, image_unique_df)
    error_cnt = error_cnt + output_files(TMP_WRK, ZIP_DUPLICATE_IMAGE_OUTPUT, image_dup_df)

    logging.info("Total images processed: %s", len(image_df))
    logging.info("Total unique images: %s", len(image_unique_df))
    logging.info("Total duplicates found: %s", len(image_dup_df))
    logging.info("Total errors: %s", error_cnt)

    # cleanup temp dir
    remove_files_from_dir(TMP_WRK)
    os.rmdir(TMP_WRK)

    if len(image_df) > 0:
        image_df.to_csv(PROCESS_IMAGE_FULL_PATH, index=False, header=True, encoding='utf-8', sep=',')

    if len(image_unique_df) > 0:
        image_unique_df.to_csv(UNIQUE_IMAGE_FULL_PATH, index=False, header=True, encoding='utf-8', sep=',')

    if len(image_dup_df) > 0:
        image_dup_df.to_csv(DUPLICATE_IMAGE_FULL_PATH, index=False, header=True, encoding='utf-8', sep=',')

    logging.info("Image extraction run time: " + format_duration(time.time() - start_time))
