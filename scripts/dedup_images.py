import argparse
import logging
import os
import sys
import tempfile

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
    os.makedirs(file_path_config['data_output']['matching_images_csv_filename'], exist_ok=True)
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


if __name__ == "__main__":
    print("Python version:", sys.version)
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_type", dest="output_type", help="'unique': Zip archive to store the unique images "
                                                                    "or `all': Zip archive that includes a folder for "
                                                                    "each unique image, and a subfolder containing all "
                                                                    "duplicates. This is useful when using similarity "
                                                                    "of images", required=True)
    parser.add_argument("--inputs", dest="inputs", nargs="+", help="Zip archives of images to deduplicate",
                        required=True)
    parser.add_argument("--total_image_corpus", dest="image_corpus",
                        help="The entire set of images to deduplicate. This is in contrast to the input, where you may "
                             "only want to process a subset of images, but against the whole corpus of images. If not "
                             "specified, the input will be treated as the entire image corpus.")
    parser.add_argument("--image_similarity", dest="image_threshold", help="Zip archives of images to deduplicate")
    parser.add_argument("--matching_images_file", dest="match_images_file", help=
                                                                "If this parameter is supplied, it will take the list "
                                                                "of matching file IDs and output them to the path."
                                                                "This will override the setting in the "
                                                                "dedup_config.yaml")
    parser.add_argument("--image_ext", dest="image_ext")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    unique_image_hash_pd = pd.DataFrame(columns=['image_file_name', 'hash'])
    image_matching_hash_pd = pd.DataFrame(columns=['match_image_file_name', 'match_hash', 'match_with_image_file_name',
                                                   "match_with_original_hash"])

    DEFAULT_CONFIG_PATH = os.path.join('..\config', 'dedup_config.yaml')
    config = load_config(DEFAULT_CONFIG_PATH)
    MATCH_IMAGE_FULL_PATH = ''
    if args.match_images_file:
        MATCH_IMAGE_FULL_PATH = args.match_images_file
    else:
        MATCH_IMAGE_FULL_PATH = os.path.join(config['data_output']['matching_images_csv_dir'],
                                             config['data_output']['matching_images_csv_filename'])

    LOG_FILE = os.path.join(config['data_output']['dedup_log_file_dir'],
                            config['data_output']['dedup_log_file_name'])

    ZIP_UNIQUE_IMAGE_OUTPUT = os.path.join(config['data_output']['image_output_dir'],
                                           config['data_output']['unique_image_output_filename'])

    ZIP_DUPLICATE_IMAGE_OUTPUT = os.path.join(config['data_output']['image_output_dir'],
                                           config['data_output']['duplicate_image_output_filename'])

    TMP_WRK = os.path.join(config['data_output']['tmp_working_dir'])

    # init file structure
    init_file_structure(config)

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(file_handler)
    logging.info("Application Logging Initialized")
    logging.info("Saving duplicated image list here: %s", MATCH_IMAGE_FULL_PATH)

    for zip_path in args.inputs:

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Iterate over each item
            for entry in zip_ref.infolist():
                name = entry.filename

                # Check if it is a directory, if not then process.
                if not name.endswith('/'):
                    logging.info("Processing image: '%s'", name)
                    image = Image.open(zip_ref.open(entry))
                    image_hash = imagehash.phash(image)
                    logging.info("Hash for image '%s': '%s'", name, image_hash)

                    matched_hashes_pd = unique_image_hash_pd[unique_image_hash_pd['hash'] == image_hash]

                    # if there is a matching hash then put in the image_matching_hash_pd
                    # otherwise not match exists then put in image_hash_pd
                    if not matched_hashes_pd.empty:
                        new_record = pd.Series([name,
                                                image_hash,
                                                matched_hashes_pd.iloc[0]['image_file_name'],
                                                matched_hashes_pd.iloc[0]['hash']],
                                               index=image_matching_hash_pd.columns)

                        # hashes matched, put into image_matching_hash_pd
                        image_matching_hash_pd = pd.concat([image_matching_hash_pd, pd.DataFrame([new_record])],
                                                          ignore_index=True)

                        # Only store file if out_type == 'all' because by definition, if an image matched then it's not
                        # unique
                        if args.output_type.lower() == 'all':
                            with zipfile.ZipFile(ZIP_DUPLICATE_IMAGE_OUTPUT, 'a') as zip_output_match_ref:
                                # root of the matching files is the first encountered filename, and duplicates are
                                # saved in a subfolder called duplicates
                                zip_ref.extract(name, os.path.join(TMP_WRK))
                                logging.info("unique file name = %s", matched_hashes_pd.iloc[0]['image_file_name'])
                                duplicate_file_path = os.path.join(matched_hashes_pd.iloc[0]['image_file_name'],
                                                                   "duplicates",
                                                                   name)
                                zip_output_match_ref.write(os.path.join(TMP_WRK, name), arcname=name)

                    else:
                        new_record = pd.Series([name,
                                                image_hash],
                                               index=unique_image_hash_pd.columns)
                        unique_image_hash_pd = pd.concat([unique_image_hash_pd, pd.DataFrame([new_record])],
                                                         ignore_index=True)
                        # if output is `all` then store the original in a folder of original image file name
                        # and all duplicates are in a subfolder called duplicates. if output is `unique` then just
                        # store in the image in the output folder
                        if args.output_type.lower() == 'all':

                            with zipfile.ZipFile(ZIP_UNIQUE_IMAGE_OUTPUT, 'a') as zip_output_unique_ref:
                                # root of the matching files is the first encountered filename, and duplicates are
                                # saved in a subfolder called duplicates
                                zip_ref.extract(name, os.path.join(TMP_WRK))
                                zip_output_unique_ref.write(os.path.join(TMP_WRK, name), arcname=name)

    logging.info("Total unique images: %s", len(unique_image_hash_pd))
    logging.info("Total duplicates found: %s", len(image_matching_hash_pd))

    # cleanup temp dir
    remove_files_from_dir(TMP_WRK)

    if len(image_matching_hash_pd) > 0:
        image_matching_hash_pd.to_csv(MATCH_IMAGE_FULL_PATH, index=False, header=True, encoding='utf-8', sep=',')

