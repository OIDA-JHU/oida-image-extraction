import subprocess
import re
import sys
import os.path
import os
import argparse
import shlex
import time
from glob import glob
import logging
import zipfile
import tarfile
import tempfile
import shutil
import yaml
from solr_search import SolrSearch


def load_config(config_path):
    with open(config_path, 'r') as file:
        loaded_data = yaml.safe_load(file)

    return loaded_data


def build_partial_load_inputs(partial_load):
    return partial_load


# Extracts images, recursing into zip/tar archives as needed,
# keeping a counter and only writing to output file if between
# the minimum and maximum specified indices (stopping early if
# counter exceeds the latter).  Has to pass along lots of ugly
# configuration info with each recursion, could be much more
# elegant.
def process_file(
        final_ofd,
        current_index,
        fname, 
        fhandle=None, 
        prefix="", 
        temp_path=None, 
        image_exts=[".jpeg", ".jpg", ".png"],
        zip_exts=[".zip", ".pptx", ".xlsx"],
        tar_exts=[".tar", ".tgz", ".tbz2", ".tar.gz", ".tar.bz2"],
        old_exts=[".ppt", ".xls"],        
        min_index=0,
        max_index=None
):
    # Only keep processing if below upper limit/upper limit not set
    if not (max_index and current_index >= max_index):
        ext = re.match(r".*?((\.tar)?\.[^\.]+)$", fname.lower())
        ext = ext.group(1) if ext else None
        name = os.path.join(prefix, fname.strip("/"))
        logging.debug("Processing file '%s'", fname)
        if ext in zip_exts:
            logging.debug("Recursively processing a zip file")
            try:
                with zipfile.ZipFile(fhandle, "r") as nested_ifd:
                    for nested_fname in nested_ifd.namelist():
                        current_index = process_file(
                            final_ofd,
                            current_index,
                            nested_fname,
                            nested_ifd.open(nested_fname, "r"),
                            prefix=name,
                            temp_path=temp_path,
                            min_index=min_index,
                            max_index=max_index,
                            image_exts=image_exts,
                            zip_exts=zip_exts,
                            old_exts=old_exts,
                            tar_exts=tar_exts
                        )
            except zipfile.BadZipFile as bad_zip_error:
                logging.info("Unable to open archive at index at %s for file %s. Err msg: %s",
                            current_index, name, bad_zip_error)
            except Exception as unknown_error:
                logging.info("Unknown error opening archive at index at %s for file %s. Err msg: %s",
                            current_index, name, unknown_error)

        elif ext in old_exts:
            logging.debug("Treating '%s' as old Microsoft format", fname)
            input_fname = os.path.join(temp_path, os.path.basename(fname))
            with open(input_fname, "wb") as ofd:
                ofd.write(fhandle.read())
            pid = subprocess.Popen(
                shlex.split("python -m hachoir.subfile --category image") + [input_fname, temp_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            pid.communicate()
            for output_fname in glob(os.path.join(temp_path, "*")):
                current_index += 1
                if output_fname != input_fname:
                    with open(output_fname, "rb") as nested_ifd:
                        current_index = process_file(
                            final_ofd,
                            current_index,
                            os.path.basename(output_fname), 
                            nested_ifd, 
                            prefix=name, 
                            temp_path=temp_path,
                            min_index=min_index,
                            max_index=max_index,
                            image_exts=image_exts,
                            zip_exts=zip_exts,
                            old_exts=old_exts,
                            tar_exts=tar_exts
                        )
                os.remove(output_fname)
        elif ext in image_exts:
            current_index += 1
            if current_index > min_index:
                logging.debug("Adding image to archive as '%s'", name)
                with final_ofd.open(name, "w") as image_ofd:
                    image_ofd.write(fhandle.read())
            else:
                logging.debug("Skipping image '%s'", name)
        elif ((sys.version_info >= (3,9) and tarfile.is_tarfile(fhandle)) or ext in tar_exts):
            logging.debug("Recursively processing a tar file")
            with tarfile.open(fileobj=fhandle) as nested_ifd:
                for member in nested_ifd:
                    current_index = process_file(
                        final_ofd,
                        current_index,
                        member.name,
                        nested_ifd.extractfile(member),
                        prefix=name,
                        temp_path=temp_path,
                        min_index=min_index,
                        max_index=max_index,
                        image_exts=image_exts,
                        zip_exts=zip_exts,
                        old_exts=old_exts,
                        tar_exts=tar_exts
                    )
        else:
            logging.debug("Skipping file with unknown extension/content ('%s')", fname)
    return current_index


def format_duration(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="inputs", 
        nargs="*",
        help="Any number and mixture of Powerpoint and Excel files to process (or zip/tar files in archive mode). "
             "If specifying input_dir=TRUE, then this should be a directory.",
    )
    parser.add_argument("--output",
                        dest="output",
                        help="Zip file to append extracted images to (will be created if necessary)",
                        required=False)
    parser.add_argument("--input_dir",
                        dest="input_dir",
                        help="A boolean value whether the inputs is a list of file(s) or list of directories.",
                        choices=["TRUE", "FALSE", "true", "false"],
                        required=False)
    parser.add_argument("--partial_load_query",
                        dest="partial_load_query",
                        help="If supplying a partial load query, then a partial load of files will be added to the "
                             "already existing output. This will query the UCSF index and pull in files based on the "
                             "query. If the files already exist, it will skip them and log that they were skipped. "
                             "A separate log will be generated of these files that were added. This argument will "
                             "override any input argument. The output will append a date timestamp to the zip archive "
                             "file name.",
                        required=False
                        )
    parser.add_argument("--start", dest="start", default=0, type=int, help="Which image index to start saving at")
    parser.add_argument("--count", dest="count",  type=int, help="How many images to save")
    parser.add_argument("--image_extensions", dest="image_extensions", nargs="*", default=[".jpg", ".jpeg", ".png"])
    parser.add_argument("--zip_extensions", dest="zip_extensions", nargs="*", default=[".zip", ".xlsx", ".pptx"])
    parser.add_argument("--old_extensions", dest="old_extensions", nargs="*", default=[".ppt", ".xls"])
    parser.add_argument("--tar_extensions", dest="tar_extensions", nargs="*",
                        default=[".tar", ".tgz", ".tbz2", ".tar.bz2", ".tar.gz"])
    parser.add_argument("--log_file",
                        dest="log_file",
                        help="Overrides the parameter `process_log_file` in the process_config.yaml. Include the full "
                             "path and file name e.g. /output/logfile.txt")
    parser.add_argument("--config_file",
                        dest="config_file",
                        help="Overrides the default location of the process_config.yaml.")
    parser.add_argument("--log_level", dest="log_level", choices=["DEBUG", "INFO", "WARN", "ERROR"], default="INFO")
    args = parser.parse_args()

    # Setup configuration
    config_path = os.path.join('..', 'config', 'process_config.yaml')
    if args.config_file:
        config_path = args.config_file

    config = load_config(config_path)

    if not args.log_file:
        args.log_file = os.path.join(config['data_output']['process_log_file'])

    if not args.output:
        args.output = os.path.join(config['data_output']['output_file'])

    if not args.input_dir:
        if os.path.join(config['data_input']['input_dir']):
            args.input_dir = 'TRUE'
            args.inputs.append(os.path.join(config['data_input']['input_dir']))


    # Prints lots of information while running: set the log level to e.g. "WARN" 
    # for less verbosity.
    logging.basicConfig(level=getattr(logging, args.log_level))

    file_handler = logging.FileHandler(args.log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(file_handler)
    logging.info("Application Logging Initialized")

    # The temporary path is used for invoking the command-line
    # 'hachoir' tool.
    temp_path = tempfile.mkdtemp()

    if args.partial_load_query:
        logging.info("Processing partial load query: %s", args.partial_load_query)
        query = SolrSearch(args.partial_load_query)
        query.search(number=config['partial_load']['total_files_download'])
        results = query.ids_and_scores
        args.inputs = build_partial_load_inputs(results)

    current_index = 0
    try:
        with zipfile.ZipFile(args.output, "w") as ofd:
            if args.input_dir.upper() == 'FALSE':
                for fname in args.inputs:
                    logging.info("Processing individual top-level file '%s'", fname)
                    with open(fname, "rb") as ifd:
                        logging.info("Opened top level file '%s'", fname)
                        current_index = process_file(
                            ofd,
                            current_index,
                            fname,
                            fhandle=ifd,
                            temp_path=temp_path,
                            image_exts=args.image_extensions,
                            zip_exts=args.zip_extensions,
                            old_exts=args.old_extensions,
                            tar_exts=args.tar_extensions,
                            min_index=args.start,
                            max_index=args.start + args.count if args.count else None
                        )
                        logging.info("Done processing top-level file '%s'", fname)
            elif args.input_dir.upper() == 'TRUE':
                logging.info("Processing input directory")
                for dir in args.inputs:
                    for dirpath, dirnames, filenames in os.walk(dir):
                        for file_name in filenames:
                            file_path = os.path.join(dirpath, file_name)
                            with open(file_path, "rb") as ifd:
                                logging.info("Open file in input directory '%s'", file_path)
                                current_index = process_file(
                                    ofd,
                                    current_index,
                                    file_path,
                                    fhandle=ifd,
                                    temp_path=temp_path,
                                    image_exts=args.image_extensions,
                                    zip_exts=args.zip_extensions,
                                    old_exts=args.old_extensions,
                                    tar_exts=args.tar_extensions,
                                    min_index=args.start,
                                    max_index=args.start + args.count if args.count else None
                                )
                                logging.info("Processed file at index: %s", current_index)
            else:
                logging.error("No specified input directory or file names.")
    except Exception as e:
        raise e
    finally:
        # Clean up temporary path used for hachoir subprocesses.
        shutil.rmtree(temp_path)

    logging.info("Image extraction run time: " + format_duration(time.time() - start_time))
