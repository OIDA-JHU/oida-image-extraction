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


logger = logging.getLogger("process_files")


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
        logger.debug("Processing file '%s'", fname)
        if ext in zip_exts:
            logger.debug("Recursively processing a zip file")
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
                logger.info("Unable to open archive at index at %s for file %s. Err msg: %s",
                            current_index, name, bad_zip_error)
            except Exception as unknown_error:
                logger.info("Unknown error opening archive at index at %s for file %s. Err msg: %s",
                            current_index, name, unknown_error)

        elif ext in old_exts:
            logger.debug("Treating '%s' as old Microsoft format", fname)
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
                logger.debug("Adding image to archive as '%s'", name)
                with final_ofd.open(name, "w") as image_ofd:
                    image_ofd.write(fhandle.read())
            else:
                logger.debug("Skipping image '%s'", name)
        elif ((sys.version_info >= (3,9) and tarfile.is_tarfile(fhandle)) or ext in tar_exts):
            logger.debug("Recursively processing a tar file")
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
            logger.debug("Skipping file with unknown extension/content ('%s')", fname)
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
        nargs="+", 
        help="Any number and mixture of Powerpoint and Excel files to process (or zip/tar files in archive mode)",
    )
    parser.add_argument("--output", dest="output", help="Zip file to append extracted images to (will be created if "
                                                        "necessary)", required=True)
    parser.add_argument("--input_dir", dest="input_dir",
                        help="A boolean value whether the inputs is a list of file(s) or list of directories.",
                        default="FALSE",
                        choices=["TRUE", "FALSE", "true", "false"])
    parser.add_argument("--start", dest="start", default=0, type=int, help="Which image index to start saving at")
    parser.add_argument("--count", dest="count",  type=int, help="How many images to save")
    parser.add_argument("--image_extensions", dest="image_extensions", nargs="*", default=[".jpg", ".jpeg", ".png"])
    parser.add_argument("--zip_extensions", dest="zip_extensions", nargs="*", default=[".zip", ".xlsx", ".pptx"])
    parser.add_argument("--old_extensions", dest="old_extensions", nargs="*", default=[".ppt", ".xls"])
    parser.add_argument("--tar_extensions", dest="tar_extensions", nargs="*",
                        default=[".tar", ".tgz", ".tbz2", ".tar.bz2", ".tar.gz"])
    parser.add_argument("--log_level", dest="log_level", choices=["DEBUG", "INFO", "WARN", "ERROR"], default="INFO")
    args = parser.parse_args()

    # Prints lots of information while running: set the log level to e.g. "WARN" 
    # for less verbosity.
    logging.basicConfig(level=getattr(logging, args.log_level))

    # The temporary path is used for invoking the command-line
    # 'hachoir' tool.
    temp_path = tempfile.mkdtemp()

    current_index = 0
    try:
        with zipfile.ZipFile(args.output, "w") as ofd:
            if args.input_dir == 'FALSE':
                for fname in args.inputs:
                    logger.info("Processing individual top-level file '%s'", fname)
                    with open(fname, "rb") as ifd:
                        logger.info("Opened top level file '%s'", fname)
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
                        logger.info("Done processing top-level file '%s'", fname)
            elif args.input_dir == 'TRUE':
                logger.info("Processing input directory")
                for dir in args.inputs:
                    for dirpath, dirnames, filenames in os.walk(dir):
                        for file_name in filenames:
                            file_path = os.path.join(dirpath, file_name)
                            with open(file_path, "rb") as ifd:
                                logger.info("Open file in input directory '%s'", file_path)
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
                                logger.info("Processed file at index: %s", current_index)
            else:
                logger.error("No specified input directory or file names.")
    except Exception as e:
        raise e
    finally:
        # Clean up temporary path used for hachoir subprocesses.
        shutil.rmtree(temp_path)

    logger.info("Image extraction run time: " + format_duration(time.time() - start_time))
