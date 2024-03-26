# Introduction: OIDA Image Extraction and Deduplication Tool
These set of scripts: `process_files.py`, `filter_files.py`, and `dedup_images.py` are part of the first stage of the
OIDA Image Collection data pipeline. They can be run independently, but are designed to process the images from Excel
and Powerpoint documents in the Opioid Internet Document Archive and be prepared, analyzed, and curated by researchers.

The intended steps in the processing of documents are the following:

1) Extract Images: Executing the script `scripts/process_files.py` all images will be extracted from the documents 
provided.
2) Filter Images: Executing the script `scripts/filter_files.py` will select only the images with the provided 
arguments.
3) Deduplication: Executing the script `scripts/dedup_images.py` will remove duplicate images based on a MD5 file hash. 
Running this step after filtering will improve overall performance as this step is more time-consuming than filtering.

  
# Image Extraction
The script `scripts/process_files.py` in this repository can be used to extract images from Powerpoint and Excel files 
in both the older "CFB" Microsoft format, and the newer XML-based format (typically distinguished by an additional "x", 
e.g. "file.ppt" versus "file.pptx").  For the approximately 900 example files from the OIDA corpus, the script runs in 
under two minutes on a typical laptop, and extracts approximately 4000 images.

## Running the Image Extraction script
On any computer with a recent version of Python (e.g. >3.6) and Git, first clone this repository and change directory 
into it:

```
git clone https://github.com/comp-int-hum/oida-image-extraction.git
cd oida-image-extraction
```

Then create and load a virtual environment, and install the requirements:

```
python3 -m venv local
source local/bin/activate
pip install -r requirements.txt
```

You can then invoke the script, which requires an output file name for the zip file it will write, and any number of 
unnamed arguments corresponding to files to process:

```
python scripts/process_files.py --output some_output_file.zip file1.xlsx file2.pptx file3.ppt file4.zip file5.tgz ...
```

One can optionally specify, in the stream of images extracted from a given run, at which index to start writing to 
output (i.e. an "offset"), and how many images to keep (this allows massively parallel batch processing).  The set of 
file extensions considered to be images, zip files, tar archives, etc, can be overridden with arguments 
(see "python scripts/process_files.py -h").  

The script also supports specifying a directory to process images. By using the `--input_dir` parameter and using the 
`TRUE` argument. Example usage:

```
python process_files.py --output /output/output.zip --input_dir TRUE /document_input
```

## Input, Processing, and Output
The script currently handles the old and new formats of Microsoft Powerpoint and Excel, which it distinguishes based on 
file extension.  The old formats are searched for known bit-patterns corresponding to file formats 
(using the Hachoir library), while the new formats are unpacked as zip archives and filtered for image extensions.  In 
all cases, each image found in a given input file `FILE_NAME` is extracted with name `FILE_NAME/IMAGE_NAME`, and so 
remains unambiguously associated with its source (nested archives will create longer sequences that will also be unique).

## Partial Loading
To partially process images to be prepared for loading into the Image Collection, using the `partial_load_query` 
parameter will take an argument that is a query string, for example:

```
python process_files.py --output /output/output.zip --partial_load_query 
"ddudate:[2023-11-01T00:00:00Z TO 2023-12-01T00:00:00Z] AND (filename:*xls OR filename:*xlsx) AND industry:Opioids"
```


# Filter Files
Given a zip file of images such as produced by the script, a filtered 
archive can be created with:

```
python scripts/filter_files.py --input some_output.zip --output filtered_output.zip
```

By default, the filter will exclude files with "thumb" in the name, and images with width or height less than 200 pixels
or entropy less than 6.0.  These defaults can be specified differently on the command line, see the script's help 
message for details (i.e. using the "-h" switch).

# Deduplication


# Configuration
Some of the parameters can be configured in the `process_config.yaml`. The script will default to `./config` to look for
this configuration file. It can be overridden by using the `--config_file` parameter. The following is an example
configuration file for the `process_files.py` script:

```
data_output:
  output_file: '/data_output/output.zip'
  process_log_file: '/data/process_log_file.txt'

data_input:
  input_dir: '/input_directory'

partial_load:
  total_files_download: 'all'
  partial_load_root_dir: '/partial_load_directory'
 ```

| Variable               | Required                                         | Can be overridden?                    |
|------------------------|--------------------------------------------------|---------------------------------------|
| output_file            | No                                               | Yes, by using `--output`              |
| process_log_file       | No                                               | Yes, by using `--log_file`            |
| input_dir              | No                                               | Yes, by using `--input_dir`           |
| total_files_download   | Yes, only if a partial load is being performed   | No                                    |
| partial_load_root_dir  | No                                               | Yes, by using `partial_load_root_dir` |


# Known Issues and Extending the Code

It might be worthwhile to be more sophisticated about determining input file format, e.g. using magic bytes, 
particularly if the data sources become less constrained or curated.  This could include the image-extraction stage for
the newer formats, where each zip archive member could be tested, rather than also relying on file extensions.

The Hachoir approach used on the older formats is very general (searching for known patterns at the bit level), and so 
could prove useful for adding arbitrary additional input format handlers (i.e. branches to the main if-statement). Care 
should be taken to ensure that Hachoir is being given uncompressed and/or unencrypted files, otherwise the bit-patterns 
won't be meaningful.

The SCons build system probably won't work out-of-the-box in arbitrary environments, but should give a decent idea of 
how one might run the script over a massive collection on a grid (SLURM etc).

# Authors
The original code was forked from https://github.com/comp-int-hum/oida-image-extraction and was authored by Tom 
Lippincott in the [department of computer science at JHU](https://engineering.jhu.edu/faculty/thomas-lippincott/).

Additional updates, features, and pipeline stages were made by:
- [Tim Sanders, Digital Research and Curation Center JHU](https://www.library.jhu.edu/staff/tim-sanders/)
