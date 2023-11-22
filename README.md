translate key range in manifest file

USAGE "python3 ./manifest_translator.py -f [manifest_dump_file, ...]"

options are below:
-f --files "target file list"
-d --directory "target directory"
-o --output "output file path (default: "./manifest_out")
-s --start_time "target trace start time (default: 0)
-e --end_time "target trace end time (default: 0)

* If a default is defined, it can be optionally applied
* The file list and directory can be simultaneously applied, scanning all relevant files

1. parse all key in manifest_dump
  - Check for trivial move and indicate the previous level where it was located
  - Verify if it's included within the trace window
2. allocate new value of all key
  - Collect the chars used in keys for each column family
  - encode all key, then get range(min - max) for each column family
  - New keys are cast as integers and floats based on a determined ratio within a specified range
3. print output file (default: "manifest_out")
  - output contains SST identifier, Column Family, size of SST, level, index of start key, index of end key, Creation/Deletion Time