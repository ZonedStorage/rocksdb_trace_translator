# rocksdb_log

convert key range in manifest file

USAGE "python3 ./manifest_converter.py -f [manifest_dump_file]"

1. parse all key in manifest_dump
2. sort and give index number of all key
3. print output file (default: "convert_result")
  - output contains SST identifier, size of SST, level, index of start key, index of end key
