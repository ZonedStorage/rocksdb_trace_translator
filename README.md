translate key range in manifest file

USAGE "python3 ./manifest_translator.py -f [manifest_dump_file]"

1. parse all key in manifest_dump
2. allocate new value of all key
  - remove common suffix strings (e.g., db_bench Fill unused area with '30' pattern)
  - get min value of key
  - new value is defined with key(casting to int type) - min value
3. print output file (default: "result")
  - output contains SST identifier, Column Family, size of SST, level, index of start key, index of end key, Creation/Deletion Time
