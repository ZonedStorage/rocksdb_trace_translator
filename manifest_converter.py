#!/usr/bin/python3
import argparse
#import git
import os
import os.path
import sys

VERBOSE = None
OUTPUT = None

class SSTInfo:
  def __init__(self, ID, level, size, key_start, key_end):
    self.ID = ID
    self.level = level
    self.size = size
    self.key_start = key_start
    self.key_end = key_end

def vprint(*args, **kwargs):
  print(*args, **kwargs)

def setup_argparse():
  parser = argparse.ArgumentParser(description='manifest converter')
  parser.add_argument('-v', '--verbose', action='store_true', help='Print extra information to stdout', default=False)
# RAID/RAIZN parameters
  parser.add_argument('-f', '--file', default="./manifest_sample", help='target file path (e.g. ./manifest_sample)', required=True)
  parser.add_argument('-o', '--output', default="./convert_result", help='result file path (e.g. ./manifest_out)', required=False)
  return parser

def parse_data(file_path):
  key_list = []
  manifest = []

  f = open(file_path, 'r')

  line = f.readline()
  while True:
    if not line: break

    if "leveldb" in line:
      line = f.readline()
    elif "level" in line:
      data = line.split()
      level = data[1] + data[2]
      while True:
        line = f.readline()
        if not line: break
        if "level" in line: break
        if "[" not in line: break
        
        data = line.split(":")
        ID = data[0]
        size = data[1].split("[")[0]
        data = line.split("'")
        key_start = data[1]
        key_end = data[3]
        
        manifest.append(SSTInfo(ID, level, size, key_start, key_end))
        if key_start not in key_list:
          key_list.append(key_start)
        if key_end not in key_list:
          key_list.append(key_end)
          
        # for item in range(key_start, key_end + 1, 1):
        #   if item not in key_list:
        #     key_list.append(item)
    else:
      line = f.readline()
  
  f.close()
  
  common_suffix = ''
  min_len = min(map(len, key_list))
  
  for i in range(1, min_len + 1):
    if len(set(s[-i] for s in key_list)) == 1:
      common_suffix = key_list[0][-i] + common_suffix
    else:
      break
  
  print(common_suffix)
  
  int_key_list = []
  length = len(common_suffix)
  for item in key_list:
    l = len(item) - length
    int_key_list.append(int(item[:l], 16))
  
  int_key_list.sort()
  
  return int_key_list, manifest, common_suffix
 
def process(args):
  key_list, manifest, common_suffix = parse_data(args.file)
  
  min_val = min(key_list)
  
  key_dict = {}
  for item in key_list:
    key_dict[item] = item - min_val + 1
   
  f = open(OUTPUT, 'w')
  f.write("SSTID level size key_start key_end\n")
  
  for item in manifest:
    l = len(item.key_start) - len(common_suffix)
    start_val = int(item.key_start[:l], 16)
    l = len(item.key_end) - len(common_suffix)
    end_val = int(item.key_end[:l], 16)
    f.write("{} {} {} {} {}\n".format(item.ID, item.level, item.size, key_dict[start_val], key_dict[end_val]))
    
  f.close()

# Validates input and adds additional information to args
def validate_args(args):
  global OUTPUT
  if os.path.exists(args.file) == False:
    return False
  if os.path.exists(OUTPUT):
    os.remove(OUTPUT)
  return True

def main():
  global VERBOSE
  global OUTPUT
  
  parser = setup_argparse()
  args = parser.parse_args()
  
  OUTPUT = args.output
  VERBOSE = args.verbose
  
  if not validate_args(args):
    print('An error occurred during parameter validation, exiting without doing anything...')
    sys.exit(-1)

  process(args)
  sys.exit(0)

if __name__ == '__main__':
  main()

