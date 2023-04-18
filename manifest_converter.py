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
        key_start = int(data[1], 16)
        key_end = int(data[3], 16)
        
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
  key_list.sort()
  
  return key_list, manifest
 
def process(args):
  key_list, manifest = parse_data(args.file)
  
  key_dict = {}
  index = 0
  for item in key_list:
    key_dict[item] = index
    index += 1
   
  f = open(OUTPUT, 'w')
  f.write("SSTID level size key_start key_end\n")
  
  for item in manifest:
    f.write("{} {} {} {} {}\n".format(item.ID, item.level, item.size, key_dict[item.key_start], key_dict[item.key_end]))
    
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

