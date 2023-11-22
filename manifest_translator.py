#!/usr/bin/python3
import argparse
#import git
import os
import os.path
import sys
from typing import Final

VERBOSE = None
MAX_CF: Final = 100
MAX_LV: Final = 100

class SSTInfo:
  def __init__(self, level, id, size, key_start, key_end, creation_time, column_family, create_version, create_is_in, trivial_moved):
    self.level = (level)
    self.id = (id)
    self.size = size
    self.key_start = key_start
    self.key_end = key_end
    self.creation_time = creation_time
    self.deletion_time = -1
    self.column_family = column_family
    self.create_version = create_version
    self.delete_version = -1
    self.key_start_int = 0
    self.key_end_int = 0
    self.key_start_decimal = 0.0
    self.key_end_decimal = 0.0
    self.create_is_in = create_is_in
    self.delete_is_in = False
    self.trivial_moved = trivial_moved
    
class CreateInfo:
  def __init__(self, level, id, size, key_start, key_end, creation_time, create_version, create_is_in):
    self.level = int(level)
    self.id = int(id)
    self.size = size
    self.key_start = key_start
    self.key_end = key_end
    self.creation_time = creation_time
    self.create_version = create_version
    self.create_is_in = create_is_in
    
class DeleteInfo:
  def __init__(self, level, id):
    self.level = int(level)
    self.id = int(id)

class Translator:
  def __init__(self, start_time, end_time):
    self.min_key_dict = {}
    self.manifest_dict = {}
    self.encode_tbl = [[] for _ in range(MAX_CF)]
    self.max_key_length = [0] * MAX_CF
    self.latest_creation_time = -1
    self.version_id = 0
    self.start_time = start_time
    self.end_time = end_time
    
  def generate_encode(self, key, column_family):
    length = len(key)
    offset = 0
    if length % 2 == 1:
      val = int(key[0:1], 16)
      if val not in self.encode_tbl[column_family]:
        self.encode_tbl[column_family].append(val)
      offset = 1
      
    while offset < length:
      val = int(key[offset:offset+2], 16)
      if val not in self.encode_tbl[column_family]:
        self.encode_tbl[column_family].append(val)
      offset += 2
      
  def is_in_trace(self, num):
    return self.start_time <= num <= self.end_time
  
  def is_trivial_moved(self, level, id, column_family):
    result = -1
    for level_idx in range(level):
      key = self.get_dict_key(level_idx, id, column_family)
      if key in self.manifest_dict:
        result = level_idx
    return result

  def get_dict_key(self, level, id, column_family):
    return id * (MAX_CF * MAX_LV) + column_family * MAX_LV + level
  
  def convert_val_to_int_range(self, val, ratio):
    if val == 0:
      return 0
    return val // ratio

  def convert_val_to_decimal_range(self, val, ratio):
    if val == 0:
      return 0.0
    return val / ratio

  def get_encode_value(self, encode_tbl, max_len, key):
    length = len(key)
    offset = 0
    encode_length = len(encode_tbl)
    result = 0
    
    if length % 2 == 1:
      val = int(key[0:1], 16)
      result = val
      offset = 1
      max_len -= 2
    
    while offset < length:
      val = int(key[offset:offset+2], 16)
      result = (result * encode_length) + val
      offset += 2
      max_len -= 2
      
    while max_len > 0:
      result *= encode_length
      max_len -= 2
    
    return result
  
  def encoding_key_all(self):
    # sorting encoding table
    for tbl in self.encode_tbl:
      tbl.sort()
    
    for idx in range(MAX_CF):
      if self.max_key_length[idx] % 2 == 1:
        self.max_key_length[idx] += 1
    
    min_val = [-1] * MAX_CF
    max_val = [0] * MAX_CF
    # get max, min encoded value
    for key, item in self.manifest_dict.items():
      val = self.get_encode_value(self.encode_tbl[item.column_family], 
                                  self.max_key_length[item.column_family], 
                                  item.key_start)
      if min_val[item.column_family] == -1:
        min_val[item.column_family] = val
      min_val[item.column_family] = min(min_val[item.column_family], val)
      max_val[item.column_family] = max(max_val[item.column_family], val)
      
      val = self.get_encode_value(self.encode_tbl[item.column_family], 
                                  self.max_key_length[item.column_family], 
                                  item.key_end)
      min_val[item.column_family] = min(min_val[item.column_family], val)
      max_val[item.column_family] = max(max_val[item.column_family], val)

    ratio_int = [1] * MAX_CF
    ratio_decimal = [1] * MAX_CF
    
    max_int = 2**64 - 1
    max_decimal = int(sys.float_info.max) - 1
    
    for idx in range(MAX_CF):  
      max_val[idx] -= min_val[idx]
      
      if max_int < max_val[idx]:
        ratio_int[idx] = max_val[idx] // max_int

      if max_decimal < max_val[idx]:
        ratio_decimal[idx] = max_val[idx] // max_decimal
      
    # convert Int, Float range (8 Bytes limit)
    for key, item in self.manifest_dict.items():
      val = self.get_encode_value(self.encode_tbl[item.column_family], 
                                  self.max_key_length[item.column_family], 
                                  item.key_start)
      val -= min_val[item.column_family]
      item.key_start_int = self.convert_val_to_int_range(val, ratio_int[item.column_family])
      item.key_start_decimal = self.convert_val_to_decimal_range(val, ratio_decimal[item.column_family])
      
      val = self.get_encode_value(self.encode_tbl[item.column_family],
                                  self.max_key_length[item.column_family],
                                  item.key_end)
      val -= min_val[item.column_family]
      item.key_end_int = self.convert_val_to_int_range(val, ratio_int[item.column_family])
      item.key_end_decimal = self.convert_val_to_decimal_range(val, ratio_decimal[item.column_family])
      
    return
    
  def parse_data(self, file_path):
    file = open(file_path, 'r')
    line = file.readline()
    
    create_info_arr = []
    delete_info_arr = []
    
    while line:
      if "VersionEdit" in line:
        self.version_id += 1
      elif "AddFile" in line:
        data = line.split()
        level = data[1]
        id = data[2]
        size = data[3]
        key_start = (data[4].split("'")[1])
        key_end = (data[8].split("'")[1])
       
        creation_time = 0
        for item in data:
          if "file_creation_time:" in item:
            creation_time = int(item.split(":")[1])
            break
        create_info_arr.append(CreateInfo(level, 
                                               id, 
                                               size, 
                                               key_start, 
                                               key_end, 
                                               creation_time, 
                                               self.version_id, 
                                               self.is_in_trace(creation_time)))
        self.latest_creation_time = max(self.latest_creation_time, creation_time)
      elif "DeleteFile:" in line:
        data = line.split()
        level = data[1]
        id = data[2]
        delete_info_arr.append(DeleteInfo(level, id))
      elif "ColumnFamily:" in line:
        data = line.split()
        column_family = int(data[1])
        
        for item in create_info_arr:
          self.generate_encode(item.key_start, column_family)
          self.generate_encode(item.key_end, column_family)
          self.max_key_length[column_family] = max(self.max_key_length[column_family], len(item.key_start), len(item.key_end))
        
          dictkey = self.get_dict_key(item.level, item.id, column_family)
          self.manifest_dict[dictkey] = SSTInfo(item.level, 
                                                item.id, 
                                                item.size, 
                                                item.key_start, 
                                                item.key_end, 
                                                item.creation_time, 
                                                column_family,
                                                item.create_version,
                                                item.create_is_in,
                                                self.is_trivial_moved(item.level, 
                                                                      item.id, 
                                                                      column_family))
          if column_family not in self.min_key_dict:
            self.min_key_dict[column_family] = item.key_start
          
          self.min_key_dict[column_family] = min(item.key_start, item.key_end, self.min_key_dict[column_family])
                
        for item in delete_info_arr:
          key = self.get_dict_key(item.level, item.id, column_family)
          if key in self.manifest_dict:
            # There is no deletion timestamp in the trace. However we know that
            # files become obsolete as soon as compaction finishes, which is
            # approximately the time the last file was created.
            assert self.latest_creation_time != -1
            self.manifest_dict[key].deletion_time = self.latest_creation_time
            self.manifest_dict[key].delete_version = self.version_id
            self.manifest_dict[key].delete_is_in = self.is_in_trace(self.latest_creation_time)
          else:
            print("deletion log: missing SST!")
          
        create_info_arr = []
        delete_info_arr = []
        
      line = file.readline()
    file.close()
    return
    
  def write_output_file(self, output_file):
    f = open(output_file, 'w')
    for idx in range(MAX_CF):
      if self.max_key_length[idx] > 0:
        f.write(f"CF {idx} max_key_length: {self.max_key_length[idx]}\n")
        out = f"CF {idx} Encoding Info: "
        for item in self.encode_tbl[idx]:
          out += f"{item}, "
        out += "\n"
        f.write(out)
    f.write(f"start_time: {self.start_time} end_time: {self.end_time}\n")
    f.write("SSTID CF Level size Creation Deletion key_start_int key_end_int key_start_float key_end_float")
    f.write(" Create_Version Delete_Version Create_trace_window Delete_trace_window Trivial_moved\n")
  
    for key, item in self.manifest_dict.items():
      f.write("{} {} {} {} {} {} {} {} {} {} {} {} {} {} {}\n"
              .format(item.id, 
                      item.column_family, 
                      item.level, 
                      item.size, 
                      item.creation_time, 
                      item.deletion_time, 
                      item.key_start_int, 
                      item.key_end_int,
                      item.key_start_decimal, 
                      item.key_end_decimal,
                      item.create_version,
                      item.delete_version,
                      item.create_is_in,
                      item.delete_is_in,
                      item.trivial_moved))
      
    f.close()
    return

def vprint(*args, **kwargs):
  print(*args, **kwargs)

def setup_argparse():
  parser = argparse.ArgumentParser(description='manifest converter')
  parser.add_argument('-v', '--verbose', action='store_true', help='Print extra information to stdout', default=False)
  parser.add_argument('-f', '--files', nargs='+', help='target files path (e.g. -f ./manifest_sample ./mani_dump)', required=False)
  parser.add_argument('-d', '--directory', help='target directory path (e.g. -d ./manifest_dump)', required=False)
  parser.add_argument('-o', '--output', default="./manifest_out", help='result file path (e.g. ./manifest_out)', required=False)
  parser.add_argument('-s', '--start_time', default=0, help='manifest start trace time')
  parser.add_argument('-e', '--end_time', default=0, help='manifest end trace time')
  return parser

def process(args):
  translator = Translator(int(args.start_time), int(args.end_time))
  
  if args.files is not None:
    for file in args.files:
      print(f"target parsing file: {file}")
      translator.parse_data(file)
  
  if args.directory is not None:
    for file in os.listdir(args.directory):
      print(f"target parsing file in {args.directory}: {file}")
      translator.parse_data(file)

  translator.encoding_key_all()
  
  translator.write_output_file(args.output)

# Validates input and adds additional information to args
def validate_args(args):
  if args.files is not None:
    for file in args.files:
      if os.path.exists(file) == False:
        return False
  
  if args.directory is not None:
    if os.path.exists(args.directory) == False:
      return False
  
  try:
    test = int(args.start_time)
    test = int(args.end_time)
  except ValueError:
    return False
  
  if os.path.exists(args.output):
    os.remove(args.output)
  return True

def main():
  global VERBOSE
  
  parser = setup_argparse()
  args = parser.parse_args()
  
  VERBOSE = args.verbose
  
  if not validate_args(args):
    print('An error occurred during parameter validation, exiting without doing anything...')
    sys.exit(-1)

  process(args)
  sys.exit(0)

if __name__ == '__main__':
  main()

