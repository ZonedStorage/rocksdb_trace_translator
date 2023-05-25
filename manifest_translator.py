#!/usr/bin/python3
import argparse
#import git
import os
import os.path
import sys

VERBOSE = None
OUTPUT = None

class SSTInfo:
  def __init__(self, Level, ID, Size, Key_Start, Key_End, Creation, ColFamily, Create_Version):
    self.Level = (Level)
    self.ID = (ID)
    self.Size = Size
    self.Key_Start = Key_Start
    self.Key_End = Key_End
    self.Creation = Creation
    self.Deletion = -1
    self.ColFamily = ColFamily
    self.Create_Version = Create_Version
    self.Delete_Version = -1
    self.Key_Start_Int = 0
    self.Key_End_Int = 0
    self.Key_Start_Float = 0.0
    self.Key_End_Float = 0.0
    
class AddInfo:
  def __init__(self, Level, ID, Size, Key_Start, Key_End, Creation, Create_Version):
    self.Level = int(Level)
    self.ID = int(ID)
    self.Size = Size
    self.Key_Start = Key_Start
    self.Key_End = Key_End
    self.Creation = Creation
    self.Create_Version = Create_Version
    
class DeleteInfo:
  def __init__(self, Level, ID):
    self.Level = int(Level)
    self.ID = int(ID)

def vprint(*args, **kwargs):
  print(*args, **kwargs)

def setup_argparse():
  parser = argparse.ArgumentParser(description='manifest converter')
  parser.add_argument('-v', '--verbose', action='store_true', help='Print extra information to stdout', default=False)
# RAID/RAIZN parameters
  parser.add_argument('-f', '--file', default="./manifest_sample", help='target file path (e.g. ./manifest_sample)', required=True)
  parser.add_argument('-o', '--output', default="./result", help='result file path (e.g. ./manifest_out)', required=False)
  return parser

def getDictKey(Level, ID, ColFamily):
  return ID * 10000 + ColFamily * 100 + Level

def convertIntRange(val, ratio):
  if val == 0:
    return 0
  return val // ratio

def convertFloatRange(val, ratio):
  if val == 0:
    return 0.0
  return val / ratio

def getEncodeValue(MappingTbl, max_len, Key):
  length = len(Key)
  offset = 0
  encode_length = len(MappingTbl)
  result = 0
  
  if length % 2 == 1:
    val = int(Key[0:1], 16)
    result = val
    offset = 1
    max_len -= 2
  
  while offset < length:
    val = int(Key[offset:offset+2], 16)
    result = (result * encode_length) + val
    offset += 2
    max_len -= 2
    
  while max_len > 0:
    result *= encode_length
    max_len -= 2
  
  return result

def generate_map(MappingTbl, Key):
  length = len(Key)
  offset = 0
  if length % 2 == 1:
    val = int(Key[0:1], 16)
    if val not in MappingTbl:
      MappingTbl.append(val)
    offset = 1
    
  while offset < length:
    val = int(Key[offset:offset+2], 16)
    if val not in MappingTbl:
      MappingTbl.append(val)
    offset += 2

def parse_data(file_path):
  keyDict = {}
  manifestDict = {}

  f = open(file_path, 'r')

  MappingTbl = [0]

  AddInfoArr = []
  DeleteInfoArr = []
  LatestAddTime = -1
  VersionId = 0
  max_keylength = 0

  line = f.readline()
  while True:
    if not line: break

    if "VersionEdit" in line:
      VersionId += 1
    elif "AddFile:" in line:
      data = line.split()
      Level = data[1]
      ID = data[2]
      Size = data[3]
      StartKey = (data[4].split("'")[1])
      EndKey = (data[8].split("'")[1])
      max_keylength = max(max_keylength, len(StartKey), len(EndKey))
      generate_map(MappingTbl, StartKey)
      generate_map(MappingTbl, EndKey)
      
      Creation = 0
      for item in data:
        if "file_creation_time:" in item:
          Creation = int(item.split(":")[1])
          break
      AddInfoArr.append(AddInfo(Level, ID, Size, StartKey, EndKey, Creation, VersionId))
      LatestAddTime = max(LatestAddTime, Creation)
    elif "DeleteFile:" in line:
      data = line.split()
      DeleteInfoArr.append(DeleteInfo(data[1], data[2]))
    elif "ColumnFamily:" in line:
      data = line.split()
      curCF = int(data[1])
      
      for AddItem in AddInfoArr:
        dictkey = getDictKey(AddItem.Level, AddItem.ID, curCF)
        manifestDict[dictkey] = SSTInfo(AddItem.Level, 
                                        AddItem.ID, 
                                        AddItem.Size, 
                                        AddItem.Key_Start, 
                                        AddItem.Key_End, 
                                        AddItem.Creation, 
                                        curCF,
                                        AddItem.Create_Version)
        if curCF not in keyDict:
          keyDict[curCF] = AddItem.Key_Start
        
        keyDict[curCF] = min(AddItem.Key_Start, AddItem.Key_End, keyDict[curCF])
              
      for DeletedItem in DeleteInfoArr:
        dictkey = getDictKey(DeletedItem.Level, DeletedItem.ID, curCF)
        if dictkey in manifestDict:
          # There is no deletion timestamp in the trace. However we know that
          # files become obsolete as soon as compaction finishes, which is
          # approximately the time the last file was created.
          assert LatestAddTime != -1
          manifestDict[dictkey].Deletion = LatestAddTime
          manifestDict[dictkey].Delete_Version = VersionId
        else:
          print("deletion log: missing SST!")
        
      AddInfoArr = []
      DeleteInfoArr = []
      
    line = f.readline()
  f.close()
  
  # sorting encoding table
  MappingTbl.sort()
  max_key_len = max_keylength
  if max_keylength % 2 == 1:
    max_keylength += 1
  
  min_val = -1
  max_val = 0
  # get max, min encoded value
  for key, item in manifestDict.items():
    val = getEncodeValue(MappingTbl, max_keylength, item.Key_Start)
    if min_val == -1:
      min_val = val
    min_val = min(min_val, val)
    max_val = max(max_val, val)
    
    val = getEncodeValue(MappingTbl, max_keylength, item.Key_End)
    min_val = min(min_val, val)
    max_val = max(max_val, val)
  
  max_val -= min_val
  
  maxInt = 2**64 - 1
  maxFloat = int(sys.float_info.max) - 1
  
  ratioInt = 1
  if maxInt < max_val:
    ratioInt = max_val // maxInt

  ratioFloat = 1
  if maxFloat < max_val:
    ratioFloat = max_val // maxFloat  
    
  # convert Int, Float range (8 Bytes limit)
  for key, item in manifestDict.items():
    val = getEncodeValue(MappingTbl, max_keylength, item.Key_Start)
    val -= min_val
    item.Key_Start_Int = convertIntRange(val, ratioInt)
    item.Key_Start_Float = convertFloatRange(val, ratioFloat)
    
    val = getEncodeValue(MappingTbl, max_keylength, item.Key_End)
    val -= min_val
    item.Key_End_Int = convertIntRange(val, ratioInt)
    item.Key_End_Float = convertFloatRange(val, ratioFloat)
  
  return keyDict, manifestDict, max_key_len, MappingTbl
 
def process(args):
  keyDict, manifestDict, max_len, MappingTbl = parse_data(args.file)
  
  f = open(OUTPUT, 'w')
  f.write(f"maxlength: {max_len}\n")
  out = "Encoding Info: "
  for item in MappingTbl:
    out += f"{item}, "
  out += "\n"
  f.write(out)
  f.write("SSTID CF Level size Creation Deletion key_start key_end key_start_float key_end_float Create_Version Delete_Version\n")
 
  for key, item in manifestDict.items():
    min_val = keyDict[item.ColFamily]
   
    f.write("{} {} {} {} {} {} {} {} {} {} {} {}\n".format(item.ID, 
                                                     item.ColFamily, 
                                                     item.Level, 
                                                     item.Size, 
                                                     item.Creation, 
                                                     item.Deletion, 
                                                     item.Key_Start_Int, 
                                                     item.Key_End_Int,
                                                     item.Key_Start_Float, 
                                                     item.Key_End_Float,
                                                     item.Create_Version,
                                                     item.Delete_Version))
    
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

