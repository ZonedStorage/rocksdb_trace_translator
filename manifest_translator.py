#!/usr/bin/python3
import argparse
#import git
import os
import os.path
import sys

VERBOSE = None
OUTPUT = None

class SSTInfo:
  def __init__(self, Level, ID, Size, Key_Start, Key_End, Creation, ColFamily):
    self.Level = (Level)
    self.ID = (ID)
    self.Size = Size
    self.Key_Start = Key_Start
    self.Key_End = Key_End
    self.Creation = Creation
    self.Deletion = -1
    self.ColFamily = ColFamily
    
class AddInfo:
  def __init__(self, Level, ID, Size, Key_Start, Key_End, Creation):
    self.Level = int(Level)
    self.ID = int(ID)
    self.Size = Size
    self.Key_Start = Key_Start
    self.Key_End = Key_End
    self.Creation = Creation
    
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

def getIntKey(strKey):
  result = 0
  if strKey[-16:] == "3030303030303030":
    result = int(strKey[:len(strKey) - 16], 16)
  else:
    result = int(strKey, 16)
  return result

def parse_data(file_path):
  keyDict = {}
  manifestDict = {}

  f = open(file_path, 'r')

  AddInfoArr = []
  DeleteInfoArr = []
  LatestAddTime = -1

  line = f.readline()
  while True:
    if not line: break

    if "AddFile:" in line:
      data = line.split()
      Level = data[1]
      ID = data[2]
      Size = data[3]
      StartKey = getIntKey(data[4].split("'")[1])
      EndKey = getIntKey(data[8].split("'")[1])
      Creation = 0
      for item in data:
        if "file_creation_time:" in item:
          Creation = int(item.split(":")[1])
          break
      AddInfoArr.append(AddInfo(Level, ID, Size, StartKey, EndKey, Creation))
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
                                        AddItem.Creation, curCF)
        if curCF not in keyDict:
          keyDict[curCF] = AddItem.Key_Start
        
        keyDict[curCF] = min(AddItem.Key_Start, AddItem.Key_End, keyDict[curCF])
              
      for DeletedItem in DeleteInfoArr:
        dictkey = getDictKey(DeletedItem.Level, DeletedItem.ID, curCF)
        if dictkey in manifestDict:
          # There is no deletion timestamp in the trace. However we know that
          # files become obsolete as soon as compaction finishes, which is
          # approximately the time the last file was created by compaction.
          #
          # If the below assertion fails, it means there is a VersionEdit that
          # only contains DeleteFile entries. If we need to handle that case, we
          # should probably estimate deletion time according to AddFile entries
          # in an adjacent VersionEdit.
          assert LatestAddTime != -1
          manifestDict[dictkey].Deletion = LatestAddTime
        else:
          print("deletion log: missing SST!")
        
      AddInfoArr = []
      DeleteInfoArr = []
      LatestAddTime = -1
      
    line = f.readline()
  f.close()
  
  return keyDict, manifestDict
 
def process(args):
  keyDict, manifestDict = parse_data(args.file)
  
  f = open(OUTPUT, 'w')
  f.write("SSTID CF Level size Creation Deletion key_start key_end\n")
 
  for key, item in manifestDict.items():
    min_val = keyDict[item.ColFamily]
   
    f.write("{} {} {} {} {} {} {} {}\n".format(item.ID, 
                                               item.ColFamily, 
                                               item.Level, 
                                               item.Size, 
                                               item.Creation, 
                                               item.Deletion, 
                                               item.Key_Start - min_val, 
                                               item.Key_End - min_val))
    
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

