import utilities
import pandas as pd
import numpy as np
import json
from itertools import repeat
import pickle
import lzma

from .data_importer import *
from .data_checker import *
from .data_analyzer import *
from .value_index import *


__all__ =[
   "EbasData",
]

class EbasData:
   def __init__(self, data_path, file_type = "nc", compression="xz"):
      # store parameters
      self.data_path = data_path
      self.file_type = file_type
      self.compression = compression
      
      # make necessary directory
      if not os.path.exists(os.path.join(self.data_path,"archived")):
               os.makedirs(os.path.join(self.data_path,"archived"))
      if not os.path.exists("ebas_proj_dump"):
            os.makedirs("ebas_proj_dump") 
      
      
      self.site_infor = None
      self.site_infor_detailed = None
   
      self.data_checker = EbasFtpDataChecker(data_path)
      self.data_importer = EbasFtpDataImporter(data_path, compression)
      self.data_analyzer = EbasFtpDataAnalyzer(data_path)
      self.value_index = ValueIndex()
      
      self.raw_data_files = os.listdir(self.data_path)

   def create_value_index(self):
      print("creating value index...")
      matrix = self.data_analyzer.summary_attr("matrix")
      unit = self.data_analyzer.summary_attr("unit")
      res_code = self.data_analyzer.summary_attr("res_code")
      component = self.data_analyzer.summary_attr("component")
      site = list(self.site_infor.keys())
      
      self.value_index.update_index("matrix", matrix)
      self.value_index.update_index("unit", unit)
      self.value_index.update_index("res_code", res_code)
      self.value_index.update_index("component", component)
      self.value_index.update_index("site", site)
      
      pass

   def dump_site_infor(self):
      with open("site_infor.json","w") as f:
         json.dump(self.site_infor, f, indent=4,  sort_keys=True, default=str) 
   
   def load_files(self,files):
      # load data with file instead of gathering information from raw datafiles
      print("Loading data from files...")
      for attr, file in files:
         data = None
         if file.endswith("csv"):
            data = pd.read_csv(file)
         elif file.endswith("xz"):
            with lzma.open(file, "rb") as pickle_file:
               data = pickle.load(pickle_file)
         elif file.endswith("json"):
            with open(file,"r") as json_file:
               data = json.load(json_file)
         
         setattr(self, attr, data)
         print(f"{file:20} is loaded for {attr:>20}.")
         
         self.data_analyzer.site_infor = self.site_infor

   # todo:
   def check_updates(self, download=False, print_file=True):
      new, archive = self.data_checker.check_updates(download)
      if print_file:
         print("Files need to be downloaded:", new)
         print("Files need to be archived", archive)
      # must update local files because they were updated.
      self.raw_data_files = os.listdir(self.data_path)
      
   
   def get_site_infor(self, exporting ="xz", use_value_index=False):
      print("-"*100)
      print("Gathering site information...")
      # filter out non ".nc" files
      self.data_importer.use_value_index = use_value_index
      files = list(filter(lambda x: x.endswith(self.file_type), self.raw_data_files))
      res = utilities.run_mp(self.data_importer.get_indexing, files, self.data_importer.combine_infor)
      
      self.site_infor = res
      bad =[]
      for k in res.keys():
         if k.startswith(self.data_path):
            bad.append(k)

      print(f"site number : {len(res.keys())}")
      if len(bad)>0:
         print(f"Bad files :{len(bad)}")
         for b in bad:
            print(b)
      
      print("Dumping data to disk...")      
      if exporting =="xz":
         with lzma.open("site_infor.xz", "wb") as pickle_file:
            pickle.dump(self.site_infor, pickle_file)
         print("Data is written to 'site_infor.xz'.")
      else:
         with open("site_infor.json","w") as f:
            json.dump(res, f, indent=4,  sort_keys=True, default=str)  
         print("Data is written to 'site_infor.json'.")
      
      self.data_analyzer.site_infor = self.site_infor
      
      
   def import_site_data(self):
      files = []
      for k in self.site_infor.keys():
         files.append(self.site_infor[k]["files"])
      
      print("Importing datafile of each site...")
      utilities.run_mp(self.data_importer.get_site_data, files)
   
   