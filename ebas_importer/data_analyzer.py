import os
import shutil
import utilities
from datetime import datetime
from tqdm import tqdm
import pandas as pd


class EbasFtpDataAnalyzer:
   def __init__(self, data_path):
      self.data_path = data_path
      self.site_infor = None
   
   def exam_site_files(self):
      res = []
      for k in self.site_infor.keys():
         size = 0
         for f in self.site_infor[k]["files"].keys():
            size+= os.path.getsize(os.path.join(self.data_path, f))
         res.append(
            {"site":k,
               "size (MB)":size/1024/1024,
               "files": len(list(self.site_infor[k]["files"].keys()))})
      
      res =pd.DataFrame(res)
      res.to_csv("site_file_size.csv")
   
   def summary_sites(self, exporting=True):
      pass
   
   # def gene_site_df(self):
   #    res = {}
   # for k in tqdm(self.site_infor.keys(), desc="creating site_df..."):
   #    components = []
   #    for f in self.site_infor[k]["files"].keys():
   #       content = self.site_infor[k]["files"][f]["content"]
   #       for c in content:
   #          components.append(c["component"])
   #    components = list(set(components))
      
   #    res[k]={
   #       "id": self.site_infor[k]["id"],
   #       "name": self.site_infor[k]["name"],
   #       "country": self.site_infor[k]["country"],
   #       "land_use": self.site_infor[k]["land_use"],
   #       "station_setting": self.site_infor[k]["station_setting"],
   #       "alt": self.site_infor[k]["alt"],
   #       "lat": self.site_infor[k]["lat"],
   #       "lon": self.site_infor[k]["lon"],
   #       "file_num": self.site_infor[k]["file_num"],
   #       "components": components
   #    }
   # self.site_df = pd.DataFrame(res).T
   
   def summary_attr(self, attr):
      # attr can be anything in contents: matrix, res_code, unit, var, component
      res = []
      for site in self.site_infor.keys():
         files = self.site_infor[site]["files"]
         for file in files.keys():
            contents = files[file]["contents"]
            for c in contents:
               res.append(c[attr])
      res = list(set(res))
      res.sort()
      return res
   
         