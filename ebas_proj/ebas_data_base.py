import pickle
import lzma
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from ebas_importer import EbasData
import utilities
import json
import time

from ebas_importer.value_index import *
from .ebas_data_file import EbasFiles

class EbasDataBase:
   def __init__(self, site_infor_path, data_path ="ebas_proj_dump", lazy_loading=True, compression='xz'):
      self.site_infor_path = site_infor_path
      self.data_path = data_path
      self.lazy_loading = lazy_loading
      self.compression = compression
      
      self.db={}
      self.db_index={}
      self.selected = {}
      
      self.summary = {}
      self.selected_summary = {}
      
      self.value_index = ValueIndex()
      
      self.init_db()
      
   
   def init_db(self):
      print("init database...")
      print(f"\t{len(os.listdir(self.data_path))} files in the data path {self.data_path}.")
      print(f"\t{self.compression} compression method is used in for the data file.")
      
      print("load site information...")
      site_infor = EbasFiles.load_file(file={"name":"site_infor",
                                       "path":self.site_infor_path})
      self.site_infor = site_infor["data"]
      
      if self.lazy_loading:
         print("lazy loading is used, data files will be loaded when necessary.")
         print("load ebas data index...")
      else:
         print("load all ebas data files...")
         
      files = EbasFiles.get_load_files(data_path=self.data_path, 
                                       selected="all", 
                                       loaded_db=[], 
                                       full_db=list(self.site_infor.keys()), 
                                       compression=self.compression,
                                       lazy_loading=self.lazy_loading)
      
      if self.lazy_loading and os.path.exists("ebas_db_index.dump"):
         db_index = EbasFiles.load_file(file={"name":"db_index",
                                       "path":"ebas_db_index.dump"})
         self.db_index = db_index["data"]
      else:
         db_index, db = EbasFiles.load_files(files=files, 
                                             lazy_loading=self.lazy_loading)
         self.db_index.update(db_index)
         self.db.update(db)
      
      if not os.path.exists("ebas_db_index.dump"): 
         print("dumping database index...")
         with open("ebas_db_index.dump", "wb") as pickle_file:
            pickle.dump(self.db_index, pickle_file)
      
      print("gathering database summary...")
      self.summary = self.db_summary()
   
   def db_summary(self, all =True):
      # generate summary for all sites or selected sites
      if all:
         items = self.site_infor.keys()
      else:
         items =self.selected
      res ={"country":[],"site":[]}
      
      for k in items:
         res["country"].append(self.site_infor[k]["country"])
         res["site"].append({
            "country":self.site_infor[k]["country"],
            "land_use":self.site_infor[k]["land_use"],
            "station_setting":self.site_infor[k]["station_setting"],
            "name":self.site_infor[k]["name"],
            "id":self.site_infor[k]["id"],
            "lat":self.site_infor[k]["lat"],
            "lon":self.site_infor[k]["lon"],
            "alt":self.site_infor[k]["alt"],
            })
      res["country"]= list(set(res["country"]))
      res["components"] =self.summary_attr("component")
      res["matrix"] = self.summary_attr("matrix")
      res["st"] = np.min(self.summary_attr("st"))
      res["ed"] = np.max(self.summary_attr("ed"))
      
      
      print(f"{len(res['site']):<10} sites included in current database.")
      print(f"{len(res['components']):<10} components included in current database.")
      print(f"{len(res['matrix']):<10} matrix included in current database.")
      print(f"{len(res['country']):<10} country included in current database.")
      print(f"Current database ranges from {np.datetime_as_string(res['st'], unit='D'):10} to {np.datetime_as_string(res['ed'], unit='D'):10}.")
      
      return res
      
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
      if attr in ["matrix", "res_code", "unit", "component"]:
         res = self.value_index.convert_list(attr, res)
      return res
   
   def select_db(self, condition):
      res = {}
      condition_key = list(condition.keys())
      site_selector_key = ["id", "name", "land_use", "station_setting","country"]
      site_selector_key = list(set(site_selector_key) & set(condition_key))
            
      measure_selector_key = ["component", "matrix","stat"]
      measure_selector_key = list(set(measure_selector_key) & set(condition_key))
      
      if "component" in measure_selector_key:
         condition["component"] = self.value_index.convert_list("component", condition["component"])
      if "matrix" in measure_selector_key:
         condition["matrix"] = self.value_index.convert_list("matrix", condition["matrix"])
      if "id" in measure_selector_key:
         condition["id"] = self.value_index.convert_list("id", condition["id"])   

      time_selector_key =  ["st", "ed"]
      time_selector_key = list(set(time_selector_key) & set(condition_key))
      
      self.time_selector = {}
      for k in time_selector_key:
         self.time_selector[k] = condition[k]
      
      for site_id in tqdm(self.site_infor.keys(), desc="seraching..."):
         select =True
         if len(site_selector_key)>0:
            for key in site_selector_key:
               if self.site_infor[site_id][key] not in condition[key]:
                  select=False
                  break
               
         # if the site is not the target, examing files are not necessary.
         if not select:
            continue
         
         files = []
         for file_id in self.db_index[site_id].keys():
            select_file = True
            content = self.db_index[site_id][file_id]
            if "st" in site_selector_key:
               if content["ed"]<condition["st"]:
                  select_file = False
            if "ed" in site_selector_key:
               if content["st"]>condition["ed"]:
                  select_file = False
            for k in measure_selector_key:
               if content[k] not in condition[k]:
                  select_file = False
                  break
            if select_file:
               files.append(file_id)
         
         if len(files)>0:
            res[site_id] = files
      
      self.selected = res
      return res
   
   
   def list_db(self, db_list):
      for site_index in db_list.keys():
         for file in db_list[site_index]:
            print(site_index, self.db[site_index][file])
            
   def get_selected_db(self, use_number_index=True):
      print("Loading necessary data...")
      files = EbasFiles.get_load_files(data_path=self.data_path, 
                                       selected=list(self.selected.keys()), 
                                       loaded_db=list(self.db.keys()), 
                                       full_db=list(self.site_infor.keys()), 
                                       compression=self.compression,
                                       lazy_loading=False)
      
      _, db = EbasFiles.load_files(files=files, 
                                          lazy_loading=False)
      
      self.db.update(db)
      
      print("Gathering data to dataframe...")
      res_ts = []
      res_val = []
      
      for site in tqdm(self.selected.keys()):
         site_id = site
         for file in self.selected[site_id]:
            header = self.db_index[site_id][file]
            ts = self.db[site_id][file]["ts"]
            val = self.db[site_id][file]["val"]
            
            if len(self.time_selector)>0:
               if "st" in self.time_selector.keys():
                  st = self.time_selector["st"]
                  index = ts[:,0]>= st
                  ts = ts[index]
                  val = val[index]
               if "ed" in self.time_selector.keys():
                  ed = self.time_selector["ed"]
                  index = ts[:,1]<=ed
                  ts = ts[index]
                  val = val[index]           
            
            infor = np.empty((ts.shape[0], 4))
            infor[:,0] = self.value_index.site[site_id]
            infor[:,1] = header["component"]
            infor[:,2] = header["unit"]
            infor[:,3] = header["matrix"]
                        
            val = np.hstack((val,infor))
            
            res_ts.append(ts)
            res_val.append(val)
      
      # todo: handle None
      res_ts = np.vstack(tuple(res_ts))
      res_val = np.vstack(tuple(res_val))
      
      df1 = pd.DataFrame(res_ts, columns=["st", "ed"])
      df2 = pd.DataFrame(res_val, columns=["val", "site","component","unit","matrix"])
      df = pd.concat([df1,df2],axis=1)

      if not use_number_index:
         df["site"] =df["site"].map(self.value_index.site)
         df["component"] =df["component"].map(self.value_index.component)
         df["unit"] =df["unit"].map(self.value_index.unit)
         df["matrix"] =df["matrix"].map(self.value_index.matrix)
         
      return df
      