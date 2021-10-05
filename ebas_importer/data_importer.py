import json
import xarray as xr
import sys
import utilities
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import pickle
import lzma

from .value_index import *

class EbasFtpDataImporter:
   """
   this class is designed for handle ".nc" file:
   1. for one single file: get indexing information, including site, components_content, and file attrs.
   2. for one single file: get component vals and qc
   3. combine results from multiple file.
   """
   
   def __init__(self, data_path, compression):
      self.data_path = data_path
      self.site = {}
      # self.components = list(pd.read_csv("components.csv")["components"])
      self.detailed = True
      self.compression = compression
      self.value_index = ValueIndex()
      self.use_value_index = False
      
     
   def get_indexing(self, file_name):
      try:
         # get site information
         ebas = xr.open_dataset(os.path.join(self.data_path, file_name))
         ebas_metadata = ebas.ebas_metadata
         ebas_metadata = json.loads(ebas_metadata)
            
         site = {
                "id": ebas_metadata["Station code"],
                "name": ebas_metadata["Station name"],
                "country": utilities.code2country(ebas_metadata["Station code"][0:2]),
                "land_use":None,
                "station_setting":None,
                "alt":None,
                "lat":None,
                "lon":None,
                "files":{},
               #  "var_content":[]
            }
         try:
            site["land_use"] = ebas_metadata["Station land use"]           
         except:
            pass
         try:
            site["station_setting"] = ebas_metadata["Station setting"]         
         except:
            pass
         try:
            site["alt"] = ebas_metadata["Station altitude"]                           
         except:
            pass
         try:           
            site["lat"] = ebas_metadata["Station latitude"]                           
         except:
            pass
         try:            
            site["lon"] = ebas_metadata["Station longitude"]               
         except:
            pass
         
         # get var content
         vars = list(ebas.data_vars.keys())
         vars = list(filter(lambda x: not x.endswith("_qc") and not x.endswith("_ebasmetadata"), vars))
         vars.remove("time_bnds")
         vars.remove("metadata_time_bnds")
         
         var_content = []
         for v in vars:
            temp = ebas[v+"_ebasmetadata"].data.tolist()[-1]
            
            while isinstance(temp, list):
               temp = temp[-1]
               
            temp = json.loads(temp)
            
            if "Matrix" in temp.keys():
               content ={
                  "res_code": ebas_metadata["Resolution code"],
                  "matrix": temp["Matrix"],
                  "unit":temp["Unit"],
                  "meta": "no_ebas", 
                  
                  "var":v,
                  "site": ebas_metadata["Station code"],
                  "stat": temp["Statistics"],
                  "component":temp["Component"],
                  "st":ebas["time_bnds"][0,0].values,
                  "ed":ebas["time_bnds"][-1,1].values,
               }
            elif "ebas_matrix" in temp.keys():
               content ={
                  # "res": res_code_index[ebas_metadata["Resolution code"]],
                  # "matrix": matrix_index[temp["ebas_matrix"]],
                  # "unit":units_index[temp["ebas_unit"]],
                  # "meta": meta_index["no_ebas"],
                  
                  "res_code": ebas_metadata["Resolution code"],
                  "matrix": temp["ebas_matrix"],
                  "unit":temp["ebas_unit"],
                  "meta": "no_ebas",
                  
                  "var":v,
                  "site": ebas_metadata["Station code"],
                  "stat": temp["ebas_statistics"],
                  "component":temp["ebas_component"],
                  "st":ebas["time_bnds"][0,0].values,
                  "ed":ebas["time_bnds"][-1,1].values,
               }
            
            # convert string to indexing number
            if self.use_value_index:
               content["site"] = self.value_index.site[content["site"]]
               content["matrix"] = self.value_index.matrix[content["matrix"]]
               content["component"] = self.value_index.component[content["component"]]
               content["unit"] = self.value_index.unit[content["unit"]]
               content["res_code"] = self.value_index.res_code[content["res_code"]]
               content["meta"] = self.value_index.meta[content["meta"]]
               
            var_content.append(content)
            
         # get attr information
         attr_content={}
         if self.detailed:
            attrs = ebas.attrs
            attr_content ={}
            for a in attrs:
               temp = getattr(ebas,a)
               if isinstance(temp, np.ndarray):
                  temp= temp.tolist()
               attr_content[a] = temp
         
         site["files"] = {file_name:{"contents": var_content, "attrs": attr_content}}
                       
         return {site["id"]: site}
      
      except Exception as e:
         print(e)
         print(file_name)
         return {file_name: {
                "id": "",
                "name": "",
                "land_use": "",
                "station_setting": "",
                "lat": "",
                "lon": "",
                "alt": "",
                "error":str(e),
            }}

   @staticmethod
   def combine_infor(list_dict_infor):
      res = {}
      for d in list_dict_infor:
         id = list(d.keys())[0]
         if id not in res.keys():
            res[id] = d[id]
            # will add stats for how many files this site has
            res[id]["file_num"] =1
            res[id]["components"]={}
         else:
            res[id]["file_num"] +=1
            res[id]["files"].update(d[id]["files"])
         
         # create a component indexing dict
         for f in d[id]["files"].keys():
            contents = d[id]["files"][f]["contents"]
            for c in contents:
               if c["component"] not in res[id]["components"].keys():
                  res[id]["components"][c["component"]] = {
                  "st":c["st"],
                  "ed":c["ed"],
               }
               else:
                  if c["st"] < res[id]["components"][c["component"]]["st"]:
                     res[id]["components"][c["component"]]["st"] = c["st"]
                  if c["ed"] > res[id]["components"][c["component"]]["ed"]:
                     res[id]["components"][c["component"]]["ed"] = c["ed"]
                      
      return res


   def get_site_data(self, files):
      """
      {
         content_index: {
          id1:{
             st, ed, comp, var, matrix, res_code, units
          } ,
          id2:{
             }
         ,...
         }
         id1:(df: st, ed, val_qc)
         id2:(df: st, ed, val_qc)
      }
      """
      compression = self.compression
      res = { "content_index" :{} }
      id_count = 0
      for file in files.keys():
         try:
            site_id = file.split(".")[0]
            ebas = xr.open_dataset(os.path.join(self.data_path, file))
            for content in files[file]["contents"]:
               res["content_index"][id_count] = {
                     "st": content["st"],
                     "ed": content["ed"],
                     "component":content["component"],
                     "matrix": content["matrix"],
                     "res_code": content["res_code"],
                     "unit": content["unit"],
                     "var": content["var"],
                     "stat": content["stat"],
                     "file":file
               }
               # temp = pd.DataFrame()
               st = ebas["time_bnds"].data[:,0]
               ed = ebas["time_bnds"].data[:,1]
               ts = np.array([st,ed]).T
               
               # get var value and qc, the values can be updated for several times, so additional dimensions may be applied
               val = ebas[content["var"]].data
               while len(val.shape)>1:
                  val = val[-1,:]  
               qc = ebas[content["var"]+"_qc"].data
               while len(qc.shape)>1:
                  qc = qc[-1,:]
               
               # filter value with qc values
               val[np.isin(qc, bad_qc)]=None
               
               val = np.array([val]).T
               
               res[id_count] = {"ts": ts, "val": val}
               id_count+=1
            
         except Exception as e:
            print(e, file)  
      
      if compression=="xz":   
         with lzma.open(os.path.join("ebas_proj_dump_xz", f"{site_id}.xz"), "wb") as pickle_file:
            pickle.dump(res, pickle_file)
      elif compression is None:
         with open(os.path.join("ebas_proj_dump", f"{site_id}"), "wb") as pickle_file:
            pickle.dump(res, pickle_file)
      