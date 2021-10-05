import utilities
import pickle
import lzma
import json
import os

class EbasFiles:
   def __init__(self):
      pass

   @staticmethod
   def get_load_files(data_path, selected, loaded_db, full_db, compression='xz', lazy_loading=False):
      """this method generate files need to be loaded for db
      
      Args:
          data_path (str): data path to files
          selected (list): list of selected sites
          loaded_db (list): list of loaded sites
          full_db (list): list of all sites
          compression (str, optional):  Defaults to 'xz'.

      Returns:
          (list): list of  {"name":"", "path":""}
      """
      
      files = []
      suffix = '.xz' if compression=="xz" else ''
      
      if selected == "all":
         for site in full_db:
            temp = {"path": os.path.join(data_path, site+suffix),
                     "name": site}
            if lazy_loading:
               temp["lazy_loading"] = True
            files.append(temp)
      else:
         for site in selected:
            if site not in loaded_db:
               temp = {"path": os.path.join(data_path, site+suffix),
                     "name": site}
               if lazy_loading:
                  temp["lazy_loading"] = True
               files.append(temp)
            
      return files
   
   @staticmethod
   def load_files(files, lazy_loading=True):
      """this method opens ebas data files
      
      Args:
          files (dict): {"path":"", "name":""}
          lazy_loading (bool, optional): [whether load whole dataset]. Defaults to True.
      
      Returns:
          (tuple): (db_index, db)    
      """
      db_index = {}
      db = {}
      res = utilities.run_mp(EbasFiles.load_file, files)
            
      # combine all the data
      for r in res:
         if not lazy_loading:
            db_index[r["name"]] = r["data"]["content_index"]
            r["data"].pop("content_index")
            db[r["name"]] = r["data"]
         else:
            db_index[r["name"]] = r["data"]
      
      return db_index, db
   
   @staticmethod
   def load_file(file):
      """this method opens one '.xz', '.json', and python pickle files

      Args:
          file (dict): {"name":"", "path":"", lazy_loading:""}

      Returns:
          dict: {"name":"", "data":""}
      """
      if "lazy_loading" in file.keys():
         lazy_loading = file["lazy_loading"]
      else:
         lazy_loading = False
      
      file_path = file["path"]
      
      if file_path.endswith("xz"):
         with lzma.open(file_path, "rb") as pickle_file:
            res = pickle.load(pickle_file)
      elif file_path.endswith("json"):
         with open(file_path,"r") as json_file:
            res = json.load(json_file)
      else:
         with open(file_path, "rb") as pickle_file:
            res = pickle.load(pickle_file)
      
      if lazy_loading:
         return {"name":file["name"], "data":res["content_index"]}
      else:
         return {"name":file["name"], "data":res}
