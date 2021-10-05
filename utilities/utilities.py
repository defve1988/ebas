import concurrent.futures
import multiprocessing
from tqdm import tqdm
import pickle
import lzma
import csv
import pycountry
import requests
from bs4 import BeautifulSoup
import wget
import math
import pandas as pd
import sys

def run_mp(map_func, arg_list, combine_func=None):
   num_cores = multiprocessing.cpu_count()
   
   with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as pool:
      with tqdm(total=len(arg_list)) as progress:
         futures = []
         
         for args in arg_list:
               future = pool.submit(map_func, args)
               future.add_done_callback(lambda p: progress.update())
               futures.append(future)

         results = []
         
         for future in futures:
               result = future.result()
               results.append(result)
   
   if combine_func is not None:
      return combine_func(results)
   else:
      return results   

def load_xz_file(file):
   with lzma.open(file["path"], "rb") as pickle_file:
      return {"name": file["name"],
              "data": pickle.load(pickle_file)}

def load_xz_file_index(file):
   with lzma.open(file["path"], "rb") as pickle_file:
      return {"name": file["name"],
              "data": pickle.load(pickle_file)["content_index"]}

def download_ftp(files, out_path, ftp="https://thredds.nilu.no/thredds/fileServer/ebas/"):
   errors =[]
   for (index, f) in enumerate(files):
      try:
         wget.download(ftp+"/"+f, out=out_path)
         print(f" ------{index+1}/{len(files)}")
      except Exception as e:
         print(e)
         errors.append([f])
   
   list2csv(errors, "errors.csv", header=["error"], single_col=True)
   
   return errors


def progress_bar(curr, total, curr_progress, disp_progress=5):
   # curr_progress = 0
   # curr_progress= utilities.progress_bar(index, len(local_files)-1, curr_progress)         
   
   sys.stdout.write('\r')
   progress_val = math.floor(curr/total*100)
   if curr/total*100>curr_progress:
      sys.stdout.write("[%-100s] %d%%" % ('.'*(curr_progress+disp_progress), progress_val))
      sys.stdout.flush()      
      return curr_progress+disp_progress
   elif progress_val>=100:
      sys.stdout.write("[%-100s] %d%%" % ('.'*(curr_progress+disp_progress), progress_val))
      sys.stdout.flush()
      print()
      return curr_progress+disp_progress
   else:
      return curr_progress
      
   
def list2dict(list_dict):
   # convert [{"id1":{}}, {"id2":{}}, ....] => {"id1":{}, "id2":{},....}
   res={}
   count={}
   for d in list_dict:
      # print(res,d)
      if d is not None:
         id = list(d.keys())[0]
         if id in res.keys():
            count[id]+=1
         else:
            count[id]=1
         res.update(d)
   return res, count


def list2set(list_list):
   # convert [ [], [], [], ...] => { [] + []+ [], ...}
   res=[]
   for l in list_list:
      res.extend(l)
   return list(set(res))

def combine_vars(vars):
   # convert [{file:[var1, var2,...]}]=> { site:{file:{"vars":[var1, var2, ..]}, site2:{}, ...}}
   res = {}
   for v in vars:
      file_name = list(v.keys())[0]
      site_id = file_name.split(".")[0]
      if site_id not in res.keys():
         res[site_id]={"files":{}}
         res[site_id]["files"][file_name]=v[file_name]
      else:
         res[site_id]["files"][file_name]=v[file_name]
         
   return res

def combine_infor(list_dict_infor):
   res = {}
   for d in list_dict_infor:
      id = list(d.keys())[0]
      if id not in res.keys():
         res[id] = d[id]
         res[id]["file_num"] =1
      else:
         res[id]["file_num"] +=1
         res[id]["files"].update(d[id]["files"])
   return res

def list2csv(data, file_name, header=None, single_col=True):
   with open(file_name, 'w', newline='', encoding="utf-8") as f:
    write = csv.writer(f)
    if header is not None:
      write.writerow(header)
    if single_col:
      for item in data:
          write.writerow([item])
    else:
       write.writerows(data)
   
   print(f"Data is written to {file_name}.")
   
       
       
# convert country name and code 
def country2code(country):
   return pycountry.countries.search_fuzzy(country)[0].alpha_2
def code2country(country_code):
   return pycountry.countries.get(alpha_2=country_code).name


# convert from units



# simple web scrap for sth
def bs4_get(url, selector, tags=None):
   # tags = none, will return a list of target text
   # tags= [], will return a list of dict [{tag1:content, tag2: content}, ...]
   wb_res = requests.get(url)
   if wb_res.status_code !=200:
      raise ValueError("Connection error.")

   soup = BeautifulSoup(wb_res.text, 'lxml')
   
   selected = soup.select(selector)
   res = []
   for i in selected:
      if tags is None:
         res.append(i.text)
      else:
         temp={}
         for t in tags:
            temp[t] = i.get(t)
         
         res.append(temp)
      
   return res