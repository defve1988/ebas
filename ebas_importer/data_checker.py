import os
import shutil
import utilities
from datetime import datetime
from tqdm import tqdm

from utilities.utilities import download_ftp

class EbasFtpDataChecker:
   """
   this class is used for:
   1. gathering information from ftp server
   2. comparing with local files
   3. download files from ftp server
   4. archive files
   5. export which files were added, and which files are needed to be archived
   """
   
   def __init__(self, data_path):
      self.data_path = data_path
      
   def check_updates(self, download=False):
      # check whether raw data need to be updated
      print("-"*100)
      print("Check for updates...")
      print(f"{len(os.listdir(self.data_path))-1} raw data files in current data directory.")
      
      ftp_files = self.get_current_ftp_files()
      local_files = os.listdir(self.data_path)
      
      new = []
      for f in tqdm(ftp_files, desc="check ftp files..."):
         if f not in local_files:
            new.append(f)

      archive = []
      for f in tqdm(local_files, desc="check local files..."):
         if f not in ftp_files and f!="archived":
            archive.append(f)
            
      print(f"{len(new)} files need to be added.")
      print(f"{len(archive)} files need to be archived.")
      
      if download:
         self.download_files(new)
         self.archive_files(archive)
      
      print("-"*100)
      
      return new, archive
      
   
   def archive_files(self, files):
      # move files no longer exist on ftp server to /archived folder      
      for f in files:
         if os.path.isfile(os.path.join(self.data_path,f)):
            shutil.move(os.path.join(self.data_path,f),os.path.join(self.data_path,"archived",f))
            
      if len(files)>0:
         self.log("archived",files)
   
   
   def download_files(self, files):      
      # update file need to be changed.
      errors = utilities.download_ftp(files, out_path=self.data_path, ftp="https://thredds.nilu.no/thredds/fileServer/ebas/")      
      if len(files)>0:
         self.log("downloaded",files)
      if len(errors)>0:
         print(f"\t{len(errors)} errors occurred when downloading files, check log.txt.")
         self.log("errors",errors)
   
   def get_current_ftp_files(self):
      print("Reqesting data from ftp...")
      url = "https://thredds.nilu.no/thredds/catalog/ebas/catalog.html"
      selector = "tr > td > a > tt"
      files = utilities.bs4_get(url, selector)
      print(f"{len(files)-1} files on ftp server.")
      # the first line is "Ebas"
      return files[1:]
   
   def log(self, change, files):
      with open(os.path.join(self.data_path,"archived","log.txt"),"a") as f:
         f.write(datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + "\n")
         f.writelines(change + "\n")
         for file in files:        
            if file!="archived":
               f.write(file+ "\n")
         f.writelines("-"*50+ "\n")
         
         