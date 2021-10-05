import os
import pickle

bad_qc = [459,460,471,530,533,540,549,565,566,567,568,591,599,635,658,659,663,664,666,669,677,682,683,684,685,686,687,699,783,890,980,999]


class ValueIndex:
   def __init__(self):
      self.meta={"ebas":0, "no_ebas":1, 0:"ebas", 1:"no_ebas"}
      self.matrix={}
      self.unit={}
      self.res_code={}
      self.component={}
      self.site={}
      self.load_index()

   def load_index(self):
      if os.path.exists("value_index"):
         with open("value_index","rb") as pickle_file:
            data = pickle.load(pickle_file)
         
         self.matrix = data["matrix"]         
         self.unit = data["unit"]         
         self.res_code = data["res_code"]         
         self.component = data["component"]         
         self.site = data["site"]         
         
   def convert_list(self, attr_name, vals):
      res = []
      for v in vals:
         temp = getattr(self, attr_name)
         res.append(temp[v])
         
      return res
   
   def update_index(self, attr_name, vals):
      temp = {}
      for index, val in enumerate(vals):
         temp[index]= val
         temp[val]= index
      
      setattr(self,attr_name, temp)
      
      data = {
         "matrix":self.matrix,
         "unit":self.unit,
         "res_code":self.res_code,
         "component":self.component,
         "site":self.site,
      }
      with open("value_index","wb") as pickle_file:
         pickle.dump(data, pickle_file)