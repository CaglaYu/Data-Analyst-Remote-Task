import pandas as pd
#pywebioComment_import pywebio

class DataSet:
    def __init__(self, data_file):
        self.path = data_file
  
    def data_frame(self):        
        return pd.read_excel(self.path)  
   

