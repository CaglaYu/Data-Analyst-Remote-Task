from asyncio.windows_events import NULL
import pandas as pd
import chardet
import string
import numpy as np
from FileUtilities import DataSet
#pywebioComment_import pywebio

class TransformProcess:
    def __init__(self, target_df, writer):
        self.target_df = target_df
        self.writer = writer

    def pre_process(self, file_path):
        
        #=======================     ENCODING      ===============================================================================================================#
        # Data supplied to us as input is UTF-8 encoded. However it may not always be the case. 
        # I first try to read the data right away. If I encounter a UnicodeDecodeError, 
        # then I check for the encoding using chardet and use the encoding that I find with chardet to decode the file.
        
        try:
            supplier_df = pd.read_json(file_path,lines=True )    
        except UnicodeDecodeError:
            with open(file_path, 'rb') as cars_binary:
                detect = chardet.detect(cars_binary.read(5000)) 
                supplier_df = pd.read_json(file_path,lines=True,encoding=detect["encoding"])



        #=======================      FORM NEW COLUMNS OUT OF ATTRIBUTES      ====================================================================================#
        # I set the useful items from the Attribute Names cells as columns with the corresponding Attribute Values as their values
        # At the same time, I lose the columns (TypeName, TypeNameFull and entity_id) which I am not going to use 
        
        pivoted_df = supplier_df.pivot(index=["ID","MakeText", "ModelText", "ModelTypeText"], columns="Attribute Names", values="Attribute Values")

        #=======================      REMOVE UNUSED COLUMNS       ====================================================================================================#
        #determine the attributes that I am going to use:
        useful_attributes = ["BodyTypeText","BodyColorText","ConditionTypeText","City","MakeText",
        "FirstRegYear","Km","ModelText","ModelTypeText","FirstRegMonth","ConsumptionTotalText"]

        #drop the attributes that I am not going to use:
        pivoted_df = pivoted_df.reset_index() #Reindex so that I can drop ID column as well
        pivoted_df = pivoted_df.drop([ i for i in pivoted_df.columns if i not in useful_attributes ],axis=1)

        # Output to Excel:
        pivoted_df.to_excel(self.writer, sheet_name='PreProcessed',index=False)

        return pivoted_df




