from dataclasses import dataclass
import os
import pandas as pd
#pywebioComment_import pywebio

from Processes import TransformProcess
from Normalization import TransformNormalize
from Integration import TransformIntegrate
from FileUtilities import DataSet


#===============   USER BROWSES AND PICKS THE SOURCE FILE  ===================================================================================================#
#pywebioComment_source_file=pywebio.input.file_upload("Please browse and select the supplier data:")['filename']
source_file="../supplier_car.json"  #this line should be commented out, if the line above is not

#create the dataframe for the target:
#pywebioComment_target_file=pywebio.input.file_upload("Please browse and select the target data:")['filename']
target_file = "../Target Data.xlsx" #this line should be commented out, if the line above is not
target_df = DataSet(target_file).data_frame()


#prepare an Excel writer as we are going to have multiple sheets:
excel_writer = pd.ExcelWriter('../Output_car.xlsx') 

data_transform = TransformProcess(target_df, excel_writer)
data_normalize = TransformNormalize(target_df, excel_writer)
data_integrate = TransformIntegrate(target_df, excel_writer)

#call the steps:
processed_data = data_transform.pre_process(source_file) 
normalized_data = data_normalize.normalize(processed_data)
data_integrate.integrate(normalized_data)

excel_writer.save()
excelfile = "../Output_car.xlsx"
os.startfile(os.path.normpath(excelfile))