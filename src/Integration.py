from asyncio.windows_events import NULL
import pandas as pd
import chardet
import string
import numpy as np
from FileUtilities import DataSet
#pywebioComment_import pywebio

class TransformIntegrate:
    def __init__(self, target_df, writer):
        self.target_df = target_df
        self.writer = writer

    def integrate(self, normalized_df):
        #========================     ADD MISSING COLUMNS            ========================================================================================#
        #There is no attribute in the source dataset to populate the currency column of the target dataset. However, there aren't 
        # any nulls in the target database either and as the source contains only Swiss data, I populate the currency field with "CHF":
        normalized_df["currency"] = "CHF"

        # There is no attribute in the source dataset to populate the country column of the target dataset. However, there aren't 
        # any nulls in the target database either and as the source contains only Swiss data, I populate the currency field with "CH":
        normalized_df["country"] = "CH"

        # The unit in the supplier data is km.
        normalized_df["mileage_unit"] = "kilometer"

        # There is no attribute in the source dataset to populate the POR column of the target dataset. However, there aren't any nulls in the 
        # target database either and also 75% of the current data is false. Assuming it is the default value, I populate the currency field with false:
        normalized_df["price_on_request"] = False  #kept as boolean in the target


        # mileage is kept as float with one digit after the decimal point. I convert the source accordingly:
        normalized_df["Km"] = normalized_df["Km"].astype('float64')
        normalized_df["Km"] = normalized_df["Km"].round(1)

        normalized_df.loc[normalized_df["ConsumptionTotalText"]!="null","ConsumptionTotalText"]="l_km_consumption"
        normalized_df.loc[normalized_df["ConsumptionTotalText"]=="null","ConsumptionTotalText"]=np.nan

        # There is no attribute in the source dataset to populate zip field. So I do not add a column for that. Also, in the target database,
        # the zip's of the records from CH are all null. 
        
        # There is also no attribute in the source dataset to populate drive field. So I do not add a column for that. 

        #========================     INTEGRATE             =============================================================================================#
        # rename the source dataset columns so that they match the column names of the target dataset

        column_mappings= DataSet("../ColumnMappings.xlsx").data_frame()
        columns_dict = dict(zip(column_mappings["source"],column_mappings["target"]))
        normalized_df = normalized_df.rename(columns=columns_dict)
        normalized_df = pd.concat([self.target_df, normalized_df], ignore_index=True)

        # Output to Excel:
        normalized_df.to_excel(self.writer, sheet_name='Integrated',index=False)
        #pywebioComment_pywebio.output.put_html(normalized_df.to_html(border=0))

        


