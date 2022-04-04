from asyncio.windows_events import NULL
import pandas as pd
import chardet
import string
import numpy as np
from FileUtilities import DataSet
#pywebioComment_import pywebio

class TransformNormalize:
    def __init__(self, target_df, writer):
        self.target_df = target_df
        self.writer = writer

    
    def normalize(self, processed_df):
        #=======================      NORMALIZE BODY TYPE AND VEHICLE TYPE=====================================================================================#
        # there are certain records in the supplier data which are not passenger cars. Before I go ahead and normalize the car type, 
        # I should update the vehicle type of those. For that end, I add a column for the vehicle type:
        processed_df["type"] = "car"

        # then I update the type to "Other" for the aforementioned records:
        processed_df.loc[processed_df['BodyTypeText'].isin( ["Pick-up", "Sattelschlepper","Wohnkabine"] ),'type'] = 'other'
        
        # Then I normalize the body type according to parameterized mappings:

        body_mappings_df= DataSet("../BodyMappings.xlsx").data_frame()
        body_dict = dict( zip( body_mappings_df["source"], body_mappings_df["target"] ) ) 

        processed_df["BodyTypeText"] =   processed_df["BodyTypeText"].map(body_dict).fillna(processed_df["BodyTypeText"])

        #After these operations, there happens to be one record with NaN in the BodyTypeText field. It actually is a motorcycle, thus the absence of body type.
        # I update the vehicle type for those kind of records:
        processed_df.loc[processed_df['BodyTypeText'].isnull() ,'type'] = 'Other'


        #=======================      NORMALIZE MAKE OF CAR         ===========================================================================================#
        # Makes in the target data are in Title Case. I could update the makes in the source data to Title Case but, 
        # there are exceptions like BMW, McLaren, OSCA, MINI Classic etc. That is why I first select distinct makes from the target data
        # and if the make already exists in the target, I simply use the value in the target. Otherwise I use Title Case: 
        target_makes = self.target_df.copy()['make'].dropna().unique().tolist()
        makes_dict = dict(zip(target_makes, map( lambda x: x.lower()  , target_makes) ))
        
        # Now I have a dictionary which has the original make data as the key and its lowercase version as the value
        # I am going to use the lowercase values to compare to the source makes:
        processed_df['MakeText'] = processed_df['MakeText'].str.lower()

        # Now let's exchange keys and values so that I can directly map the column:
        makes_dict = dict((v,k) for k,v in makes_dict.items())

        # There are some values which refer to the same make but with different names at the target and the source.
        # For example target holds "DMC" for the make which is kept as "DeLorean" at the source.
        # I created a mappings file in Excel for those. Now I can append the values in this mappings file to our dictionary above (makes_dict)
        # and start mapping the values accordingly.   
        # !!! -> Everytime a new inconsistency occurs, that is printed out to the user so that they shall make the neccessary addition to the mappings file:
        make_mappings_df= DataSet("../MakeMappings.xlsx").data_frame() 
        makes_dict = dict( zip( make_mappings_df["source"].str.lower(), make_mappings_df["target"] ) ) | makes_dict
        new_makes = processed_df[~processed_df["MakeText"].isin(makes_dict.keys())]["MakeText"].unique()

        
        out_str = "We do not have any record with the following makes in the target database:\n"
        out_str+=(len(new_makes) * "{}  ").format(*new_makes) 
        out_str+="""\nEither they are new, or the name used in the source data for the same make is different than the name 
        used in the target data. If they are new, no action is necessary: The script automatically integrates them to the 
        target. However, if they already exist in the target dataset with a different name, please enter a mapping for that 
        record, into the MakeMappings.xlsx file: """
        #pywebioComment_pywebio.output.put_html(out_str)
        print(out_str)

        processed_df["MakeText"] = processed_df["MakeText"].map(makes_dict).fillna(processed_df["MakeText"].apply(lambda x: string.capwords(x)))


        #=======================      NORMALIZE BODY COLOR         =======================================================================================#
        color_mappings_df= DataSet("../ColorMappings.xlsx").data_frame()
        colors_dict = dict( zip( color_mappings_df["source"].str.lower(), color_mappings_df["target"] ) ) 
        processed_df["BodyColorText"] = processed_df["BodyColorText"].map(colors_dict).fillna(processed_df["BodyColorText"].apply(lambda x: string.capwords(x)))


        #========================     NORMALIZE CONDITION         ========================================================================================#
        # Condition: actually the target dataset holds data about the condition of the car. Whether it has gone under a change of parts or not, 
        # whether they have been refurbished or not. The condition fielad in the source dataset however, holds data about the age of car. 
        # The mappings should be clarified with the customer. An oldtimer car could have been restored or kept original. For now, I make the mappings as follows:
        condition_dict = {"Occasion":"Used", "Oldtimer":"Used", "Neu":"New", "Vorführmodell":"New"}
        processed_df["ConditionTypeText"] = processed_df["ConditionTypeText"].map(condition_dict).fillna(processed_df["ConditionTypeText"])

        #========================     NORMALIZE FIRST REGISTRATION    =====================================================================================#
        # These two are kept as numeric data in the target
        processed_df["FirstRegYear"] = processed_df["FirstRegYear"].astype('int64')
        processed_df["FirstRegMonth"] = processed_df["FirstRegMonth"].astype('int64')

        # Output to Excel:
        processed_df.to_excel(self.writer, sheet_name='Normalized',index=False)
        
        return processed_df


        

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

        


