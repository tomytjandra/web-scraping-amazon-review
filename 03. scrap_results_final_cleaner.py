# import libraries
import pandas as pd
nan = float('nan')
from glob import glob
import os
from datetime import datetime

# read all csv inside the folder
FOLDER_NAME = "results"
df_list = [pd.read_csv(filename) for filename in glob(os.path.join(FOLDER_NAME, "*.csv"))]
df = pd.concat(df_list).reset_index(drop=True)

# PREPROCESS ProductBylineInfo and ProductBrandFromTable 
brand1 = df['ProductBrandFromTable'].str.upper()
brand2 = df['ProductBylineInfo'].str.replace('^(Visit the |Brand: )', '', regex=True).str.replace(' Store$', '', regex=True).str.upper()
df['ProductBrand'] = brand1.fillna(brand2)

# PREPROCESS ProductCategories
# split list of ProductCategories into multiple columns
df_cat = pd.DataFrame([pd.Series(pd.eval(x)) for x in df['ProductCategories']])
df_cat.columns = [f"ProductCategories_{x+1}" for x in df_cat.columns]

# combine both dataframe
df = pd.concat([df, df_cat], axis=1)

# remove unused columns
df.drop(columns=['ProductBrandFromTable', 'ProductBylineInfo', 'ProductCategories'], inplace=True)

print(df.shape[0])

# save to csv
SUBFOLDER_NAME = "combined"
SUBFOLDER_PATH = os.path.join(FOLDER_NAME, SUBFOLDER_NAME)
if not os.path.exists(SUBFOLDER_PATH):
    os.makedirs(SUBFOLDER_PATH)

now = datetime.now()
datetime_string = now.strftime("%Y%m%d_%H%M%S")
df.to_csv(os.path.join(SUBFOLDER_PATH, f"{datetime_string}_scrap_results_combined.csv"), index=False)
