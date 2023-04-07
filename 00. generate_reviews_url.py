import pandas as pd

# GENERATE FULL PRODUCT URL
pages = pd.read_csv("Reviews_withURL.csv", usecols=["ProductURL"])["ProductURL"].unique()
pages = pd.Series(pages, name="ProductURL")
# print(pages.shape[0])
# pages.to_csv("ProductURL.csv", index=False)

# GENERATE ONLY URL WITH MISSING VALUES (MAYBE SKIPPED DUE TO CAPTCHA)
full_data_path = "results/20230407_183209_from_0_to_1000_scrap_results.csv"

scrap_results = pd.read_csv(full_data_path)
pages_missing = scrap_results[scrap_results["ProductTitle"].isna()]["ProductURL"]
pages_missing.to_csv("ProductURL_missing.csv", index=False)
print(pages_missing.shape[0])