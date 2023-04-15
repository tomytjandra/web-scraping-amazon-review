import pandas as pd

# GENERATE FULL PRODUCT URL
def generate_full_urls():
    pages = pd.read_csv("Reviews_withURL.csv", usecols=["ProductURL"])["ProductURL"].unique()
    pages = pd.Series(pages, name="ProductURL")
    print(pages.shape[0])
    pages.to_csv("ProductURL.csv", index=False)

# GENERATE ONLY URL WITH MISSING VALUES (MAYBE SKIPPED DUE TO CAPTCHA)
def generate_missing_urls(full_data_path, target_path):
    scrap_results = pd.read_csv(full_data_path)
    pages_missing = scrap_results[scrap_results["ProductTitle"].isna()]["ProductURL"]
    pages_missing.to_csv(target_path, index=False)
    print(f"MISSING VALUES: {pages_missing.shape[0]}")

if __name__ == "__main__":
    full_data_path = "results/from_0_to_10000_scrap_results_Tomy.csv"
    target_path = "ProductURL_missing.csv"
    generate_missing_urls(full_data_path, target_path)
