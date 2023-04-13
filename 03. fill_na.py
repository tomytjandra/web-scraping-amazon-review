import pandas as pd
from glob import glob


def fill_missing_urls(full_data_path, missing_data_path):
    # read both dataframe
    full_df = pd.read_csv(full_data_path).set_index("ProductURL")
    missing_df = pd.read_csv(missing_data_path).set_index("ProductURL")

    # perform a left join
    merged_df = pd.merge(full_df, missing_df, how='left', left_index=True, right_index=True, suffixes=('', '_temp'))

    # fill in missing values of left df with right df
    for col in full_df.columns:
        merged_df[col].fillna(merged_df[f"{col}_temp"], inplace=True)

    # delete temporary columns
    merged_df = merged_df.drop(columns=merged_df.filter(regex='_temp').columns)

    # reset index
    merged_df = merged_df.reset_index()

    # save csv, replace with the left df
    merged_df.to_csv(full_data_path, index=False)
    print(f"REMAINING MISSING VALUES: {merged_df[merged_df['ProductTitle'].isna()].shape[0]}")

if __name__ == "__main__":
    # this will be the left df
    full_data_path = "results/from_0_to_10000_scrap_results_Tomy.csv"

    # this will be the right df
    missing_data_path = "results/20230413_114556_missing_from_0_to_1000_scrap_results.csv"

    fill_missing_urls(full_data_path, missing_data_path)
