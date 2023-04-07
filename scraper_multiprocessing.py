# references
# https://medium.com/analytics-vidhya/asynchronous-web-scraping-101-fetching-multiple-urls-using-arsenic-ec2c2404ecb4
# https://www.zenrows.com/blog/speed-up-web-scraping-with-concurrency-in-python#multiprocessing

# data wrangling
import pandas as pd
import numpy as np

# scraping
import asyncio  # concurrent processing
import concurrent
from concurrent.futures import ProcessPoolExecutor  # multiprocessor processing
from multiprocessing import cpu_count  # multiprocessor processing
from arsenic import get_session, browsers, services  # async selenium
from datetime import datetime
import os

# logging
import logging
import structlog

# measure computation time
from timeit import default_timer as timer


def set_arsenic_log_level(level=logging.WARNING):
    logger = logging.getLogger("arsenic")

    def logger_factory():
        return logger

    structlog.configure(logger_factory=logger_factory)
    logger.setLevel(level)


# extract detail for each page
async def extract_details(page):

    # configure the web driver
    service = services.Chromedriver()
    browser = browsers.Chrome()
    browser.capabilities = {
        "goog:chromeOptions": {
            "args": [
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-infobars",
                "--disable-notifications",
                "--disable-logging",
                "--mute-audio",
            ]
        }
    }

    async with get_session(service, browser) as session:
        data = {
            "ProductURL": page,
        }

        # navigate to the web page
        await session.get(page)
        # print(page)

        # get the title of the current page
        document_title = await session.execute_script("return document.title;")

        # skip scraping process if page not found, ex: https://www.amazon.com/dp/B004N5KULM
        if document_title == "Page Not Found":
            data["ProductTitle"] = document_title
            return data

        # extract the text content of the page
        ## TITLE
        try:
            title_el = await session.get_element("#productTitle")
            title_text = await title_el.get_text()
        except:
            title_text = ""

        ## BYLINE INFO
        try:
            byline_el = await session.get_element("#bylineInfo")
            byline_text = await byline_el.get_text()
        except:
            byline_text = ""

        ## BRAND
        """
        try:
            table_el = await session.get_element("table")
            table_text = await table_el.get_text()
            brand_text = table_text.split('Brand ')[1].split('\n')[0]
        except:
            brand_text = ""
        """

        ## DESCRIPTION
        """
        try:
            description_el = await session.get_element("#productDescription")
            description_text = await description_el.get_text()
        except:
            # not all product have description
            description_text = ""
        """

        ## FIRST IMAGE
        """
        try:
            img_el = await session.get_element("#imgTagWrapperId")
            img_el_img = await img_el.get_element("img")
            img_src = await img_el_img.get_attribute("src")
        except:
            img_src = ""
        """

        ## CATEGORIES
        try:
            cat_el = await session.get_element(".a-subheader")
            cat_text = await cat_el.get_text()
            cat_text_list = cat_text.split("\n")[::2]
        except:
            # not all product have categories, ex: https://www.amazon.com/dp/B001EO5TPM
            cat_text_list = ""

        data["ProductTitle"] = title_text
        data["ProductBylineInfo"] = byline_text
        # data["ProductBrandFromTable"] = brand_text
        # data["ProductDesc"] = description_text
        # data["ProductImg"] = img_src
        data["ProductCategories"] = cat_text_list

        return data


# function to loop the pages and gather the results
async def extract_details_task(pages_for_task):
    tasks = [extract_details(page) for page in pages_for_task]
    list_of_lists = await asyncio.gather(*tasks)
    return list_of_lists


# wrapper to run the scraping for each core
def asyncio_wrapper(pages_for_task):
    return asyncio.run(extract_details_task(pages_for_task))


def main(idx_range, num_cores=1):
    # suppress log from arsenic
    set_arsenic_log_level()

    # list of pages to be scraped
    pages = pd.read_csv("Reviews_withURL.csv", usecols=["ProductURL"])["ProductURL"].unique()
    pages = pages[idx_range[0] : idx_range[1]]

    start = timer()

    # split the scraping task into several cores, based on num_cores
    executor = ProcessPoolExecutor(max_workers=num_cores)
    tasks = [
        executor.submit(asyncio_wrapper, pages_for_task)
        for pages_for_task in np.array_split(pages, num_cores)
    ]

    # combine the scraping results from different cores
    doneTasks, _ = concurrent.futures.wait(tasks)
    results = [item.result() for item in doneTasks]
    flattened_results = sum(results, [])

    # convert result list to dataframe
    df = pd.DataFrame(flattened_results)

    # multiprocessing mess up the row order, so the following code will reorder the urls
    df = df.set_index("ProductURL").reindex(pages).reset_index()

    # save to csv
    FOLDER_NAME = "results"
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{FOLDER_NAME}/{datetime_string}_from_{idx_range[0]}_to_{idx_range[1]}_scrap_results.csv"
    df.to_csv(filename, index=False)

    end = timer()
    print(end - start)


if __name__ == "__main__":
    # please specify the index range here
    idx_range = (0, 500)
    num_cores = 3
    # num_cores = cpu_count() - 1
    main(idx_range, num_cores)
