# references
# https://medium.com/analytics-vidhya/asynchronous-web-scraping-101-fetching-multiple-urls-using-arsenic-ec2c2404ecb4
# https://www.zenrows.com/blog/user-agent-web-scraping#what-are-the-best-user-agents-for-scraping


# data wrangling
import pandas as pd

# scraping
import asyncio  # concurrent processing
from arsenic import get_session, browsers, services  # async selenium
from datetime import datetime
import os
import random

# logging
import logging
import structlog

# measure computation time
from tqdm import tqdm


def set_arsenic_log_level(level=logging.WARNING):
    logger = logging.getLogger("arsenic")

    def logger_factory():
        return logger

    structlog.configure(logger_factory=logger_factory)
    logger.setLevel(level)


user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]


async def extract_details(urls, headless):
    # iterate through multiple pages
    results = []
    for url in tqdm(urls):
        # random user_agent to avoid captcha
        user_agent = random.choice(user_agent_list)

        # configure the web driver
        service = services.Chromedriver()
        browser = browsers.Chrome()
        browser.capabilities = {
            "goog:chromeOptions": {
                "args": [
                    #"--headless",
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--disable-infobars",
                    "--disable-notifications",
                    "--disable-logging",
                    "--mute-audio",
                    f"user-agent={user_agent}",
                ]
            }
        }

        if headless:
            browser.capabilities['goog:chromeOptions']['args'].append("--headless")

        async with get_session(service, browser) as session:
            # random delay to avoid captcha
            delay = random.randint(0, 3)
            asyncio.sleep(delay)

            # navigate to the web page
            await session.get(url)
            print(url)

            # get the title of the current page
            document_title = await session.execute_script("return document.title;")

            # skip scraping process if page not found, ex: https://www.amazon.com/dp/B004N5KULM
            if document_title == "Page Not Found":
                results.append(
                    {
                        "ProductURL": url,
                        "ProductTitle": document_title,
                    }
                )
                continue

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
            try:
                table_el = await session.get_element("table")
                table_text = await table_el.get_text()
                brand_text = table_text.split('Brand ')[1].split('\n')[0]
            except:
                brand_text = ""

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

            results.append(
                {
                    "ProductURL": url,
                    "ProductTitle": title_text,
                    "ProductBylineInfo": byline_text,
                    "ProductBrandFromTable": brand_text,
                    # "ProductDesc": description_text,
                    # "ProductImg": img_src,
                    "ProductCategories": cat_text_list,
                }
            )

    return results


def main(idx_range, headless, filename):
    # suppress log from arsenic
    set_arsenic_log_level()

    # list of pages to be scraped
    pages = pd.read_csv(filename)["ProductURL"].to_list()
    pages = pages[idx_range[0] : idx_range[1]]

    results = asyncio.run(extract_details(pages, headless))

    # convert result list to dataframe
    df = pd.DataFrame(results)
 
    # save to csv
    FOLDER_NAME = "results"
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{datetime_string}_from_{idx_range[0]}_to_{idx_range[1]}_scrap_results.csv"
    df.to_csv(os.path.join(FOLDER_NAME, filename), index=False)


if __name__ == "__main__":
    # please specify the index range here
    main(idx_range=(0, 1000), headless=False, filename="ProductURL.csv")
