# references
# https://medium.com/analytics-vidhya/asynchronous-web-scraping-101-fetching-multiple-urls-using-arsenic-ec2c2404ecb4
# https://www.zenrows.com/blog/user-agent-web-scraping#what-are-the-best-user-agents-for-scraping
# https://github.com/a-maliarov/amazoncaptcha

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

# captcha solver
from amazoncaptcha import AmazonCaptcha

# delete temp files
import glob
import shutil


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

# from chatGPT
def get_user_agent():
    chrome_version = random.randint(100, 109)
    mac_version = f"Macintosh; Intel Mac OS X 10_{random.randint(9, 15)}_{random.randint(1, 9)}"
    win_version = f"Windows NT {random.choice(['6.1', '10.0'])}; Win64; x64"
    linux_version = "X11; Linux x86_64"
    
    user_agent = f"Mozilla/5.0 ({random.choice([mac_version, win_version, linux_version])}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version}.0.0.0 Safari/537.36"
    return user_agent

async def captcha_solver(session):
    # solve the captcha until correct
    while True:
        try:
            h4_el = await session.get_element("h4")
            h4_text = await h4_el.get_text()
            is_captcha_page = (h4_text == 'Enter the characters you see below')
        except:
            is_captcha_page = False

        if not is_captcha_page:
            return
    
        # get captcha link
        img_el = await session.get_element("img")
        img_src = await img_el.get_attribute("src")

        # solve the captcha
        captcha = AmazonCaptcha.fromlink(img_src)
        solution = captcha.solve()
            
        # input solution to textbox
        textbox_el = await session.get_element("#captchacharacters")
        await textbox_el.clear()
        await textbox_el.send_keys(solution)
            
        # click submit button
        button_el = await session.get_element("button")
        await button_el.click()


async def extract_details(urls, headless, random_user_agent, temp_file_path):
    # for delete temporary files
    delete_temp_every = 50
    url_counter = 0

    # iterate through multiple pages
    results = []
    for url in tqdm(urls):
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
                ]
            }
        }

        if headless:
            browser.capabilities['goog:chromeOptions']['args'].append("--headless")

        # random user_agent to avoid captcha
        if random_user_agent:
            user_agent_string = random.choice(user_agent_list)
            # user_agent_string = get_user_agent()
            browser.capabilities['goog:chromeOptions']['args'].append(f"user-agent={user_agent_string}")

        async with get_session(service, browser) as session:
            # navigate to the web page
            await session.get(url)

            # SOLVING CAPTCHA
            await captcha_solver(session)

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

        # delete temp files
        url_counter += 1
        if url_counter % delete_temp_every == 0:
            try:
                for f in glob.glob(f"{temp_file_path}/scoped_dir*"):
                    shutil.rmtree(f)
            except Exception as e:
                    print(e)

    return results


def main(filename, idx_range, headless, random_user_agent, temp_file_path):
    """
    Scrape product details from Amazon using Arsenic.

    Parameters:
        filename (str): Name of the CSV file containing the "ProductURL" column to be scraped.
        idx_range (tuple): Start and end index (end index not included) of the rows to be scraped from the CSV file.
        headless (bool): If True, the browser UI won't pop up during the scraping process.
        random_user_agent (bool): If True, a random user agent will be used for each page.
        temp_file_path (str): Path to the directory where the temporary files will be stored during the scraping process.
    """

    # suppress log from arsenic
    set_arsenic_log_level()

    # list of pages to be scraped
    pages = pd.read_csv(filename)["ProductURL"].to_list()
    pages = pages[idx_range[0] : idx_range[1]]

    results = asyncio.run(extract_details(pages, headless, random_user_agent, temp_file_path))

    # convert result list to dataframe
    df = pd.DataFrame(results)
 
    # save to csv
    FOLDER_NAME = "results"
    if not os.path.exists(FOLDER_NAME):                                                                                                                                                          
        os.makedirs(FOLDER_NAME)

    now = datetime.now()
    datetime_string = now.strftime("%Y%m%d_%H%M%S")
    if 'missing' in filename:
        filename = f"{datetime_string}_missing_from_{idx_range[0]}_to_{idx_range[1]}_scrap_results.csv"
    else:
        filename = f"{datetime_string}_from_{idx_range[0]}_to_{idx_range[1]}_scrap_results.csv"
    df.to_csv(os.path.join(FOLDER_NAME, filename), index=False)


if __name__ == "__main__":
    # please specify the parameters here
    main(
        filename="ProductURL.csv",
        idx_range=(30000, 35000),
        headless=True,
        random_user_agent=False,
        temp_file_path="C:/Users/tomyt/AppData/Local/Temp",
    )
