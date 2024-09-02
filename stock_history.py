import traceback
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

# List of tickers (example)
tickers = ['MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD']

# Set up the download directory and Chrome options
download_dir = "historical_data"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

chrome_options = Options()
prefs = {"download.default_directory": os.path.abspath(download_dir)}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--remote-debugging-port=9222")

# Update the path to chromedriver
service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

# Iterate over each ticker and download the historical data
for ticker in tickers:
    print(f"Processing ticker: {ticker}")
    try:
        url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/historical?page=1&rows_per_page=10&timeline=y10"
        driver.get(url)
        print(f"Page loaded for {ticker}")

        # Wait for the page to load completely
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )

        # Scroll down to ensure the download container is visible
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Allow time for the element to appear

        # Attempt to find and click the button
        try:
            # Method 1: Locate the "historical-download-container" and find the button
            download_container = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, "historical-download-container"))
            )
            download_button = download_container.find_element(By.CSS_SELECTOR, "button.historical-download")
            download_button.click()
            print(f"Clicked download button for {ticker}")
        except Exception as e:
            print(f"Method 1 failed for {ticker}: {str(e)}")

            try:
                # Method 2: Attempt to click using JavaScript directly
                download_button = driver.execute_script(
                    "return document.querySelector('.historical-download-container button.historical-download');"
                )
                if download_button:
                    driver.execute_script("arguments[0].click();", download_button)
                    print(f"Clicked download button for {ticker} using JavaScript")
                else:
                    print(f"JavaScript query returned no element for {ticker}")
            except Exception as e2:
                print(f"Method 2 (JavaScript click) failed for {ticker}: {str(e2)}")

                try:
                    # Method 3: Use full XPath to locate and click the button
                    download_button = WebDriverWait(driver, 30).until(
                        EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'historical-download-container')]//button[contains(@class, 'historical-download')]"))
                    )
                    download_button.click()
                    print(f"Clicked download button for {ticker} using full XPath")
                except Exception as e3:
                    print(f"Method 3 (XPath) failed for {ticker}: {str(e3)}")

                    try:
                        # Method 4: Dispatch a click event manually using JavaScript
                        download_button = driver.find_element(By.CSS_SELECTOR, ".historical-download-container button.historical-download")
                        driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {bubbles: true}));", download_button)
                        print(f"Dispatched click event for {ticker}")
                    except Exception as e4:
                        print(f"Method 4 (Dispatch event) failed for {ticker}: {str(e4)}")
                        driver.save_screenshot(f"{ticker}_error.png")

        # Wait for the download to complete
        time.sleep(10)  # Increase wait time to ensure file download completes

        # Rename the downloaded file
        downloaded_file = os.path.join(download_dir, "historical.csv")
        renamed_file = os.path.join(download_dir, f"{ticker}.csv")
        if os.path.exists(downloaded_file):
            os.rename(downloaded_file, renamed_file)
            print(f"Downloaded and renamed {ticker}.csv")
        else:
            print(f"Failed to download data for {ticker}")

    except Exception as e:
        print(f"An error occurred for {ticker}: {str(e)}")
        driver.save_screenshot(f"{ticker}_error.png")
        traceback.print_exc()

# Clean up
driver.quit()
