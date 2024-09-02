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

service = Service(executable_path="/path/to/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

# Iterate over each ticker and download the historical data
for ticker in tickers:
    try:
        url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/historical?page=1&rows_per_page=10&timeline=y10"
        driver.get(url)

        # Allow the page to load fully
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'Download Data')]")))

        # Click on the "Download historical data" button
        download_button = driver.find_element(By.XPATH, "//a[contains(text(), 'Download Data')]")
        download_button.click()

        # Wait for the download to complete
        time.sleep(5)  # Adjust as necessary based on file size

        # Rename the downloaded file
        downloaded_file = os.path.join(download_dir, "historical.csv")
        renamed_file = os.path.join(download_dir, f"{ticker}.csv")
        if os.path.exists(downloaded_file):
            os.rename(downloaded_file, renamed_file)
            print(f"Downloaded and renamed {ticker}.csv")
        else:
            print(f"Failed to download data for {ticker}")

    except Exception as e:
        print(f"An error occurred for {ticker}: {e}")

# Clean up
driver.quit()
