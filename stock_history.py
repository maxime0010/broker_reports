from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
chrome_options.add_argument("--headless")  # Run in headless mode (no browser window)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")  # Applicable for Windows
chrome_options.add_argument("--remote-debugging-port=9222")  # Helps with DevTools issues

# Set up the WebDriver service (use Service to specify the path to chromedriver)
service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

# Iterate over each ticker and download the historical data
for ticker in tickers:
    try:
        url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/historical?page=1&rows_per_page=10&timeline=y10"
        driver.get(url)

        # Allow the page to load
        time.sleep(5)  # Adjust as necessary based on your connection speed

        # Click on the "Download historical data" button
        download_button = driver.find_element("xpath", "//a[contains(text(), 'Download Data')]")
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
