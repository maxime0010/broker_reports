from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(
    filename='/root/broker_reports/debug.log',  # Use a different log file for detailed debug logs
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.debug("Starting script execution...")

# MySQL configuration
db_config = {
    'user': 'doadmin',
    'password': os.getenv("MYSQL_MDP"),
    'host': os.getenv("MYSQL_HOST"),
    'database': 'defaultdb',
    'port': 25060
}

# Check environment variables
if not db_config['password'] or not db_config['host']:
    logging.error("MYSQL_MDP or MYSQL_HOST environment variables are not set.")
else:
    logging.debug(f"MySQL host: {db_config['host']}, User: {db_config['user']}")

# Set up Chrome options
logging.debug("Configuring Selenium WebDriver...")
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
service = Service(executable_path="/usr/bin/chromedriver")

try:
    driver = webdriver.Chrome(service=service, options=chrome_options)
    logging.debug("Selenium WebDriver successfully initialized.")
except Exception as e:
    logging.error(f"Failed to initialize Selenium WebDriver: {e}")
    raise

def save_to_database(data):
    """Save the scraped data to MySQL, avoiding duplicates."""
    logging.debug("Saving data to the database...")
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        for entry in data:
            logging.debug(f"Saving entry: {entry}")
            query = """
                INSERT INTO analyst_ratings (ticker, analyst, firm, rating, action, price_target, upside, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                rating = VALUES(rating),
                action = VALUES(action),
                price_target = VALUES(price_target),
                upside = VALUES(upside),
                date = VALUES(date)
            """
            cursor.execute(query, (
                entry['ticker'], entry['analyst'], entry['firm'], entry['rating'], 
                entry['action'], entry['price_target'], entry['upside'], entry['date']
            ))
        conn.commit()
        logging.debug("Data committed to the database.")
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_oldest_ticker():
    """Get the ticker with the oldest update date from the tracking table."""
    logging.debug("Fetching the oldest ticker to update...")
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT ticker FROM scraping_progress
            ORDER BY last_updated ASC
            LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        logging.debug(f"Fetched ticker: {result}")
        return result['ticker'] if result else None
    except mysql.connector.Error as err:
        logging.error(f"Database error while fetching ticker: {err}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_last_updated_date(ticker):
    """Update the last_updated date for a given ticker in the tracking table."""
    logging.debug(f"Updating last_updated date for ticker: {ticker}")
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
            INSERT INTO scraping_progress (ticker, last_updated)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_updated = %s
        """
        today = datetime.today().strftime('%Y-%m-%d')
        cursor.execute(query, (ticker, today, today))
        conn.commit()
        logging.debug(f"Updated last_updated date for ticker: {ticker}")
    except mysql.connector.Error as err:
        logging.error(f"Database error while updating last_updated date: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Main execution
ticker = get_oldest_ticker()
if ticker:
    logging.debug(f"Ticker to update: {ticker}")
    url = f"https://stockanalysis.com/stocks/{ticker.lower()}/ratings/"
    logging.debug(f"Navigating to URL: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".rating-table tbody"))
        )
        logging.debug("Ratings table located on the page.")

        rows = driver.find_elements(By.CSS_SELECTOR, ".rating-table tbody tr")
        logging.debug(f"Found {len(rows)} rows in the table.")
        ticker_data = []

        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            logging.debug(f"Processing row: {row.text}")
            price_target_text = columns[5].text.replace('$', '').replace(',', '').strip()
            price_target = float(price_target_text.split('→')[-1].strip()) if '→' in price_target_text else float(price_target_text)
            date = datetime.strptime(columns[7].text.strip(), "%b %d, %Y").strftime("%Y-%m-%d")
            
            data = {
                'ticker': ticker,
                'analyst': columns[0].text.strip(),
                'firm': columns[1].text.strip(),
                'rating': columns[3].text.strip(),
                'action': columns[4].text.strip(),
                'price_target': price_target,
                'upside': columns[6].text.replace('%', '').strip(),
                'date': date
            }
            ticker_data.append(data)

        logging.debug(f"Collected data: {ticker_data}")

        if ticker_data:
            save_to_database(ticker_data)
            update_last_updated_date(ticker)

    except Exception as e:
        logging.error(f"Error during scraping for ticker {ticker}: {e}")

else:
    logging.debug("No ticker found to update.")

driver.quit()
logging.debug("Script execution completed.")
