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
    filename='/root/broker_reports/error.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MySQL configuration
db_config = {
    'user': 'doadmin',
    'password': os.getenv("MYSQL_MDP"),
    'host': os.getenv("MYSQL_HOST"),
    'database': 'defaultdb',
    'port': 25060
}

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

def save_to_database(data):
    """Save the scraped data to MySQL, avoiding duplicates."""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        for entry in data:
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
        logging.debug("Data saved to database without duplicates")
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_oldest_ticker():
    """Get the ticker with the oldest update date from the tracking table."""
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
        logging.debug(f"Oldest ticker fetched: {result}")
        return result['ticker'] if result else None
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_last_updated_date(ticker):
    """Update the last_updated date for a given ticker in the tracking table."""
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
        logging.error(f"Database error: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Find the ticker to update
ticker = get_oldest_ticker()

if ticker:
    url = f"https://stockanalysis.com/stocks/{ticker.lower()}/ratings/"
    driver.get(url)

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".rating-table tbody"))
        )
        
        rows = driver.find_elements(By.CSS_SELECTOR, ".rating-table tbody tr")
        ticker_data = []  # Initialize ticker_data here
        
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
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

        # Filter duplicates before saving
        unique_entries = set()
        ticker_data_filtered = []
        for entry in ticker_data:
            record_identifier = (
                entry['ticker'], entry['analyst'], entry['firm'], entry['rating'], 
                entry['action'], entry['price_target'], entry['upside'], entry['date']
            )
            if record_identifier not in unique_entries:
                unique_entries.add(record_identifier)
                ticker_data_filtered.append(entry)

        if ticker_data_filtered:
            save_to_database(ticker_data_filtered)
            update_last_updated_date(ticker)

    except Exception as e:
        logging.error(f"Error occurred for ticker {ticker}: {str(e)}")

driver.quit()
logging.debug("Script completed.")
