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
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/root/broker_reports/debug.log'),
        logging.StreamHandler()
    ]
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

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

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

# Main execution
ticker = "AMCR"  # Replace with dynamic ticker fetching logic
if ticker:
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
            try:
                columns = row.find_elements(By.TAG_NAME, "td")

                # Extract and handle price target
                price_target_text = columns[5].text.replace('$', '').replace(',', '').strip()
                if '→' in price_target_text:
                    price_target = price_target_text.split('→')[-1].strip()
                else:
                    price_target = price_target_text
                price_target = float(price_target) if price_target.lower() != 'n/a' else None

                # Extract and handle upside
                upside_text = columns[6].text.replace('%', '').strip()
                upside = float(upside_text) if upside_text.lower() != 'n/a' else None

                # Parse the date
                date_text = columns[7].text.strip()
                date = datetime.strptime(date_text, "%b %d, %Y").strftime("%Y-%m-%d")

                # Clean rating
                rating = columns[3].text.strip()
                if '→' in rating:
                    rating = rating.split('→')[-1].strip()

                data = {
                    'ticker': ticker,
                    'analyst': columns[0].text.strip(),
                    'firm': columns[1].text.strip(),
                    'rating': rating,
                    'action': columns[4].text.strip(),
                    'price_target': price_target,
                    'upside': upside,
                    'date': date
                }
                ticker_data.append(data)
                logging.debug(f"Processed data: {data}")

            except Exception as row_error:
                logging.error(f"Error processing row: {row.text}, Error: {row_error}")

        if ticker_data:
            save_to_database(ticker_data)

    except Exception as e:
        logging.error(f"Error during scraping for ticker {ticker}: {e}")

driver.quit()
logging.debug("Script execution completed.")
