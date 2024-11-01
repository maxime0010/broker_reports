from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import mysql.connector
import os

# MySQL configuration
db_config = {
    'user': 'doadmin',
    'password': os.getenv("MYSQL_MDP"),
    'host': os.getenv("MYSQL_HOST"),
    'database': 'defaultdb',
    'port': 25060
}

# List of tickers
tickers = ['MSFT']  # Add more tickers as needed

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Update the path to chromedriver if needed
service = Service(executable_path="/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=chrome_options)

def save_to_database(data):
    """Save the scraped data to MySQL."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        for entry in data:
            query = """
                INSERT INTO analyst_ratings (ticker, analyst, firm, rating, action, price_target, upside, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                entry['ticker'], entry['analyst'], entry['firm'], entry['rating'], 
                entry['action'], entry['price_target'], entry['upside'], entry['date']
            ))
        conn.commit()
        print("Data saved to database")
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
        conn.close()

# Iterate over each ticker and scrape data
for ticker in tickers:
    url = f"https://stockanalysis.com/stocks/{ticker.lower()}/ratings/"
    driver.get(url)

    # Wait until the ratings table is present
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".rating-table tbody"))
        )
        
        rows = driver.find_elements(By.CSS_SELECTOR, ".rating-table tbody tr")
        
        # List to store scraped data
        ticker_data = []
        
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            data = {
                'ticker': ticker,
                'analyst': columns[0].text.strip(),
                'firm': columns[1].text.strip(),
                'rating': columns[3].text.strip(),
                'action': columns[4].text.strip(),
                'price_target': columns[5].text.replace('$', '').replace(',', '').strip(),
                'upside': columns[6].text.replace('%', '').strip(),
                'date': columns[7].text.strip()
            }
            ticker_data.append(data)
        
        # Save the ticker's data to the database
        if ticker_data:
            save_to_database(ticker_data)

    except Exception as e:
        print(f"Error occurred for ticker {ticker}: {str(e)}")

# Clean up
driver.quit()
