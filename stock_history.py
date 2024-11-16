def save_to_database(data):
    """Save the scraped data to MySQL, avoiding duplicates."""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Loop through the data and insert only if it doesn't already exist
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

# Filter duplicates in `ticker_data`
ticker_data_filtered = []
unique_entries = set()

for entry in ticker_data:
    # Create a tuple as a unique identifier for each record
    record_identifier = (
        entry['ticker'], entry['analyst'], entry['firm'], entry['rating'], 
        entry['action'], entry['price_target'], entry['upside'], entry['date']
    )
    if record_identifier not in unique_entries:
        unique_entries.add(record_identifier)
        ticker_data_filtered.append(entry)

# Save filtered data
if ticker_data_filtered:
    save_to_database(ticker_data_filtered)
    update_last_updated_date(ticker)
