#!/usr/bin/env python3
import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import re

# Set to True to only print values and not update database
DEV_MODE = False

def connect_to_db():
    """Connect to the PostgreSQL database."""
    conn_string = os.environ.get('DATABASE_URL')
    if not conn_string:
        raise ValueError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(conn_string)

def get_distinct_urls():
    """Get all distinct URLs from the live_prizes table."""
    # Even in DEV_MODE, we'll still read URLs from the database
    # but we won't make any database updates
    print("Getting distinct URLs from database...")
    
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT url FROM live_prizes;")
        urls = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        if DEV_MODE and len(urls) == 0:
            print("DEV MODE: No URLs found in database, using test URL")
            # Fallback to test URL if no URLs found
            return ["https://www.palottery.pa.gov/Fast-Play/View-Game.aspx?id=5217"]
        
        return urls
    except Exception as e:
        print(f"Error getting URLs: {e}")
        if DEV_MODE:
            print("DEV MODE: Using test URL due to database error")
            return ["https://www.palottery.pa.gov/Fast-Play/View-Game.aspx?id=5217"]
        return []

# Dictionary mapping game names to their static properties
# Use uppercase keys to match the capitalized game names in the database
GAME_PROPERTIES = {
    "DIAMONDS AND GOLD": {
        "increment": 2.4,
        "price": 30
    }
    # Add other games as needed with their properties
}

def parse_pa_datetime(date_str):
    """
    Parse the Pennsylvania Lottery date format "M/D/YYYY h:MM:SS AM/PM"
    into a Python datetime object.
    
    Example: "5/28/2025 1:29:54 PM" -> datetime object
    """
    # Remove the "As of " prefix if present
    if "As of " in date_str:
        date_str = date_str.replace("As of ", "")
    
    # Strip any whitespace
    date_str = date_str.strip()
    
    try:
        # Parse the date string
        return datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        # Fallback to just trying the date without seconds
        try:
            return datetime.strptime(date_str, "%m/%d/%Y %I:%M %p")
        except ValueError:
            # If all else fails, return current time
            print(f"Could not parse date string: {date_str}")
            return datetime.now()

def scrape_top_prize(url):
    """Scrape the live top prize data from the given URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the progressive jackpot info section
        jackpot_info = soup.find('div', class_='fp-progressive-jackpot-info')
        
        # Initialize prize table data
        prize_data = {
            'prize1': None, 'prize2': None, 'prize3': None, 
            'prize4': None, 'prize5': None, 'prize6': None,
            'prize1value': None, 'prize2value': None, 'prize3value': None,
            'prize4value': None, 'prize5value': None, 'prize6value': None
        }
        
        # Scrape the prize table
        try:
            # Look for the table with the prizes
            prize_table = soup.find('table', class_='table-global')
            if prize_table:
                # Get all table rows
                rows = prize_table.find('tbody').find_all('tr')
                
                # Extract prize values and remaining counts
                for i, row in enumerate(rows[:6]):  # Limit to first 6 rows
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        # Store the value from the first column (prize name/value)
                        prize_data[f'prize{i+1}value'] = cols[0].text.strip()
                        # Store the value from the second column (wins remaining)
                        prize_data[f'prize{i+1}'] = cols[1].text.strip()
                
                print("Extracted prize table data:")
                for i in range(1, 7):
                    print(f"prize{i}value: {prize_data[f'prize{i}value']}, prize{i}: {prize_data[f'prize{i}']}")
            else:
                print("Prize table not found")
        except Exception as e:
            print(f"Error extracting prize table data: {e}")
        
        if jackpot_info:
            # Extract the prize amount from the strong tag
            prize_amount = jackpot_info.find('strong').text.strip()
            
            # Clean up the prize amount (remove "Est." and any other non-numeric characters)
            # Keep the commas for formatting
            prize_amount = re.search(r'\$([\d,]+)', prize_amount)
            if prize_amount:
                prize_amount = prize_amount.group(1)
                
            # Extract the game name - specifically look in the fp-detail section
            game_name_element = soup.select_one('#fp-detail h3')
            if game_name_element:
                game_name = game_name_element.text.strip()
            else:
                # Fallback to the title in the page header if specific selector doesn't work
                title_element = soup.find('title')
                if title_element:
                    title_text = title_element.text.strip()
                    # Title format is typically "Pennsylvania Lottery - Fast Play - Diamonds and Gold"
                    # Extract the game name after the last dash
                    if '-' in title_text:
                        game_name = title_text.split('-')[-1].strip()
                    else:
                        game_name = title_text
                else:
                    # Last resort fallback
                    game_name = soup.find('h3').text.strip()
            
            # Convert game name to uppercase
            game_name = game_name.upper()
            
            # Extract the timestamp from the website instead of using server time
            jackpot_datetime_element = jackpot_info.find('span', class_='fp-progressive-jackpot-datetime')
            if jackpot_datetime_element and jackpot_datetime_element.text.strip():
                # Parse the date string from the website
                jackpot_time_str = jackpot_datetime_element.text.strip()
                current_time = parse_pa_datetime(jackpot_time_str)
                print(f"Using website timestamp: {jackpot_time_str} -> {current_time}")
            else:
                # Fallback to server time if the timestamp is not available
                current_time = datetime.now()
                print("Website timestamp not found, using server time")
            
            # Get game properties if available
            game_props = GAME_PROPERTIES.get(game_name, {"increment": None, "price": None})
            
            result = {
                'game_name': game_name,
                'top_prize': prize_amount,
                'time': current_time,
                'increment': game_props["increment"],
                'price': game_props["price"],
                # Add prize table data
                'prize1': prize_data['prize1'],
                'prize2': prize_data['prize2'],
                'prize3': prize_data['prize3'],
                'prize4': prize_data['prize4'],
                'prize5': prize_data['prize5'],
                'prize6': prize_data['prize6'],
                'prize1value': prize_data['prize1value'],
                'prize2value': prize_data['prize2value'],
                'prize3value': prize_data['prize3value'],
                'prize4value': prize_data['prize4value'],
                'prize5value': prize_data['prize5value'],
                'prize6value': prize_data['prize6value']
            }
            
            return result
        else:
            print(f"Could not find prize information for {url}")
            return None
    
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def get_previous_record(game_name):
    """Get the most recent record for a specific game."""
    if DEV_MODE:
        print(f"DEV MODE: Would get previous record for {game_name}")
        return None
        
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT time, top_prize, increment 
            FROM live_prizes 
            WHERE game_name = %s 
            ORDER BY time DESC 
            LIMIT 1
            """,
            (game_name,)
        )
        result = cur.fetchone()
        return result  # Returns (time, top_prize, increment) or None if no previous record
    
    except Exception as e:
        print(f"Error retrieving previous record: {e}")
        return None
    
    finally:
        cur.close()
        conn.close()

def update_database(data, url):
    """
    Add a new row to the live_prizes table with the latest data.
    Calculate actual_sales and implied_hourly_sales based on previous record.
    Skip insertion if a record with the same timestamp already exists.
    
    In DEV_MODE, just print the data without updating the database.
    """
    if not data:
        return
    
    # If in DEV_MODE, just print the data and return
    if DEV_MODE:
        print("\n--- DEV MODE: Would insert the following data ---")
        print(f"Time: {data['time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Game: {data['game_name']}")
        print(f"Current Prize: ${data['top_prize']}")
        print(f"Increment: {data['increment']}")
        print(f"Price: ${data['price']}")
        print(f"Prize1 (Remaining): {data['prize1']}")
        print(f"Prize2 (Remaining): {data['prize2']}")
        print(f"Prize3 (Remaining): {data['prize3']}")
        print(f"Prize4 (Remaining): {data['prize4']}")
        print(f"Prize5 (Remaining): {data['prize5']}")
        print(f"Prize6 (Remaining): {data['prize6']}")
        print(f"Prize1 (Value): {data['prize1value']}")
        print(f"Prize2 (Value): {data['prize2value']}")
        print(f"Prize3 (Value): {data['prize3value']}")
        print(f"Prize4 (Value): {data['prize4value']}")
        print(f"Prize5 (Value): {data['prize5value']}")
        print(f"Prize6 (Value): {data['prize6value']}")
        print("--- DEV MODE: Database not updated ---\n")
        return
    
    # Initialize sales metrics as None (for first entries)
    actual_sales = None
    implied_hourly_sales = None
    
    # Get previous record for this game
    prev_record = get_previous_record(data['game_name'])
    
    # Calculate sales metrics if previous record exists
    if prev_record:
        prev_time, prev_top_prize, increment = prev_record
        
        # Check if the current timestamp is the same as the previous record
        if data['time'] == prev_time:
            print(f"Skipping duplicate record at {data['time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Game: {data['game_name']}")
            print(f"Current Prize: ${data['top_prize']}")
            return
        
        # Convert top prize strings to numbers for calculation
        current_prize = float(data['top_prize'].replace(',', ''))
        previous_prize = float(prev_top_prize.replace(',', ''))
        
        # Calculate prize difference and actual sales
        prize_diff = current_prize - previous_prize
        if increment:  # Use the increment from the database record
            actual_sales = prize_diff / float(increment)
        elif data['increment']:  # Fallback to the current increment value
            actual_sales = prize_diff / float(data['increment'])
        
        # Calculate time difference in minutes
        time_diff = (data['time'] - prev_time).total_seconds() / 60
        
        # Calculate implied hourly sales rate (if time difference is positive)
        if time_diff > 0:
            implied_hourly_sales = actual_sales * (60 / time_diff)
        
        print(f"Prize difference: ${prize_diff}")
        print(f"Time elapsed: {time_diff:.2f} minutes")
        print(f"Actual sales: {actual_sales:.2f} tickets")
        print(f"Implied hourly sales: {implied_hourly_sales:.2f} tickets/hour")
    
    # Check for existing record with the same timestamp
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        # Check if a record with the same timestamp already exists
        cur.execute(
            """SELECT COUNT(*) FROM live_prizes WHERE time = %s AND game_name = %s""",
            (data['time'], data['game_name'])
        )
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"Skipping duplicate record at {data['time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Game: {data['game_name']}")
            print(f"Current Prize: ${data['top_prize']}")
            return
        
        # Insert new record with current timestamp, sales metrics, and prize data
        cur.execute(
            """
            INSERT INTO live_prizes 
                (time, game_name, top_prize, url, increment, price, 
                 actual_sales, implied_hourly_sales, 
                 prize1, prize2, prize3, prize4, prize5, prize6,
                 prize1value, prize2value, prize3value, prize4value, prize5value, prize6value) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s)
            """,
            (data['time'], data['game_name'], data['top_prize'], url, 
             data['increment'], data['price'], actual_sales, implied_hourly_sales,
             data['prize1'], data['prize2'], data['prize3'], data['prize4'], 
             data['prize5'], data['prize6'],
             data['prize1value'], data['prize2value'], data['prize3value'], 
             data['prize4value'], data['prize5value'], data['prize6value'])
        )
        conn.commit()
        formatted_time = data['time'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"Added new record at {formatted_time}")
        print(f"Game: {data['game_name']}")
        print(f"Current Prize: ${data['top_prize']}")
        print(f"Increment: {data['increment']}")
        print(f"Price: ${data['price']}")
        print(f"Prize data captured: {data['prize1']}, {data['prize2']}, {data['prize3']}, {data['prize4']}, {data['prize5']}, {data['prize6']}")
        print(f"Prize values captured: {data['prize1value']}, {data['prize2value']}, {data['prize3value']}, {data['prize4value']}, {data['prize5value']}, {data['prize6value']}")
    
    except Exception as e:
        conn.rollback()
        print(f"Error updating database: {e}")
    
    finally:
        cur.close()
        conn.close()

def show_recent_entries(limit=5):
    """
    Display the most recent entries in the live_prizes table
    to verify that new rows are being added correctly.
    
    In DEV_MODE, just print a message.
    """
    if DEV_MODE:
        print("\n--- DEV MODE: Would show recent entries ---")
        print(f"--- DEV MODE: Limited to {limit} entries ---\n")
        return
        
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT time, game_name, top_prize, increment, price, 
                   actual_sales, implied_hourly_sales,
                   prize1, prize2, prize3, prize4, prize5, prize6,
                   prize1value, prize2value, prize3value, prize4value, prize5value, prize6value
            FROM live_prizes 
            ORDER BY time DESC 
            LIMIT %s
            """,
            (limit,)
        )
        entries = cur.fetchall()
        
        if entries:
            print("\nRecent entries in live_prizes table:")
            print("-" * 200)
            print(f"{'Timestamp':<20} {'Game Name':<20} {'Top Prize':<15} {'Inc.':<5} {'$':<3} {'Sales':<7} {'Hourly':<8} {'Prize1':<6} {'Prize2':<6} {'Prize3':<6} {'Prize4':<6} {'Prize5':<6} {'Prize6':<6} {'Prize1 Value':<10} {'Prize2 Value':<10} {'Prize3 Value':<10} {'Prize4 Value':<10} {'Prize5 Value':<10} {'Prize6 Value':<10}")
            print("-" * 200)
            
            for entry in entries:
                timestamp, game, prize, increment, price, sales, hourly, p1, p2, p3, p4, p5, p6, p1v, p2v, p3v, p4v, p5v, p6v = entry
                formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                sales_str = f"{sales:.2f}" if sales is not None else "N/A"
                hourly_str = f"{hourly:.2f}" if hourly is not None else "N/A"
                print(f"{formatted_time:<20} {game:<20} ${prize:<15} {increment if increment else 'N/A':<5} "
                      f"${price if price else 'N/A':<3} {sales_str:<7} {hourly_str:<8} "
                      f"{p1:<6} {p2:<6} {p3:<6} {p4:<6} {p5:<6} {p6:<6} "
                      f"{p1v:<10} {p2v:<10} {p3v:<10} {p4v:<10} {p5v:<10} {p6v:<10}")
        else:
            print("No entries found in the live_prizes table.")
    
    except Exception as e:
        print(f"Error retrieving recent entries: {e}")
    
    finally:
        cur.close()
        conn.close()

def main():
    """Main function to run the script."""
    print("Starting live prize scraper...")
    print("DEV MODE: " + ("ENABLED" if DEV_MODE else "DISABLED"))
    
    # Get all distinct URLs from the database
    urls = get_distinct_urls()
    print(f"Found {len(urls)} distinct URLs to scrape")
    
    # Scrape each URL and update the database
    for url in urls:
        print(f"Scraping {url}...")
        data = scrape_top_prize(url)
        if data:
            update_database(data, url)
    
    # Show recent entries to verify new rows are being added
    show_recent_entries()
    
    print("\nScraping complete!")

if __name__ == "__main__":
    main()
