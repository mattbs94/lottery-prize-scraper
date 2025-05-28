#!/usr/bin/env python3
import os
import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import re

def connect_to_db():
    """Connect to the PostgreSQL database."""
    conn_string = os.environ.get('DATABASE_URL')
    if not conn_string:
        raise ValueError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(conn_string)

def get_distinct_urls():
    """Get all distinct URLs from the live_prizes table."""
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT url FROM live_prizes;")
    urls = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return urls

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
            
            return {
                'game_name': game_name,
                'top_prize': prize_amount,
                'time': current_time,
                'increment': game_props["increment"],
                'price': game_props["price"]
            }
        else:
            print(f"Could not find prize information for {url}")
            return None
    
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def get_previous_record(game_name):
    """Get the most recent record for a specific game."""
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
    """
    if not data:
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
        
        # Insert new record with current timestamp and sales metrics
        cur.execute(
            """
            INSERT INTO live_prizes 
                (time, game_name, top_prize, url, increment, price, actual_sales, implied_hourly_sales) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (data['time'], data['game_name'], data['top_prize'], url, 
             data['increment'], data['price'], actual_sales, implied_hourly_sales)
        )
        conn.commit()
        formatted_time = data['time'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"Added new record at {formatted_time}")
        print(f"Game: {data['game_name']}")
        print(f"Current Prize: ${data['top_prize']}")
        print(f"Increment: {data['increment']}")
        print(f"Price: ${data['price']}")
    
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
    """
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT time, game_name, top_prize, increment, price, actual_sales, implied_hourly_sales 
            FROM live_prizes 
            ORDER BY time DESC 
            LIMIT %s
            """,
            (limit,)
        )
        entries = cur.fetchall()
        
        if entries:
            print("\nRecent entries in live_prizes table:")
            print("-" * 120)
            print(f"{'Timestamp':<20} {'Game Name':<20} {'Top Prize':<15} {'Increment':<10} {'Price':<5} {'Sales':<10} {'Hourly Rate':<15}")
            print("-" * 120)
            
            for entry in entries:
                timestamp, game, prize, increment, price, sales, hourly = entry
                formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                sales_str = f"{sales:.2f}" if sales is not None else "N/A"
                hourly_str = f"{hourly:.2f}" if hourly is not None else "N/A"
                print(f"{formatted_time:<20} {game:<20} ${prize:<15} {increment if increment else 'N/A':<10} "
                      f"${price if price else 'N/A':<5} {sales_str:<10} {hourly_str:<15}")
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
