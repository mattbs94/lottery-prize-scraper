# Lottery Prize Scraper

Automated tool to scrape and track lottery prize data for the Pennsylvania Lottery's "Diamonds and Gold" game.

## Features

- Scrapes live top prize data from the Pennsylvania Lottery website
- Tracks prize changes over time
- Calculates actual ticket sales between scrapes
- Projects hourly sales rates
- Stores historical data in PostgreSQL database
- Runs automatically every minute via Heroku scheduler

## Requirements

- Python 3.9+
- PostgreSQL database
- Required Python packages listed in requirements.txt

## Environment Variables

The application requires the following environment variables:

- `DATABASE_URL`: PostgreSQL connection string

## Deployment

This application is designed to be deployed to Heroku with a scheduler to run every minute.

### Heroku Setup

```bash
# Create Heroku app
heroku create lottery-prize-scraper

# Set environment variables
heroku config:set DATABASE_URL="your_database_url"

# Deploy to Heroku
git push heroku main

# Start the clock process
heroku ps:scale clock=1
```

## License

MIT
