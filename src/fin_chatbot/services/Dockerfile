FROM python:3.9
WORKDIR /fin_chatbot

# Set the timezone
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    cron \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Ensure scripts are executable
RUN chmod +x /fin_chatbot/*.py
RUN chmod +x /fin_chatbot/start.sh

# Create log files
RUN touch /var/log/cron.log /var/log/scheduled_fetch.log

# Set up cron jobs -- redis every 4hr and postgres at 11:59PM
RUN echo "0 */4 * * * /usr/local/bin/python /fin_chatbot/scheduled_fetch_to_redis.py >> /var/log/cron.log 2>&1" > /etc/cron.d/fetch_stocks
RUN echo "57 11 * * * /usr/local/bin/python /fin_chatbot/daily_redis_to_postgres.py >> /var/log/cron.log 2>&1" > /etc/cron.d/daily_transfer
RUN chmod 0644 /etc/cron.d/fetch_stocks /etc/cron.d/daily_transfer

# Apply cron jobs
RUN crontab /etc/cron.d/fetch_stocks
RUN crontab /etc/cron.d/daily_transfer

# Set the entrypoint to our start script
CMD ["/fin_chatbot/start.sh"]