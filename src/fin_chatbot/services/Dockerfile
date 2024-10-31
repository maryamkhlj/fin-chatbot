FROM python:3.9
WORKDIR /fin_chatbot

# Set the timezone
ENV TZ=America/Toronto
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    cron \
    bash \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create log files
RUN mkdir -p /var/log
RUN touch /var/log/cron.log /var/log/scheduled_fetch.log

# Set up cron job -- redis every 4hr
RUN echo "0 */4 * * * /usr/local/bin/python /fin_chatbot/scheduled_fetch_to_redis.py >> /var/log/scheduled_fetch.log 2>&1" > /etc/cron.d/fetch_stocks

# Set proper permissions
RUN chmod +x /fin_chatbot/*.py /fin_chatbot/start.sh
RUN chmod 0644 /etc/cron.d/fetch_stocks /var/log/cron.log /var/log/scheduled_fetch.log

# Make sure the script uses Unix line endings
RUN sed -i 's/\r$//' /fin_chatbot/start.sh

# Install the cron job
RUN crontab /etc/cron.d/fetch_stocks

# Set the entrypoint to our start script
CMD ["/bin/bash", "/fin_chatbot/start.sh"]