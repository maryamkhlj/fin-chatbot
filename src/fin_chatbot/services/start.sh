#!/bin/sh

# Check if cron is already running
if ! pgrep -x "cron" > /dev/null
then
    # Start the cron daemon
    /usr/sbin/cron
    echo "Cron daemon started" >> /var/log/cron.log
else
    echo "Cron daemon is already running" >> /var/log/cron.log
fi

# Function to run the scheduled fetch script
run_scheduled_fetch() {
    while true; do
        python /fin_chatbot/scheduled_fetch_to_redis.py >> /var/log/scheduled_fetch.log 2>&1
        echo "scheduled_fetch_to_redis.py exited, restarting in 5 seconds..." >> /var/log/scheduled_fetch.log
        sleep 5
    done
}

# Run the scheduled fetch script in the background
run_scheduled_fetch &

# Tail both log files to keep the container running and see output
tail -f /var/log/cron.log /var/log/scheduled_fetch.log