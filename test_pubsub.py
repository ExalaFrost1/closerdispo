#!/usr/bin/env python3

import os
import time
import json
import requests
import logging
from datetime import datetime
import pytz

# Configuration
CALL_COUNTER_FILE = "/var/log/asterisk/call_counter.txt"
#CALL_COUNTER_FILE = "/home/taha/call_counter/call_counter.txt"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1405944790079635537/c011MTDYQURritref5lDJ7gFQ6BwiJL1fNpYWg3exHq0UXwL7m9HciA4vvRR0YzFkTyy"
TECH_ROLE_ID = "1275962215857524756"
STATE_FILE = "/home/taha/call_counter_state.txt"
ALERT_STATE_FILE = "/home/taha/call_alert_state.json"
CHECK_INTERVAL = 600  # Check every 15 minutes
LOG_FILE = "/home/taha/call_monitor.log"

# Threshold settings
THRESHOLD = 500  # Alert every 500 calls (500, 1000, 1500, etc.)

# Notification settings
FOLLOW_UP_INTERVAL = 300  # 5 minutes between follow-ups
MAX_FOLLOW_UPS = 2  # Total 5 notifications (1 initial + 4 follow-ups)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)


def read_call_count():
    """Read current call count from file."""
    try:
        if os.path.exists(CALL_COUNTER_FILE):
            with open(CALL_COUNTER_FILE, 'r') as f:
                content = f.read().strip()
                return int(content) if content.isdigit() else 0
        return 0
    except:
        return 0


def read_last_count():
    """Read last known count."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                return int(f.read().strip())
        return 0
    except:
        return 0


def save_last_count(count):
    """Save current count."""
    try:
        with open(STATE_FILE, 'w') as f:
            f.write(str(count))
    except Exception as e:
        logging.error(f"Error saving count: {e}")


def read_alert_state():
    """Read alert state."""
    try:
        if os.path.exists(ALERT_STATE_FILE):
            with open(ALERT_STATE_FILE, 'r') as f:
                return json.load(f)
        return {
            "last_alert_time": 0,
            "follow_up_count": 0,
            "alert_active": False,
            "last_follow_up_time": 0,
            "last_threshold_crossed": 0
        }
    except:
        return {
            "last_alert_time": 0,
            "follow_up_count": 0,
            "alert_active": False,
            "last_follow_up_time": 0,
            "last_threshold_crossed": 0
        }

def get_pakistan_time():
    """Get current time in Pakistan timezone."""
    pakistan_tz = pytz.timezone('Asia/Karachi')
    return datetime.now(pakistan_tz)


def save_alert_state(state):
    """Save alert state."""
    try:
        with open(ALERT_STATE_FILE, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        logging.error(f"Error saving alert state: {e}")


def get_threshold_crossed(count):
    """Get the highest threshold crossed by this count."""
    return (count // THRESHOLD) * THRESHOLD


def has_crossed_new_threshold(current_count, last_count):
    """Check if a new threshold has been crossed."""
    current_threshold = get_threshold_crossed(current_count)
    last_threshold = get_threshold_crossed(last_count)
    return current_threshold > last_threshold and current_threshold > 0


def detect_daily_reset(current_count, last_count):
    """Detect if the counter file was reset (daily rollover)."""
    # If current count is significantly lower than last count, assume reset
    # Use threshold as the minimum difference to avoid false positives from small decreases
    return current_count < last_count and (last_count - current_count) > THRESHOLD


def reset_alert_state():
    """Reset alert state for new day."""
    return {
        "last_alert_time": 0,
        "follow_up_count": 0,
        "alert_active": False,
        "last_follow_up_time": 0,
        "last_threshold_crossed": 0
    }


def should_send_notification(alert_state, current_count, last_count):
    """Check if notification should be sent."""
    current_time = time.time()

    # Check for daily reset
    daily_reset_detected = detect_daily_reset(current_count, last_count)
    if daily_reset_detected:
        logging.info(f"Daily reset detected: {last_count} -> {current_count}")
        # Reset alert state for new day
        alert_state = reset_alert_state()
        save_alert_state(alert_state)

    # Check if we crossed a new threshold (normal case or after reset)
    threshold_crossed = has_crossed_new_threshold(current_count, last_count)
    current_threshold = get_threshold_crossed(current_count)

    # Handle threshold crossing after daily reset
    if daily_reset_detected and current_threshold > 0:
        # After reset, if current count crosses any threshold, send alert
        return True, "initial", current_threshold

    # Handle normal threshold crossing
    if threshold_crossed:
        new_threshold = current_threshold

        # If no alert is active or we crossed a higher threshold, send initial alert
        if (not alert_state["alert_active"] or
                new_threshold > alert_state.get("last_threshold_crossed", 0)):
            return True, "initial", new_threshold

    # Check for follow-up notifications
    if (alert_state["alert_active"] and
            alert_state["follow_up_count"] < MAX_FOLLOW_UPS and
            current_time - alert_state["last_follow_up_time"] >= FOLLOW_UP_INTERVAL):
        return True, "follow_up", alert_state.get("last_threshold_crossed", 0)

    # Check if alert period expired and we can start fresh
    if (alert_state["alert_active"] and
            current_time - alert_state["last_alert_time"] >= 3600):  # 1 hour
        alert_state["alert_active"] = False

    return False, "blocked", 0


def send_discord_notification(current_count, threshold_crossed, notification_type="initial", follow_up_count=0,
                              daily_reset=False):
    """Send Discord notification."""
    try:
        timestamp = get_pakistan_time().strftime("%Y-%m-%d %H:%M:%S PKT")

        if notification_type == "initial":
            if daily_reset:
                title = f"🔄 Daily Reset + Alert - {threshold_crossed} CALLS REACHED"
                content = f"🔄 **Daily counter reset detected - {threshold_crossed} calls threshold crossed again!**"
            else:
                title = f"🚨 Call Dump Alert - {threshold_crossed} CALLS REACHED"
                content = f"⚠️ **{threshold_crossed} Dumped Calls Threshold Crossed!**"
            color = 0xFF0000
            extra_fields = []
        else:
            title = f"🚨 Call Dump Alert - Follow-up #{follow_up_count}"
            color = 0xFF6600
            content = f"⚠️ **Call Dump Alert Update #{follow_up_count}**"
            remaining = MAX_FOLLOW_UPS - follow_up_count + 1
            extra_fields = [{
                "name": "Remaining Alerts",
                "value": f"{remaining} more in next {remaining * 5} minutes",
                "inline": False
            }]

        embed = {
            "title": title,
            "color": color,
            "fields": [
                          {"name": "Current Dumped Calls", "value": str(current_count), "inline": True},
                          {"name": "Threshold Crossed", "value": str(threshold_crossed), "inline": True},
                          {"name": "Next Threshold", "value": str(threshold_crossed + THRESHOLD), "inline": True}
                      ] + extra_fields,
            "footer": {"text": f"Checked at {timestamp}"}
        }

        payload = {"content": f"<@&{TECH_ROLE_ID}> {content}", "embeds": [embed]}

        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)

        if response.status_code == 204:
            reset_msg = " (after daily reset)" if daily_reset else ""
            logging.info(
                f"Alert sent: {notification_type} - Threshold {threshold_crossed} crossed{reset_msg}, Current: {current_count}")
        else:
            logging.error(f"Failed to send alert: {response.status_code}")

    except Exception as e:
        logging.error(f"Error sending notification: {e}")


def send_startup_notification():
    """Send startup notification."""
    try:
        current_count = read_call_count()
        timestamp = get_pakistan_time().strftime("%Y-%m-%d %H:%M:%S PKT")

        embed = {
            "title": "📊 Call Monitor Started",
            "color": 0x00FF00,
            "fields": [
                {"name": "Current Dumped Calls", "value": str(current_count), "inline": True},
                {"name": "Status", "value": "Monitoring Active", "inline": True}
            ],
            "footer": {"text": f"Started at {timestamp}"}
        }

        payload = {"content": f"<@&{TECH_ROLE_ID}> ✅ **Call Counter Monitor Started**", "embeds": [embed]}
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)

    except:
        pass


def monitor_calls():
    """Main monitoring function."""
    logging.info(f"Starting call counter monitor - Alert threshold: {THRESHOLD} calls")

    send_startup_notification()

    current_count = read_call_count()
    save_last_count(current_count)
    logging.info(f"Initial count: {current_count}")

    while True:
        try:
            current_count = read_call_count()
            last_count = read_last_count()
            alert_state = read_alert_state()

            # Check for daily reset before processing notifications
            daily_reset_detected = detect_daily_reset(current_count, last_count)

            should_notify, notification_type, threshold_crossed = should_send_notification(alert_state, current_count,
                                                                                           last_count)

            if should_notify:
                current_time = time.time()

                if notification_type == "initial":
                    send_discord_notification(current_count, threshold_crossed, "initial", 0, daily_reset_detected)

                    alert_state = {
                        "last_alert_time": current_time,
                        "follow_up_count": 0,
                        "alert_active": True,
                        "last_follow_up_time": current_time,
                        "last_threshold_crossed": threshold_crossed
                    }

                elif notification_type == "follow_up":
                    alert_state["follow_up_count"] += 1
                    alert_state["last_follow_up_time"] = current_time
                    threshold_crossed = alert_state.get("last_threshold_crossed", 0)

                    send_discord_notification(current_count, threshold_crossed, "follow_up",
                                              alert_state["follow_up_count"])

                    if alert_state["follow_up_count"] >= MAX_FOLLOW_UPS:
                        alert_state["alert_active"] = False
                        logging.info("Alert sequence completed")

                save_alert_state(alert_state)

            save_last_count(current_count)

            # Check for scheduled follow-ups
            alert_state = read_alert_state()
            if (alert_state["alert_active"] and
                    alert_state["follow_up_count"] < MAX_FOLLOW_UPS):

                current_time = time.time()
                if current_time - alert_state["last_follow_up_time"] >= FOLLOW_UP_INTERVAL:
                    alert_state["follow_up_count"] += 1
                    alert_state["last_follow_up_time"] = current_time
                    threshold_crossed = alert_state.get("last_threshold_crossed", 0)

                    send_discord_notification(current_count, threshold_crossed, "follow_up",
                                              alert_state["follow_up_count"])

                    if alert_state["follow_up_count"] >= MAX_FOLLOW_UPS:
                        alert_state["alert_active"] = False
                        logging.info("Alert sequence completed")

                    save_alert_state(alert_state)

            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logging.info("Monitor stopped")
            break
        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    monitor_calls()