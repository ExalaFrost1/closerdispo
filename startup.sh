#!/bin/bash

echo "Copying n /etc/systemd/system/"
sudo cp /home/vicimanager/CloserDispo/CloserDispo.service /etc/systemd/system/CloserDispo.service

echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "Enabling vici.service..."
sudo systemctl enable CloserDispo.service

echo "Restarting vici service..."
sudo systemctl restart CloserDispo.service

# Check status
sudo systemctl status CloserDispo.service --no-pager
