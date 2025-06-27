#!/bin/bash

echo "Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null

echo "Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null

# echo "Removing all images..."
# docker rmi -f $(docker images -q) 2>/dev/null

echo "Removing all volumes..."
docker volume rm $(docker volume ls -q) 2>/dev/null

echo "Removing all networks (except default)..."
docker network rm $(docker network ls | grep -v "bridge\|host\|none" | awk '{ if(NR>1) print $1 }') 2>/dev/null

echo "Pruning builder cache..."
docker builder prune -af 2>/dev/null

echo "Docker system cleaned completely."
