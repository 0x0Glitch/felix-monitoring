#!/bin/bash

# Felix User Monitoring Service Runner

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Felix User Monitoring Service${NC}"
echo "================================"

# Check for .env file
if [ ! -f .env ]; then
    echo -e "${YELLOW}Warning: .env file not found${NC}"
    if [ -f user_monitoring/.env.example ]; then
        echo "Creating .env from example..."
        cp user_monitoring/.env.example .env
        echo -e "${YELLOW}Please edit .env with your configuration${NC}"
        exit 1
    fi
fi

# Load environment variables
export $(grep -v '^#' .env | xargs)

# Check required variables
if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}Error: DATABASE_URL not set${NC}"
    exit 1
fi

if [ -z "$TARGET_MARKETS" ]; then
    echo -e "${YELLOW}Warning: TARGET_MARKETS not set, using defaults${NC}"
    export TARGET_MARKETS="BTC,ETH"
fi

echo "Configuration:"
echo "  Markets: $TARGET_MARKETS"
echo "  Chain: ${CHAIN_TYPE:-Mainnet}"
echo "  Update Interval: ${POSITION_UPDATE_INTERVAL:-10}s"
echo ""

# Install dependencies if needed
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

echo "Installing dependencies..."
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q asyncpg aiohttp websockets psycopg2-binary python-dotenv

# Run the service
echo -e "${GREEN}Starting user monitoring service...${NC}"
echo ""
python -m user_monitoring.main
