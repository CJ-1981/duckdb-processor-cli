#!/bin/bash
# DuckDB Processor Gradio App Launcher
# One-click launcher for macOS and Linux with automatic venv setup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Virtual environment directory
VENV_DIR="venv"

# Python command
PYTHON_CMD="python3"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         DuckDB Processor Gradio App Launcher               ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if Python 3 is installed
if ! command -v $PYTHON_CMD &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found. Please install Python 3.10+ first.${NC}"
    echo -e "${YELLOW}Visit: https://www.python.org/downloads/${NC}"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}✓ Found Python $PYTHON_VERSION${NC}"

# Check Python version (requires 3.10+)
PYTHON_MAJOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.major)')
PYTHON_MINOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.minor)')

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 10 ]); then
    echo -e "${RED}✗ Python 3.10+ required, found $PYTHON_VERSION${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}→ Creating virtual environment...${NC}"
    $PYTHON_CMD -m venv "$VENV_DIR"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to create virtual environment${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi

# Activate virtual environment
echo -e "${YELLOW}→ Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo -e "${YELLOW}→ Upgrading pip...${NC}"
pip install --quiet --upgrade pip

# Install/update dependencies
echo -e "${YELLOW}→ Checking dependencies...${NC}"

# Install base dependencies
echo -e "${YELLOW}  → Installing base dependencies...${NC}"
pip install --quiet -e .

# Install UI dependencies
echo -e "${YELLOW}  → Installing UI dependencies (Gradio, etc.)...${NC}"
pip install --quiet -e ".[ui]"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to install dependencies${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Dependencies installed${NC}"

# Check if gradio_app.py exists
if [ ! -f "gradio_app.py" ]; then
    echo -e "${RED}✗ gradio_app.py not found in current directory${NC}"
    exit 1
fi

# Launch Gradio app
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Starting DuckDB Processor Gradio Interface...             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Run the Gradio app
python3 gradio_app.py

# Handle exit
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo ""
    echo -e "${RED}✗ Gradio app exited with error code $exit_code${NC}"
    exit $exit_code
fi
