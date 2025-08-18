#!/bin/bash

set -e  # Exit on any error

echo "🚀 Installing TRUF.NETWORK Postgres MCP..."
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect platform
PLATFORM="unknown"
case "$(uname -s)" in
    Darwin*)    PLATFORM="macos" ;;
    Linux*)     PLATFORM="linux" ;;
    CYGWIN*|MINGW*|MSYS*) PLATFORM="windows" ;;
esac

echo -e "${BLUE}Detected platform: $PLATFORM${NC}"

# Set Claude config path based on platform
case $PLATFORM in
    "macos")
        CLAUDE_CONFIG="$HOME/Library/Application Support/Claude/claude_desktop_config.json"
        ;;
    "linux")
        CLAUDE_CONFIG="$HOME/.config/claude/claude_desktop_config.json"
        ;;
    "windows")
        CLAUDE_CONFIG="$APPDATA/Claude/claude_desktop_config.json"
        ;;
    *)
        echo -e "${RED}❌ Unsupported platform: $PLATFORM${NC}"
        exit 1
        ;;
esac

echo -e "${BLUE}Claude config location: $CLAUDE_CONFIG${NC}"
echo ""

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check if Python 3.12+ is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is required but not installed${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
PYTHON_MAJOR=$(python3 -c 'import sys; print(sys.version_info.major)')
PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info.minor)')

echo -e "${BLUE}Found Python $PYTHON_VERSION${NC}"

# Check if Python version is 3.12 or higher
if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]); then
    echo -e "${RED}❌ Python 3.12+ is required, but found Python $PYTHON_VERSION${NC}"
    echo ""
    echo "Please install Python 3.12 or higher:"
    case $PLATFORM in
        "macos")
            echo "  brew install python@3.12"
            echo "  # or download from https://www.python.org/downloads/"
            ;;
        "linux")
            echo "  # Ubuntu/Debian:"
            echo "  sudo apt update && sudo apt install python3.12"
            echo "  # or use pyenv:"
            echo "  pyenv install 3.12.0 && pyenv global 3.12.0"
            ;;
        "windows")
            echo "  # Download from https://www.python.org/downloads/"
            echo "  # or use chocolatey:"
            echo "  choco install python --version=3.12.0"
            ;;
    esac
    exit 1
fi

echo -e "${GREEN}✅ Python $PYTHON_VERSION found (3.12+ required)${NC}"

# Check if pipx is available
if ! command -v pipx &> /dev/null; then
    echo -e "${RED}❌ pipx is required but not installed${NC}"
    echo ""
    echo "Please install pipx first:"
    case $PLATFORM in
        "macos")
            echo "  brew install pipx"
            echo "  pipx ensurepath"
            ;;
        "linux")
            echo "  python3 -m pip install --user pipx"
            echo "  python3 -m pipx ensurepath"
            ;;
        "windows")
            echo "  python -m pip install --user pipx"
            echo "  python -m pipx ensurepath"
            ;;
    esac
    echo ""
    echo "Then restart your terminal and run this script again."
    exit 1
fi

echo -e "${GREEN}✅ pipx found${NC}"

# Check if psql is available for testing connection
if ! command -v psql &> /dev/null; then
    echo -e "${YELLOW}⚠️  psql not found - connection testing will be skipped${NC}"
    PSQL_AVAILABLE=false
else
    echo -e "${GREEN}✅ psql found${NC}"
    PSQL_AVAILABLE=true
fi

echo ""

# Get database connection details
echo "📊 PostgreSQL Connection Setup"
echo "Please provide your local PostgreSQL connection details:"
echo ""

read -p "Host (default: localhost): " DB_HOST
DB_HOST=${DB_HOST:-localhost}

read -p "Port (default: 5432): " DB_PORT  
DB_PORT=${DB_PORT:-5432}

read -p "Database name: " DB_NAME
while [[ -z "$DB_NAME" ]]; do
    echo -e "${RED}Database name is required${NC}"
    read -p "Database name: " DB_NAME
done

read -p "Username: " DB_USER
while [[ -z "$DB_USER" ]]; do
    echo -e "${RED}Username is required${NC}"
    read -p "Username: " DB_USER
done

read -s -p "Password: " DB_PASSWORD
echo ""
while [[ -z "$DB_PASSWORD" ]]; do
    echo -e "${RED}Password is required${NC}"
    read -s -p "Password: " DB_PASSWORD
    echo ""
done

# Construct connection URI
DB_URI="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"

echo ""
echo "🔗 Connection details:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  Username: $DB_USER"
echo ""

# Test database connection if psql is available
if [ "$PSQL_AVAILABLE" = true ]; then
    echo "🧪 Testing database connection..."
    if psql "$DB_URI" -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Database connection successful!${NC}"
    else
        echo -e "${RED}❌ Database connection failed!${NC}"
        echo "Please check your connection details and try again."
        exit 1
    fi
    echo ""
fi

# Install the MCP server
echo "📦 Installing TRUF.NETWORK Postgres MCP server with pipx..."

# Check if already installed and uninstall if needed
if pipx list | grep -q "postgres-mcp"; then
    echo -e "${YELLOW}⚠️  Existing postgres-mcp installation found, removing...${NC}"
    pipx uninstall postgres-mcp
fi

# Install from current directory
pipx install .

echo -e "${GREEN}✅ Installed with pipx${NC}"
echo ""

# Configure Claude Desktop
echo "⚙️  Configuring Claude Desktop..."

# Create backup of existing config if it exists
if [ -f "$CLAUDE_CONFIG" ]; then
    BACKUP_FILE="$CLAUDE_CONFIG.backup.$(date +%s)"
    cp "$CLAUDE_CONFIG" "$BACKUP_FILE"
    echo -e "${YELLOW}📁 Backed up existing config to: $BACKUP_FILE${NC}"
fi

# Create config directory if it doesn't exist
mkdir -p "$(dirname "$CLAUDE_CONFIG")"

# Create or update Claude Desktop config
python3 -c "
import json
import os

config_path = '$CLAUDE_CONFIG'
db_uri = '$DB_URI'

# Read existing config or create new one
config = {}
if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            config = {}

# Ensure mcpServers exists
if 'mcpServers' not in config:
    config['mcpServers'] = {}

# Add truf-postgres server
config['mcpServers']['truf-postgres'] = {
    'command': 'postgres-mcp',
    'args': ['--access-mode=unrestricted'],
    'env': {
        'DATABASE_URI': db_uri
    }
}

# Write updated config
with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)

print('✅ Claude Desktop configuration updated!')
"

echo ""

# Verify installation
echo "🔍 Verifying installation..."
if pipx list | grep -q "postgres-mcp"; then
    echo -e "${GREEN}✅ TRUF.NETWORK Postgres MCP is installed and available${NC}"
    
    # Show where it's installed
    INSTALL_PATH=$(pipx list --verbose | grep -A 5 "postgres-mcp" | grep "installed package" | cut -d' ' -f4)
    if [ ! -z "$INSTALL_PATH" ]; then
        echo -e "${BLUE}📍 Installed at: $INSTALL_PATH${NC}"
    fi
else
    echo -e "${RED}❌ Installation verification failed${NC}"
    exit 1
fi

# Test the command
echo "🧪 Testing postgres-mcp command..."
if command -v postgres-mcp &> /dev/null; then
    echo -e "${GREEN}✅ postgres-mcp command is available${NC}"
else
    echo -e "${YELLOW}⚠️  postgres-mcp command not found in PATH${NC}"
    echo "You may need to run: pipx ensurepath"
    echo "Then restart your terminal"
fi

echo ""

# Final instructions
echo -e "${GREEN}🎉 Installation completed successfully!${NC}"
echo ""
echo "📋 What was installed:"
echo "  • TRUF.NETWORK Postgres MCP server (isolated with pipx)"
echo "  • Claude Desktop configuration"
echo ""
echo "🔧 Configuration details:"
echo "  • Server name: truf-postgres"
echo "  • Access mode: unrestricted"
echo "  • Database: $DB_NAME on $DB_HOST:$DB_PORT"
echo ""
echo -e "${YELLOW}📝 Next steps:${NC}"
echo "  1. Restart Claude Desktop application"
echo "  2. Claude will now have access to your PostgreSQL database"
echo "  3. Try asking Claude: 'List the tables in my database'"
echo ""
echo -e "${BLUE}💡 Available tools:${NC}"
echo "  • list_schemas - List database schemas"
echo "  • list_objects - List tables, views, etc."
echo "  • get_object_details - Get table structure"
echo "  • execute_sql - Run SQL queries"
echo "  • explain_query - Analyze query performance"
echo "  • analyze_db_health - Check database health"
echo ""
echo -e "${GREEN}Happy querying! 🚀${NC}"
echo ""
echo -e "${BLUE}To uninstall later: pipx uninstall postgres-mcp${NC}"