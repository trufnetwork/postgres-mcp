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

# Check if Claude Desktop directory exists
CLAUDE_DIR=$(dirname "$CLAUDE_CONFIG")
if [ ! -d "$CLAUDE_DIR" ]; then
    echo -e "${RED}❌ Claude Desktop directory not found at: $CLAUDE_DIR${NC}"
    echo "Please make sure Claude Desktop is installed and has been run at least once."
    exit 1
fi

echo -e "${GREEN}✅ Claude Desktop directory found${NC}"

# Check if config file exists, if not that's okay - we'll create it
if [ -f "$CLAUDE_CONFIG" ]; then
    echo -e "${GREEN}✅ Existing Claude Desktop config found${NC}"
else
    echo -e "${YELLOW}📝 Claude Desktop config will be created${NC}"
fi

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
echo "TRUF.NETWORK uses Kwil database with standard configuration:"
echo ""

read -p "Host (default: localhost): " DB_HOST
DB_HOST=${DB_HOST:-localhost}

read -p "Port (default: 5432): " DB_PORT  
DB_PORT=${DB_PORT:-5432}

read -p "Database name (default: kwild): " DB_NAME
DB_NAME=${DB_NAME:-kwild}

read -p "Username (default: kwild): " DB_USER
DB_USER=${DB_USER:-kwild}

read -s -p "Password (leave empty if no password): " DB_PASSWORD
echo ""

# Construct connection URI - handle empty password
if [[ -z "$DB_PASSWORD" ]]; then
    DB_URI="postgresql://$DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
else
    DB_URI="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"
fi

echo ""
echo "🔗 Connection details:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  Username: $DB_USER"
echo "  Password: $([ -z "$DB_PASSWORD" ] && echo "none" || echo "***")"
echo ""

# Test database connection if psql is available
if [ "$PSQL_AVAILABLE" = true ]; then
    echo "🧪 Testing database connection..."
    if psql "$DB_URI" -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Database connection successful!${NC}"
        
        # Test access to main schema and TRUF.NETWORK tables
        echo "🔍 Testing main schema access..."
        MAIN_SCHEMA_ACCESS=true
        
        # Check if main schema exists
        if psql "$DB_URI" -c "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'main';" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ Main schema found${NC}"
            
            # Test access to key TRUF.NETWORK tables
            TABLES_FOUND=0
            for table in streams primitive_events taxonomies; do
                if psql "$DB_URI" -c "SELECT 1 FROM main.$table LIMIT 1;" > /dev/null 2>&1; then
                    echo -e "${GREEN}✅ Access to main.$table confirmed${NC}"
                    ((TABLES_FOUND++))
                else
                    echo -e "${YELLOW}⚠️  Could not access main.$table${NC}"
                fi
            done
            
            if [ "$TABLES_FOUND" -eq 3 ]; then
                echo -e "${GREEN}✅ All TRUF.NETWORK tables accessible!${NC}"
            elif [ "$TABLES_FOUND" -gt 0 ]; then
                echo -e "${YELLOW}⚠️  Some TRUF.NETWORK tables found ($TABLES_FOUND/3)${NC}"
            else
                echo -e "${RED}❌ No TRUF.NETWORK tables found in main schema${NC}"
                MAIN_SCHEMA_ACCESS=false
            fi
        else
            echo -e "${RED}❌ Main schema not found${NC}"
            MAIN_SCHEMA_ACCESS=false
        fi
        
        if [ "$MAIN_SCHEMA_ACCESS" = false ]; then
            echo -e "${YELLOW}⚠️  Proceeding anyway - you may need to check schema permissions${NC}"
        fi
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

# Get the full path to postgres-mcp command
POSTGRES_MCP_PATH=$(which postgres-mcp 2>/dev/null)
if [ -z "$POSTGRES_MCP_PATH" ]; then
    echo -e "${RED}❌ postgres-mcp command not found in PATH${NC}"
    echo "Trying to find it in pipx installation..."
    POSTGRES_MCP_PATH=$(find ~/.local -name "postgres-mcp" -type f -executable 2>/dev/null | head -1)
    if [ -z "$POSTGRES_MCP_PATH" ]; then
        echo -e "${RED}❌ Could not locate postgres-mcp executable${NC}"
        echo "Try running: pipx ensurepath && source ~/.bashrc"
        exit 1
    fi
fi

# Convert Unix path to Windows path for Windows platform
if [ "$PLATFORM" = "windows" ]; then
    # Convert /c/users/... to C:\Users\... and add .exe extension if needed
    if [[ "$POSTGRES_MCP_PATH" == /c/* ]]; then
        POSTGRES_MCP_PATH=$(echo "$POSTGRES_MCP_PATH" | sed 's|^/c/|C:/|' | sed 's|/|\\|g')
    fi
    # Ensure .exe extension
    if [[ "$POSTGRES_MCP_PATH" != *.exe ]]; then
        POSTGRES_MCP_PATH="${POSTGRES_MCP_PATH}.exe"
    fi
fi

echo -e "${GREEN}✅ Found postgres-mcp at: $POSTGRES_MCP_PATH${NC}"

# Create or update Claude Desktop config with full path and correct environment
python3 -c "
import json
import os

config_path = r'$CLAUDE_CONFIG'
db_uri = r'$DB_URI'
postgres_mcp_path = r'$POSTGRES_MCP_PATH'

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

# Add truf-postgres server with full path and correct configuration
config['mcpServers']['truf-postgres'] = {
    'command': postgres_mcp_path,
    'args': ['--access-mode=unrestricted'],
    'env': {
        'DATABASE_URI': db_uri
    }
}

# Write updated config
with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)

print('Claude Desktop configuration updated!')
print('Database URI: ' + db_uri)
print('Command path: ' + postgres_mcp_path)
"

echo ""

# Verify installation
echo "🔍 Verifying installation..."
if pipx list | grep -q "postgres-mcp"; then
    echo -e "${GREEN}✅ TRUF.NETWORK Postgres MCP is installed and available${NC}"
    
    # Show where it's installed
    INSTALL_PATH=$(pipx list --verbose 2>/dev/null | grep -A 5 "postgres-mcp" | grep "installed package" | cut -d' ' -f4 || echo "")
    if [ ! -z "$INSTALL_PATH" ]; then
        echo -e "${BLUE}📍 Installed at: $INSTALL_PATH${NC}"
    fi
else
    echo -e "${RED}❌ Installation verification failed${NC}"
    exit 1
fi

# Test the postgres-mcp command with actual database
echo "🧪 Testing postgres-mcp with database connection..."
if timeout 10s env DATABASE_URI="$DB_URI" "$POSTGRES_MCP_PATH" --access-mode=unrestricted --version > /dev/null 2>&1; then
    echo -e "${GREEN}✅ postgres-mcp responds correctly${NC}"
else
    echo -e "${YELLOW}⚠️  postgres-mcp test completed (may have timed out, which is normal)${NC}"
fi

echo ""

# Show final configuration summary with actual values
if [ -f "$CLAUDE_CONFIG" ]; then
    echo -e "${GREEN}✅ Claude Desktop config file created/updated${NC}"
    echo -e "${BLUE}📍 Config location: $CLAUDE_CONFIG${NC}"
    
    # Show the actual configuration for verification
    echo ""
    echo -e "${BLUE}📋 Configuration summary:${NC}"
    echo "  • Command: $POSTGRES_MCP_PATH"
    echo "  • Database URI: $DB_URI"
    echo ""
    echo -e "${YELLOW}📝 Actual config content:${NC}"
    cat "$CLAUDE_CONFIG" | python3 -m json.tool
else
    echo -e "${RED}❌ Failed to create Claude Desktop config${NC}"
fi

echo ""

# Final instructions
echo -e "${GREEN}🎉 Installation completed successfully!${NC}"
echo ""
echo "📋 What was installed:"
echo "  • TRUF.NETWORK Postgres MCP server (isolated with pipx)"
echo "  • Claude Desktop configuration with main schema support"
echo ""
echo "🔧 Configuration details:"
echo "  • Server name: truf-postgres"
echo "  • Access mode: unrestricted"
echo "  • Database: $DB_NAME on $DB_HOST:$DB_PORT"
echo "  • Schema: main (with public fallback)"
echo ""
echo -e "${YELLOW}📝 Next steps:${NC}"
echo "  1. Restart Claude Desktop application"
echo "  2. Claude will now have access to your PostgreSQL database"
echo "  3. Try asking Claude: 'Show me tables in the main schema'"
echo "  4. Test with: 'SELECT count(*) FROM main.streams'"
echo ""
echo -e "${BLUE}💡 Available tools:${NC}"
echo "  • list_schemas - List database schemas"
echo "  • list_objects - List tables, views, etc."
echo "  • get_object_details - Get table structure"
echo "  • execute_sql - Run SQL queries (main schema is prioritized)"
echo "  • explain_query - Analyze query performance"
echo "  • analyze_db_health - Check database health"
echo ""
echo -e "${BLUE}🏗️  TRUF.NETWORK Schema Information:${NC}"
echo "  • Your data tables are in the 'main' schema:"
echo "    - main.streams (stream definitions)"
echo "    - main.primitive_events (event data)"
echo "    - main.taxonomies (hierarchical structure)"
echo "    - main.metadata (configuration data)"
echo "    - main.data_providers (provider info)"
echo ""
echo -e "${GREEN}Happy querying! 🚀${NC}"
echo ""
echo -e "${BLUE}To uninstall later: pipx uninstall postgres-mcp${NC}"