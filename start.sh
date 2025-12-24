#!/bin/bash
# Start script for Atool backend
# Starts Nginx reverse proxy first, then Flask + Job Worker

set -e  # Exit on any error

echo "========================================================================"
echo "🚀 ATOOL BACKEND STARTUP SCRIPT"
echo "========================================================================"

# Check if running in Docker/Wispbyte or local
if [ -f /.dockerenv ]; then
    echo "📦 Running in Docker environment"
    WORKING_DIR="/home/container/new2"
else
    echo "💻 Running locally"
    WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

echo "📂 Working directory: $WORKING_DIR"
echo ""

# ========================================================================
# Step 1: Start Nginx (Reverse Proxy for HTTPS)
# ========================================================================
echo "========================================================================"
echo "🔒 Step 1: Starting Nginx reverse proxy (HTTPS on port 8080)..."
echo "========================================================================"

# Copy Nginx config to system location if in Docker
if [ -f /.dockerenv ]; then
    if [ -f "$WORKING_DIR/../nginx.conf" ]; then
        cp "$WORKING_DIR/../nginx.conf" /etc/nginx/sites-available/default
        echo "✅ Nginx config copied"
    fi
    
    # Start Nginx in background
    nginx -g 'daemon off;' &
    NGINX_PID=$!
    echo "✅ Nginx started (PID: $NGINX_PID)"
else
    # Local development - try to start Nginx if available
    if command -v nginx &> /dev/null; then
        nginx -c "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/nginx.conf" &
        NGINX_PID=$!
        echo "✅ Nginx started locally (PID: $NGINX_PID)"
    else
        echo "⚠️  Nginx not found locally - skipping (OK for development)"
    fi
fi

sleep 2
echo ""

# ========================================================================
# Step 2: Start Flask Backend API
# ========================================================================
echo "========================================================================"
echo "🌐 Step 2: Starting Flask backend (HTTP on port 5000)..."
echo "========================================================================"

cd "$WORKING_DIR"

# Install dependencies if needed
if [ -f "requirements.txt" ]; then
    echo "📦 Installing dependencies..."
    pip install -q -r requirements.txt
fi

# Start Flask in background
python app.py &
FLASK_PID=$!
echo "✅ Flask started (PID: $FLASK_PID)"
sleep 3
echo ""

# ========================================================================
# Step 3: Wait for processes
# ========================================================================
echo "========================================================================"
echo "✅ ALL SERVICES STARTED"
echo "========================================================================"
echo "🔒 Nginx reverse proxy:  https://atoolwispbyte.duckdns.org:8080"
echo "🌐 Flask backend:        http://localhost:5000"
echo ""
echo "📊 Monitor logs:"
echo "   - Nginx: /var/log/nginx/atool_*.log"
echo "   - Flask: stdout above"
echo ""
echo "Press Ctrl+C to stop all services"
echo "========================================================================"
echo ""

# Keep processes running
wait
