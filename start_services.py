#!/usr/bin/env python3
"""
Atool Backend Startup Wrapper
Starts Nginx (if available), then Flask app + Job Worker
Works in Docker containers and locally
"""

import subprocess
import sys
import os
import time
import signal
import shutil

NGINX_PID = None
FLASK_PID = None

def cleanup(signum=None, frame=None):
    """Cleanup: stop all processes on shutdown"""
    global NGINX_PID, FLASK_PID
    
    print("\n" + "="*70)
    print("🛑 SHUTTING DOWN...")
    print("="*70)
    
    if FLASK_PID:
        try:
            os.kill(FLASK_PID, signal.SIGTERM)
            print(f"✅ Stopped Flask (PID: {FLASK_PID})")
        except:
            pass
    
    if NGINX_PID:
        try:
            os.kill(NGINX_PID, signal.SIGTERM)
            print(f"✅ Stopped Nginx (PID: {NGINX_PID})")
        except:
            # Try killall as fallback
            try:
                subprocess.run(["killall", "nginx"], stderr=subprocess.DEVNULL)
            except:
                pass
    
    print("="*70)
    sys.exit(0)

def nginx_available():
    """Check if nginx is installed"""
    return shutil.which("nginx") is not None

def start_nginx():
    """Start Nginx reverse proxy"""
    global NGINX_PID
    
    print("\n" + "="*70)
    print("🔒 Starting Nginx reverse proxy (HTTPS on port 8080)...")
    print("="*70)
    
    # Check if Nginx is available
    if not nginx_available():
        print("⚠️  Nginx not found in container")
        print("   To enable HTTPS, install Nginx:")
        print("   apt-get update && apt-get install -y nginx")
        print("   Skipping Nginx - Flask will run on HTTP only")
        return False
    
    try:
        # For Docker containers, nginx config should be in parent directory
        nginx_config = None
        possible_paths = [
            "/home/container/nginx.conf",
            "../nginx.conf",
            "/app/nginx.conf",
            "./nginx.conf"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                nginx_config = os.path.abspath(path)
                print(f"📍 Found Nginx config: {nginx_config}")
                break
        
        if not nginx_config:
            print("⚠️  nginx.conf not found, skipping Nginx")
            return False
        
        # Start Nginx
        try:
            NGINX_PID = subprocess.Popen(
                ["nginx", "-c", nginx_config],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            ).pid
            
            print(f"✅ Nginx started (PID: {NGINX_PID})")
            print("   🔗 HTTPS: https://atoolwispbyte.duckdns.org:8080")
            time.sleep(1)
            return True
        except Exception as e:
            print(f"❌ Failed to start Nginx: {e}")
            return False
    
    except Exception as e:
        print(f"⚠️  Error starting Nginx: {e}")
        return False

def start_flask():
    """Start Flask backend API"""
    global FLASK_PID
    
    print("\n" + "="*70)
    print("🌐 Starting Flask Backend API (HTTP on port 5000)...")
    print("="*70)
    
    working_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Install/update dependencies
    req_file = os.path.join(working_dir, "requirements.txt")
    if os.path.exists(req_file):
        print("📦 Installing dependencies...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "-r", req_file],
                timeout=300
            )
            print("✅ Dependencies installed")
        except Exception as e:
            print(f"⚠️  Error installing dependencies: {e}")
    
    # Start Flask
    print("▶️  Starting Flask...")
    try:
        FLASK_PID = subprocess.Popen(
            [sys.executable, "app.py"],
            cwd=working_dir,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True
        ).pid
        
        print(f"✅ Flask started (PID: {FLASK_PID})")
        print("   🌐 HTTP: http://localhost:5000")
        return True
    except Exception as e:
        print(f"❌ Failed to start Flask: {e}")
        return False

def main():
    """Main startup function"""
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    print("\n" + "="*70)
    print("🚀 ATOOL BACKEND STARTUP")
    print("="*70)
    
    in_docker = os.path.exists("/.dockerenv")
    print(f"{'📦 Docker Container' if in_docker else '💻 Local Environment'}")
    print("="*70)
    
    nginx_ok = False
    flask_ok = False
    
    # ====================================================================
    # Step 1: Try to start Nginx (optional)
    # ====================================================================
    if os.environ.get("USE_NGINX") == "1" or in_docker:
        nginx_ok = start_nginx()
    else:
        print("\n⚠️  Nginx disabled (set USE_NGINX=1 to enable)")
    
    # ====================================================================
    # Step 2: Start Flask
    # ====================================================================
    flask_ok = start_flask()
    
    # ====================================================================
    # Step 3: Status Summary
    # ====================================================================
    print("\n" + "="*70)
    if flask_ok:
        print("✅ BACKEND READY")
    else:
        print("❌ BACKEND FAILED TO START")
    print("="*70)
    
    if nginx_ok:
        print("🔒 Nginx:  RUNNING (HTTPS)")
    elif in_docker:
        print("⚠️  Nginx:  NOT RUNNING (install with: apt-get install nginx)")
    else:
        print("ℹ️  Nginx:  DISABLED")
    
    if flask_ok:
        print("🌐 Flask:  RUNNING (HTTP)")
    else:
        print("❌ Flask:  FAILED")
    
    print("="*70)
    print("Press Ctrl+C to stop all services")
    print("="*70 + "\n")
    
    if not flask_ok:
        sys.exit(1)
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()

if __name__ == "__main__":
    main()

