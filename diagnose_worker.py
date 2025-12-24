"""
Diagnostic script to check worker setup and identify issues
"""

import os
import requests
from dotenv import load_dotenv
from supabase_client import supabase

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:5000")

print("=" * 70)
print("🔍 WORKER DIAGNOSTICS")
print("=" * 70)
print()

# Test 1: Check if backend is running
print("Test 1: Backend connectivity")
print("-" * 70)
try:
    response = requests.get(f"{BACKEND_URL}/health", timeout=5)
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Backend is running: {BACKEND_URL}")
        print(f"   Status: {data.get('status')}")
        print(f"   Has cached URL: {data.get('has_url')}")
        print(f"   Cached URL: {data.get('cached_url')}")
    else:
        print(f"❌ Backend returned status {response.status_code}")
except Exception as e:
    print(f"❌ Cannot connect to backend: {e}")
    print(f"   Make sure backend is running: python app.py")
print()

# Test 2: Check Modal deployments
print("Test 2: Modal deployments in database")
print("-" * 70)
try:
    response = supabase.table("modal_deployments").select("*").execute()
    deployments = response.data or []
    
    if not deployments:
        print("❌ NO Modal deployments found in database!")
        print()
        print("💡 SOLUTION:")
        print("   1. Deploy your Modal app:")
        print("      cd ../modelrunv1")
        print("      modal deploy modal_app.py")
        print()
        print("   2. Add the deployment URL to database:")
        print("      python notify_discord.py")
        print()
    else:
        print(f"✅ Found {len(deployments)} deployment(s)")
        print()
        
        active = [d for d in deployments if d.get("is_active")]
        inactive = [d for d in deployments if not d.get("is_active")]
        
        print(f"   Active: {len(active)}")
        print(f"   Inactive: {len(inactive)}")
        print()
        
        if active:
            print("   Active deployments:")
            for dep in active:
                print(f"      #{dep['deployment_number']}")
                print(f"         Image URL: {dep['image_url']}")
                print(f"         Video URL: {dep['video_url']}")
                print(f"         Created: {dep.get('created_at')}")
                print()
        else:
            print("   ⚠️  NO ACTIVE DEPLOYMENTS!")
            print()
            if inactive:
                print("   💡 You have inactive deployments. To activate one:")
                print("      python reactivate_deployment.py")
                print()
            else:
                print("   💡 Deploy Modal and add URL:")
                print("      cd ../modelrunv1")
                print("      modal deploy modal_app.py")
                print("      python notify_discord.py")
                print()
        
except Exception as e:
    print(f"❌ Error checking deployments: {e}")
print()

# Test 3: Check pending jobs
print("Test 3: Pending jobs")
print("-" * 70)
try:
    response = supabase.table("jobs").select("*").eq("status", "pending").execute()
    pending_jobs = response.data or []
    
    if not pending_jobs:
        print("✅ No pending jobs (queue is empty)")
    else:
        print(f"⚠️  Found {len(pending_jobs)} pending job(s)")
        print()
        print("   Recent pending jobs:")
        for job in pending_jobs[:5]:
            print(f"      Job ID: {job['job_id']}")
            print(f"         Type: {job.get('job_type', 'image')}")
            print(f"         Model: {job.get('model')}")
            print(f"         Prompt: {job.get('prompt', '')[:50]}...")
            print(f"         Created: {job.get('created_at')}")
            print()
except Exception as e:
    print(f"❌ Error checking jobs: {e}")
print()

# Test 4: Try to fetch Modal URL
print("Test 4: Fetch Modal URL from backend")
print("-" * 70)
try:
    response = requests.get(f"{BACKEND_URL}/get-url", timeout=10)
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            print(f"✅ Got Modal URL: {data.get('url')}")
            print(f"   Source: {data.get('source')}")
            print(f"   Cached: {data.get('cached')}")
        else:
            print(f"❌ Failed to get URL: {data.get('error')}")
            print()
            print("💡 This is why the worker cannot process jobs!")
            print("   The worker needs a valid Modal URL to send generation requests.")
    else:
        print(f"❌ Backend returned status {response.status_code}")
except Exception as e:
    print(f"❌ Error fetching URL: {e}")
print()

# Summary
print("=" * 70)
print("📋 DIAGNOSIS SUMMARY")
print("=" * 70)
print()
print("For the worker to process jobs, you need:")
print("   1. ✓ Backend running (app.py)")
print("   2. ? Active Modal deployment in database")
print("   3. ? Valid Modal URL accessible from /get-url")
print()
print("If you see ❌ above, follow the solutions to fix the issues.")
print()
