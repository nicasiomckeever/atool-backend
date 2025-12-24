"""
Test script to verify gallery is working correctly
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print("=" * 60)
print("GALLERY TEST - Checking Jobs Table")
print("=" * 60)

# 1. Check all jobs
print("\n1️⃣ Fetching all jobs...")
try:
    all_jobs = supabase.table("jobs").select("*").execute()
    print(f"✅ Total jobs in database: {len(all_jobs.data)}")
    
    if all_jobs.data:
        print("\n📊 Job breakdown:")
        statuses = {}
        with_images = 0
        without_images = 0
        
        for job in all_jobs.data:
            status = job.get('status')
            statuses[status] = statuses.get(status, 0) + 1
            
            if job.get('image_url'):
                with_images += 1
            else:
                without_images += 1
        
        for status, count in statuses.items():
            print(f"   - {status}: {count}")
        
        print(f"\n🖼️  Jobs with image_url: {with_images}")
        print(f"❌ Jobs without image_url: {without_images}")
        
        # Show recent completed jobs
        print("\n📸 Recent completed jobs with images:")
        completed_with_images = [j for j in all_jobs.data if j.get('status') == 'completed' and j.get('image_url')]
        
        if completed_with_images:
            for job in completed_with_images[:5]:  # Show first 5
                print(f"\n   Job ID: {job['job_id']}")
                print(f"   User ID: {job['user_id']}")
                print(f"   Prompt: {job['prompt'][:50]}...")
                print(f"   Image URL: {job['image_url'][:80]}...")
                print(f"   Created: {job['created_at']}")
        else:
            print("   ❌ No completed jobs with images found!")
    else:
        print("❌ No jobs found in database!")
        
except Exception as e:
    print(f"❌ Error: {e}")

# 2. Check users
print("\n\n2️⃣ Fetching users...")
try:
    users = supabase.table("users").select("*").execute()
    print(f"✅ Total users: {len(users.data)}")
    
    if users.data:
        for user in users.data:
            print(f"\n   📧 {user['email']}")
            print(f"      ID: {user['id']}")
            print(f"      Credits: {user.get('credits', 0)}")
            
            # Count jobs for this user
            user_jobs = supabase.table("jobs").select("*").eq("user_id", user['id']).execute()
            completed_jobs = [j for j in user_jobs.data if j.get('status') == 'completed' and j.get('image_url')]
            
            print(f"      Total jobs: {len(user_jobs.data)}")
            print(f"      Completed with images: {len(completed_jobs)}")
except Exception as e:
    print(f"❌ Error: {e}")

# 3. Test gallery query (simulating frontend)
print("\n\n3️⃣ Testing gallery query (frontend simulation)...")
try:
    # Get first user
    users = supabase.table("users").select("*").limit(1).execute()
    if users.data:
        test_user_id = users.data[0]['id']
        print(f"Testing with user: {users.data[0]['email']}")
        
        # Query like the frontend does
        gallery_query = supabase.table("jobs")\
            .select("*")\
            .eq("user_id", test_user_id)\
            .eq("status", "completed")\
            .order("created_at", desc=True)\
            .limit(20)\
            .execute()
        
        images = [j for j in gallery_query.data if j.get('image_url')]
        
        print(f"✅ Gallery query returned: {len(gallery_query.data)} completed jobs")
        print(f"🖼️  Jobs with images: {len(images)}")
        
        if images:
            print("\n✅ Gallery should show these images:")
            for img in images[:3]:
                print(f"   - {img['prompt'][:50]}...")
        else:
            print("\n❌ No images found! Gallery will show empty state.")
    else:
        print("❌ No users in database!")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 60)
print("✅ Test Complete!")
print("=" * 60)
