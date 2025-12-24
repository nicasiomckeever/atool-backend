"""
Test if Realtime receives UPDATE events
"""

import os
import asyncio
import sys
from dotenv import load_dotenv
from supabase import create_client

# Fix Windows console encoding
if sys.platform == "win32":
    try:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())
    except:
        pass

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

print("=" * 70)
print("🧪 TESTING REALTIME WITH MANUAL UPDATE")
print("=" * 70)
print()

async def test_update():
    from supabase import acreate_client
    
    # Create async client
    async_client = await acreate_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    
    # Create sync client for the update
    sync_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    
    print("📡 Step 1: Subscribe to Realtime...")
    
    event_received = False
    
    def handle_event(payload):
        nonlocal event_received
        event_received = True
        print(f"\n🎉 EVENT RECEIVED!")
        print(f"Payload: {payload}")
        sys.stdout.flush()
    
    channel = async_client.channel("test-update")
    await channel.on_postgres_changes(
        event="*",
        schema="public",
        table="jobs",
        callback=handle_event
    ).subscribe()
    
    print("✅ Subscribed to Realtime")
    print()
    
    # Wait a moment for subscription to be ready
    await asyncio.sleep(2)
    
    print("📝 Step 2: Get a pending job to update...")
    jobs = sync_client.table("jobs").select("*").eq("status", "pending").limit(1).execute()
    
    if not jobs.data:
        print("❌ No pending jobs found!")
        print("   Please create a job first, then run this test")
        return
    
    job = jobs.data[0]
    job_id = job['job_id']
    print(f"✅ Found job: {job_id}")
    print()
    
    print("🔄 Step 3: Updating job progress...")
    sync_client.table("jobs").update({
        "progress": 50
    }).eq("job_id", job_id).execute()
    
    print("✅ Update sent to database")
    print()
    
    print("⏳ Step 4: Waiting 5 seconds for Realtime event...")
    for i in range(5):
        await asyncio.sleep(1)
        if event_received:
            print("✅✅✅ REALTIME IS WORKING! ✅✅✅")
            break
    
    if not event_received:
        print("❌❌❌ NO EVENT RECEIVED ❌❌❌")
        print()
        print("This means Realtime is NOT broadcasting database changes.")
        print()
        print("Possible causes:")
        print("1. Realtime is not enabled in Supabase Settings → API")
        print("2. The publication exists but events aren't being sent")
        print("3. There's a bug in the Supabase Realtime Python client")
    
    await channel.unsubscribe()

if __name__ == "__main__":
    asyncio.run(test_update())
