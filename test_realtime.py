"""
Test Realtime subscription to diagnose connection issues
"""

import os
import asyncio
import sys
from dotenv import load_dotenv

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
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

print("=" * 70)
print("🔍 REALTIME DIAGNOSTIC TEST")
print("=" * 70)
print(f"Supabase URL: {SUPABASE_URL}")
print(f"Service Key: {SUPABASE_SERVICE_KEY[:20]}..." if SUPABASE_SERVICE_KEY else "None")
print(f"Anon Key: {SUPABASE_ANON_KEY[:20]}..." if SUPABASE_ANON_KEY else "None")
print("=" * 70)
print()

async def test_with_key(key_name, api_key):
    """Test Realtime subscription with a specific API key"""
    from supabase import acreate_client
    
    print(f"\n{'='*70}")
    print(f"🧪 TESTING WITH {key_name}")
    print(f"{'='*70}\n")
    
    try:
        client = await acreate_client(SUPABASE_URL, api_key)
        
        # Test 1: Can we read jobs table?
        print("📊 Test 1: Reading jobs table...")
        try:
            response = await client.table("jobs").select("*").limit(1).execute()
            print(f"✅ Can read jobs table ({len(response.data)} records)")
        except Exception as e:
            print(f"❌ Cannot read jobs table: {e}")
        
        # Test 2: Subscribe to Realtime
        print("\n📡 Test 2: Subscribing to Realtime...")
        
        event_received = False
        
        def handle_event(payload):
            nonlocal event_received
            event_received = True
            print(f"\n🎉 EVENT RECEIVED!")
            print(f"Full payload: {payload}")
            print(f"Event Type: {payload.get('eventType')}")
            print(f"Table: {payload.get('table')}")
            print(f"Schema: {payload.get('schema')}")
            
            # Try multiple ways to extract the record
            new_record = payload.get('new') or payload.get('record') or payload
            print(f"New record: {new_record}")
            
            if isinstance(new_record, dict):
                print(f"Job ID: {new_record.get('job_id')}")
                print(f"Status: {new_record.get('status')}")
                print(f"Prompt: {new_record.get('prompt', '')[:50]}...")
            
            sys.stdout.flush()
        
        channel = client.channel(f"test-{key_name}")
        
        subscription = await channel.on_postgres_changes(
            event="*",
            schema="public",
            table="jobs",
            callback=handle_event
        ).subscribe()
        
        print(f"✅ Subscription created: {subscription}")
        print(f"\n⏳ Waiting 10 seconds for events...")
        print(f"   Create a new job in the frontend now!\n")
        sys.stdout.flush()
        
        # Wait for events
        for i in range(10):
            await asyncio.sleep(1)
            if event_received:
                print(f"✅ Event was received with {key_name}!")
                break
        
        if not event_received:
            print(f"❌ No events received with {key_name} after 10 seconds")
        
        # Cleanup
        await channel.unsubscribe()
        
    except Exception as e:
        print(f"❌ Error testing with {key_name}: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Run all diagnostic tests"""
    
    # Test with service role key
    if SUPABASE_SERVICE_KEY:
        await test_with_key("SERVICE_ROLE_KEY", SUPABASE_SERVICE_KEY)
    else:
        print("⚠️  SERVICE_ROLE_KEY not found in .env")
    
    # Test with anon key
    if SUPABASE_ANON_KEY:
        await test_with_key("ANON_KEY", SUPABASE_ANON_KEY)
    else:
        print("⚠️  ANON_KEY not found in .env")
    
    print("\n" + "=" * 70)
    print("🏁 DIAGNOSTIC TEST COMPLETE")
    print("=" * 70)
    print()
    print("📋 NEXT STEPS:")
    print()
    print("If NO events received with either key:")
    print("  1. Check Supabase Dashboard → Database → Replication")
    print("     - Ensure 'jobs' table has Realtime enabled")
    print("     - Check if 'Insert', 'Update', 'Delete' are all enabled")
    print()
    print("  2. Check Row Level Security (RLS) policies:")
    print("     - RLS policies can block Realtime events")
    print("     - Try temporarily DISABLING RLS on 'jobs' table")
    print("     - Or add RLS policy: allow SELECT for service_role")
    print()
    print("  3. Check Supabase Realtime settings:")
    print("     - Settings → API → Realtime")
    print("     - Ensure Realtime is enabled for your project")
    print()

if __name__ == "__main__":
    print("Starting diagnostic test...")
    print("This will subscribe to Realtime and wait for events.")
    print()
    
    asyncio.run(main())
