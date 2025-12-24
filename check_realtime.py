"""
Check Supabase Realtime Configuration
Run this to verify realtime is properly set up for the jobs table
"""
# -*- coding: utf-8 -*-
import sys
import io

# Force UTF-8 encoding for output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from supabase_client import supabase

print("=" * 70)
print("CHECKING SUPABASE REALTIME CONFIGURATION")
print("=" * 70)

# Check 1: Verify replica identity
print("\n1. Checking replica identity...")
try:
    result = supabase.rpc('check_replica_identity', {}).execute()
    print(f"   Result: {result.data}")
except Exception as e:
    print(f"   [WARNING] RPC not available, running direct query...")
    try:
        result = supabase.table('pg_class').select('relreplident').eq('relname', 'jobs').execute()
        if result.data:
            replica_identity = result.data[0].get('relreplident')
            if replica_identity == 'f':
                print(f"   ✅ Replica identity is FULL")
            else:
                print(f"   ❌ Replica identity is {replica_identity} (should be 'f')")
                print(f"   Run: ALTER TABLE jobs REPLICA IDENTITY FULL;")
    except Exception as e2:
        print(f"   ❌ Error: {e2}")

# Check 2: Verify publication
print("\n2. Checking publication...")
try:
    # Try to check if jobs table is in supabase_realtime publication
    query = """
    SELECT schemaname, tablename
    FROM pg_publication_tables
    WHERE pubname = 'supabase_realtime' 
      AND tablename = 'jobs';
    """
    print(f"   [WARNING] Cannot query pg_publication_tables directly via Supabase client")
    print(f"   Please run this in Supabase SQL Editor:")
    print(f"   {query}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Check 3: Test a job update
print("\n3. Testing job update...")
try:
    # Get a recent job
    result = supabase.table('jobs').select('job_id, status').order('created_at', desc=True).limit(1).execute()
    if result.data:
        job = result.data[0]
        job_id = job['job_id']
        current_status = job['status']
        print(f"   Found job: {job_id} (status: {current_status})")
        
        # Try updating progress
        update_result = supabase.table('jobs').update({
            'progress': 50
        }).eq('job_id', job_id).execute()
        
        if update_result.data:
            print(f"   ✅ Successfully updated job progress")
            print(f"   If realtime is working, frontend should receive this update")
        else:
            print(f"   ❌ Failed to update job")
    else:
        print(f"   ⚠️  No jobs found in database")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "=" * 70)
print("MANUAL VERIFICATION STEPS:")
print("=" * 70)
print("""
1. Go to Supabase Dashboard → Database → Replication
2. Verify "jobs" table has replication enabled
3. Run this SQL in SQL Editor:

   -- Check replica identity
   SELECT relname, relreplident 
   FROM pg_class 
   WHERE relname = 'jobs';
   -- Should show: jobs | f

   -- Check publication
   SELECT schemaname, tablename
   FROM pg_publication_tables
   WHERE pubname = 'supabase_realtime' 
     AND tablename = 'jobs';
   -- Should return 1 row with jobs table

4. Check browser console when job completes for:
   - "📡 Realtime subscription status: SUBSCRIBED"
   - "🔔 Realtime event received:"
""")
print("=" * 70)
