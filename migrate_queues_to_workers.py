"""
Queue Migration Script - Migrate existing queue data to worker projects
Run this script ONCE after deploying edge functions to move existing data
"""

import os
from dotenv import load_dotenv
from supabase_client import supabase
from worker_client import get_worker_client
from datetime import datetime

# Load environment variables
load_dotenv()


def migrate_queue(queue_name: str, worker_client) -> dict:
    """
    Migrate a single queue table from main project to worker projects
    
    Args:
        queue_name: Name of queue table (e.g., 'priority1_queue')
        worker_client: WorkerClient instance for edge function routing
        
    Returns:
        Migration statistics
    """
    print(f"\n{'='*70}")
    print(f"Migrating {queue_name}")
    print('='*70)
    
    # Get all unprocessed jobs from main project
    try:
        response = supabase.table(queue_name).select("*").eq("processed", False).execute()
        jobs = response.data
        
        if not jobs:
            print(f"✅ No unprocessed jobs in {queue_name}")
            return {
                'queue': queue_name,
                'total': 0,
                'migrated': 0,
                'failed': 0,
                'skipped': 0
            }
        
        print(f"📦 Found {len(jobs)} unprocessed jobs to migrate")
        
        migrated_count = 0
        failed_count = 0
        skipped_count = 0
        
        for i, job in enumerate(jobs, 1):
            try:
                # Skip if already marked as migrated
                if job.get('migrated'):
                    print(f"   [{i}/{len(jobs)}] ⏭️  Job {job['queue_id']} already migrated")
                    skipped_count += 1
                    continue
                
                # Prepare job data for worker (exclude queue_id - let worker generate new one)
                job_data = {
                    'user_id': job['user_id'],
                    'job_id': job['job_id'],
                    'request_payload': job['request_payload'],
                    'processed': False,
                    'created_at': job['created_at'],
                    'updated_at': datetime.utcnow().isoformat()
                }
                
                # Insert into worker via edge function
                worker_client.insert(queue_name, job_data)
                
                # Mark as migrated in main project (keep record for audit)
                supabase.table(queue_name).update({
                    'migrated': True,
                    'migrated_at': datetime.utcnow().isoformat()
                }).eq('queue_id', job['queue_id']).execute()
                
                print(f"   [{i}/{len(jobs)}] ✅ Migrated job {job['job_id']}")
                migrated_count += 1
                
            except Exception as e:
                print(f"   [{i}/{len(jobs)}] ❌ Failed to migrate job {job.get('job_id', 'unknown')}: {e}")
                failed_count += 1
        
        print(f"\n📊 {queue_name} Migration Summary:")
        print(f"   Total jobs: {len(jobs)}")
        print(f"   ✅ Migrated: {migrated_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   ⏭️  Skipped: {skipped_count}")
        
        return {
            'queue': queue_name,
            'total': len(jobs),
            'migrated': migrated_count,
            'failed': failed_count,
            'skipped': skipped_count
        }
        
    except Exception as e:
        print(f"❌ Error migrating {queue_name}: {e}")
        return {
            'queue': queue_name,
            'total': 0,
            'migrated': 0,
            'failed': 0,
            'skipped': 0,
            'error': str(e)
        }


def verify_migration(queue_name: str) -> dict:
    """
    Verify migration was successful by comparing counts
    
    Args:
        queue_name: Name of queue table
        
    Returns:
        Verification results
    """
    try:
        # Count unprocessed, non-migrated jobs in main project
        main_response = supabase.table(queue_name).select("queue_id", count="exact").eq("processed", False).eq("migrated", False).execute()
        main_count = main_response.count or 0
        
        # Count jobs in worker (via edge function - select all)
        worker_client = get_worker_client()
        worker_jobs = worker_client.select(queue_name, filters={'eq': {'processed': False}})
        worker_count = len(worker_jobs) if worker_jobs else 0
        
        return {
            'queue': queue_name,
            'main_remaining': main_count,
            'worker_count': worker_count,
            'verified': main_count == 0
        }
        
    except Exception as e:
        print(f"⚠️ Could not verify {queue_name}: {e}")
        return {
            'queue': queue_name,
            'verified': False,
            'error': str(e)
        }


def add_migrated_column():
    """
    Add 'migrated' column to queue tables in main project for tracking
    Run this first before migration
    """
    print("\n🔧 Adding 'migrated' columns to queue tables...")
    
    queues = ['priority1_queue', 'priority2_queue', 'priority3_queue']
    
    for queue in queues:
        try:
            # Try to add column - will fail if already exists (that's OK)
            # Note: Direct SQL execution requires service role or RLS bypass
            # Alternative: manually add column via Supabase dashboard SQL editor
            
            print(f"   ⚠️  Please manually add these columns to {queue} via Supabase SQL Editor:")
            print(f"      ALTER TABLE {queue} ADD COLUMN IF NOT EXISTS migrated BOOLEAN DEFAULT FALSE;")
            print(f"      ALTER TABLE {queue} ADD COLUMN IF NOT EXISTS migrated_at TIMESTAMP;")
            
        except Exception as e:
            print(f"   ℹ️  {queue}: {e}")
    
    print("\n📝 Manual Steps Required:")
    print("   1. Go to your main Supabase project")
    print("   2. Open SQL Editor")
    print("   3. Run the following SQL:")
    print()
    print("   -- Add migration tracking columns")
    for queue in queues:
        print(f"   ALTER TABLE {queue} ADD COLUMN IF NOT EXISTS migrated BOOLEAN DEFAULT FALSE;")
        print(f"   ALTER TABLE {queue} ADD COLUMN IF NOT EXISTS migrated_at TIMESTAMP;")
    print()
    input("Press Enter after you've added the columns...")


def main():
    """
    Main migration function
    """
    print("="*70)
    print("Queue Migration to Worker Projects")
    print("="*70)
    
    # Check if edge function is enabled
    use_edge_function = os.getenv('USE_EDGE_FUNCTION', 'false').lower() == 'true'
    
    if not use_edge_function:
        print("\n⚠️  Warning: USE_EDGE_FUNCTION is not enabled in .env")
        print("Migration requires edge function to be configured.")
        print("\nSet USE_EDGE_FUNCTION=true in backend/.env and try again.")
        return
    
    # Check if edge function URL is set
    edge_function_url = os.getenv('EDGE_FUNCTION_URL')
    if not edge_function_url:
        print("\n❌ Error: EDGE_FUNCTION_URL not set in .env")
        print("Please configure edge function URL and try again.")
        return
    
    print(f"\n✅ Edge function URL: {edge_function_url}")
    
    # Step 1: Add migrated columns (manual step)
    add_migrated_column()
    
    # Step 2: Initialize worker client
    try:
        worker_client = get_worker_client()
        print("✅ Worker client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize worker client: {e}")
        return
    
    # Step 3: Migrate each queue
    queues = ['priority1_queue', 'priority2_queue', 'priority3_queue']
    results = []
    
    for queue in queues:
        result = migrate_queue(queue, worker_client)
        results.append(result)
    
    # Step 4: Verify migration
    print("\n" + "="*70)
    print("Verification")
    print("="*70)
    
    for queue in queues:
        verification = verify_migration(queue)
        print(f"\n{queue}:")
        print(f"   Remaining in main: {verification.get('main_remaining', 'unknown')}")
        print(f"   Count in worker: {verification.get('worker_count', 'unknown')}")
        print(f"   Status: {'✅ Verified' if verification.get('verified') else '⚠️ Check manually'}")
    
    # Step 5: Summary
    print("\n" + "="*70)
    print("Migration Summary")
    print("="*70)
    
    total_migrated = sum(r.get('migrated', 0) for r in results)
    total_failed = sum(r.get('failed', 0) for r in results)
    total_skipped = sum(r.get('skipped', 0) for r in results)
    
    print(f"\n📊 Overall Statistics:")
    print(f"   ✅ Total migrated: {total_migrated}")
    print(f"   ❌ Total failed: {total_failed}")
    print(f"   ⏭️  Total skipped: {total_skipped}")
    
    if total_failed > 0:
        print("\n⚠️  Some jobs failed to migrate. Review logs above for details.")
    else:
        print("\n🎉 Migration completed successfully!")
    
    print("\n📝 Next Steps:")
    print("   1. Verify worker projects have the migrated data")
    print("   2. Test creating new jobs with edge function")
    print("   3. Monitor edge function logs: supabase functions logs route-queue")
    print("   4. Optional: Clean up old queue data in main project after verification")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
