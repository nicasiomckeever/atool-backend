"""
Test Script for Priority Queue System
Tests the generation count increment and priority queue routing
"""

from supabase_client import supabase
import uuid
from jobs import create_job, get_next_pending_job

def test_priority_queue():
    """
    Test the priority queue system with different generation counts
    """
    print("\n" + "="*60)
    print("🧪 TESTING PRIORITY QUEUE SYSTEM")
    print("="*60)
    
    # Create a test user
    test_email = f"test_priority_{uuid.uuid4().hex[:8]}@example.com"
    test_user_id = str(uuid.uuid4())
    
    print(f"\n📝 Creating test user: {test_email}")
    
    try:
        # Create test user with 100 credits and 0 generation_count
        user_data = {
            "id": test_user_id,
            "email": test_email,
            "credits": 100,
            "generation_count": 0
        }
        
        user_response = supabase.table("users").insert(user_data).execute()
        print(f"✅ Test user created: {test_user_id}")
        
        # Test 1: First 10 generations should go to priority1_queue
        print(f"\n{'='*60}")
        print("TEST 1: First 10 generations → Priority 1 Queue")
        print("="*60)
        
        for i in range(1, 6):  # Create 5 jobs to test priority 1
            result = create_job(
                user_id=test_user_id,
                prompt=f"Test image {i} - Priority 1",
                model="flux-dev",
                aspect_ratio="1:1"
            )
            
            if result["success"]:
                job = result["job"]
                print(f"✅ Job {i}: ID={job['id'][:8]}... Priority={job['priority']} Generation #{job['generation_number']}")
            else:
                print(f"❌ Job {i} failed: {result['error']}")
        
        # Check priority1_queue
        priority1_count = supabase.table("priority1_queue").select("*", count="exact").eq("user_id", test_user_id).execute()
        print(f"\n📊 Priority 1 Queue: {len(priority1_count.data)} jobs")
        
        # Test 2: Jobs 11-50 should go to priority2_queue
        print(f"\n{'='*60}")
        print("TEST 2: Update count to 11 and create jobs → Priority 2 Queue")
        print("="*60)
        
        # Update generation_count to 10
        supabase.table("users").update({"generation_count": 10}).eq("id", test_user_id).execute()
        
        for i in range(11, 14):  # Create 3 jobs to test priority 2
            result = create_job(
                user_id=test_user_id,
                prompt=f"Test image {i} - Priority 2",
                model="flux-dev",
                aspect_ratio="1:1"
            )
            
            if result["success"]:
                job = result["job"]
                print(f"✅ Job {i}: ID={job['id'][:8]}... Priority={job['priority']} Generation #{job['generation_number']}")
            else:
                print(f"❌ Job {i} failed: {result['error']}")
        
        # Check priority2_queue
        priority2_count = supabase.table("priority2_queue").select("*", count="exact").eq("user_id", test_user_id).execute()
        print(f"\n📊 Priority 2 Queue: {len(priority2_count.data)} jobs")
        
        # Test 3: Jobs >50 should go to priority3_queue
        print(f"\n{'='*60}")
        print("TEST 3: Update count to 51 and create jobs → Priority 3 Queue")
        print("="*60)
        
        # Update generation_count to 50
        supabase.table("users").update({"generation_count": 50}).eq("id", test_user_id).execute()
        
        for i in range(51, 54):  # Create 3 jobs to test priority 3
            result = create_job(
                user_id=test_user_id,
                prompt=f"Test image {i} - Priority 3",
                model="flux-dev",
                aspect_ratio="1:1"
            )
            
            if result["success"]:
                job = result["job"]
                print(f"✅ Job {i}: ID={job['id'][:8]}... Priority={job['priority']} Generation #{job['generation_number']}")
            else:
                print(f"❌ Job {i} failed: {result['error']}")
        
        # Check priority3_queue
        priority3_count = supabase.table("priority3_queue").select("*", count="exact").eq("user_id", test_user_id).execute()
        print(f"\n📊 Priority 3 Queue: {len(priority3_count.data)} jobs")
        
        # Test 4: Worker processing order (Priority 1 → 2 → 3)
        print(f"\n{'='*60}")
        print("TEST 4: Worker Queue Processing Order")
        print("="*60)
        
        # Process jobs in order
        for i in range(11):  # Process all 11 jobs (5 from P1, 3 from P2, 3 from P3)
            result = get_next_pending_job()
            
            if result["success"] and result["job"]:
                job = result["job"]
                priority = result.get("priority", "?")
                print(f"🔧 Worker picked: Job {job['job_id'][:8]}... from Priority {priority} queue")
            else:
                print(f"💤 No more jobs in queue")
                break
        
        # Final check
        print(f"\n{'='*60}")
        print("FINAL QUEUE STATUS")
        print("="*60)
        
        # Check unprocessed jobs in each queue
        p1_remaining = supabase.table("priority1_queue").select("*", count="exact").eq("processed", False).execute()
        p2_remaining = supabase.table("priority2_queue").select("*", count="exact").eq("processed", False).execute()
        p3_remaining = supabase.table("priority3_queue").select("*", count="exact").eq("processed", False).execute()
        
        print(f"📊 Priority 1 Queue (unprocessed): {len(p1_remaining.data)}")
        print(f"📊 Priority 2 Queue (unprocessed): {len(p2_remaining.data)}")
        print(f"📊 Priority 3 Queue (unprocessed): {len(p3_remaining.data)}")
        
        # Clean up - delete test user and jobs
        print(f"\n{'='*60}")
        print("CLEANUP")
        print("="*60)
        
        # Delete jobs (cascade will delete queue entries)
        supabase.table("jobs").delete().eq("user_id", test_user_id).execute()
        supabase.table("users").delete().eq("id", test_user_id).execute()
        
        print(f"✅ Test user and jobs deleted")
        
        print(f"\n{'='*60}")
        print("✅ ALL TESTS COMPLETED")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Clean up on error
        try:
            supabase.table("jobs").delete().eq("user_id", test_user_id).execute()
            supabase.table("users").delete().eq("id", test_user_id).execute()
            print(f"✅ Cleanup completed")
        except:
            pass


if __name__ == "__main__":
    test_priority_queue()
