"""
Run Migration 015: Add job_type column to jobs table

This migration adds the job_type column to distinguish between image and video jobs.
This enables proper job recovery after page refresh.

Usage:
    python run_migration_015.py
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

def run_migration():
    """Run the migration to add job_type column"""
    
    print("\n" + "="*60)
    print("🔄 RUNNING MIGRATION 015: Add job_type Column")
    print("="*60)
    
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        return False
    
    # Create Supabase client
    supabase = create_client(supabase_url, supabase_key)
    
    # Read migration SQL
    migration_path = Path(__file__).parent / "migrations" / "015_add_job_type_column.sql"
    
    if not migration_path.exists():
        print(f"❌ Migration file not found: {migration_path}")
        return False
    
    print(f"📄 Reading migration file: {migration_path.name}")
    
    with open(migration_path, 'r') as f:
        sql = f.read()
    
    print("\n📋 Migration SQL:")
    print("-" * 60)
    print(sql[:500] + "..." if len(sql) > 500 else sql)
    print("-" * 60)
    
    try:
        print("\n🚀 Executing migration...")
        
        # Execute the migration
        result = supabase.rpc('exec_sql', {'sql': sql}).execute()
        
        print("✅ Migration executed successfully!")
        print("\n📊 Verification:")
        
        # Verify the column was added
        verify_query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'jobs' AND column_name = 'job_type';
        """
        
        verify_result = supabase.rpc('exec_sql', {'sql': verify_query}).execute()
        
        if verify_result.data:
            print("✅ job_type column successfully added to jobs table")
            print(f"   Details: {verify_result.data}")
        else:
            print("⚠️  Could not verify column (might need to check manually)")
        
        # Check if existing jobs were updated
        count_query = """
        SELECT COUNT(*) as total, job_type
        FROM jobs
        GROUP BY job_type;
        """
        
        try:
            count_result = supabase.rpc('exec_sql', {'sql': count_query}).execute()
            if count_result.data:
                print("\n📈 Job type distribution:")
                for row in count_result.data:
                    print(f"   {row.get('job_type', 'unknown')}: {row.get('total', 0)} jobs")
        except:
            # Fallback to direct query if RPC doesn't work
            try:
                jobs = supabase.table('jobs').select('job_type').execute()
                if jobs.data:
                    image_count = sum(1 for j in jobs.data if j.get('job_type') == 'image')
                    video_count = sum(1 for j in jobs.data if j.get('job_type') == 'video')
                    print("\n📈 Job type distribution:")
                    print(f"   image: {image_count} jobs")
                    print(f"   video: {video_count} jobs")
            except Exception as e:
                print(f"   ⚠️  Could not fetch job counts: {e}")
        
        print("\n" + "="*60)
        print("✅ MIGRATION 015 COMPLETED SUCCESSFULLY")
        print("="*60)
        print("\n💡 Next steps:")
        print("   1. Test job creation with job_type='image'")
        print("   2. Test job creation with job_type='video'")
        print("   3. Test page refresh recovery on Image Generation page")
        print("   4. Test page refresh recovery on Video Generation page")
        print()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("\n💡 Alternative: Run the SQL manually in Supabase SQL Editor:")
        print(f"   1. Go to: {supabase_url}/project/_/sql")
        print(f"   2. Paste the contents of: {migration_path}")
        print("   3. Click 'Run'")
        print()
        return False


if __name__ == "__main__":
    success = run_migration()
    exit(0 if success else 1)
