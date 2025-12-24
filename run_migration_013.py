"""
Quick Script to Run Migration 013: Add video_url Column
Run this to add the video_url column to your Supabase database
"""

import os
from dotenv import load_dotenv
from supabase_client import supabase

load_dotenv()

# SQL Migration
MIGRATION_SQL = """
-- Add video_url column to jobs table
ALTER TABLE jobs 
ADD COLUMN IF NOT EXISTS video_url TEXT;

-- Create index for faster video job queries
CREATE INDEX IF NOT EXISTS idx_jobs_video_url ON jobs(video_url) WHERE video_url IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN jobs.video_url IS 'Cloudinary URL for generated video (used for video generation jobs)';
"""

def run_migration():
    """Run migration 013 to add video_url column"""
    print("\n" + "="*60)
    print("🔧 RUNNING MIGRATION 013: Add video_url Column")
    print("="*60)
    print()
    
    try:
        # Execute the migration SQL
        print("📝 Executing SQL migration...")
        print()
        print(MIGRATION_SQL)
        print()
        
        # Supabase Python client doesn't support raw SQL execution
        # You need to run this via RPC or directly in Supabase SQL Editor
        print("⚠️  IMPORTANT: Supabase Python client doesn't support ALTER TABLE")
        print()
        print("📋 Please run this migration manually:")
        print()
        print("OPTION 1: Supabase Dashboard (Recommended)")
        print("  1. Go to: https://supabase.com/dashboard")
        print("  2. Select your project")
        print("  3. Click 'SQL Editor' in left sidebar")
        print("  4. Paste the SQL above")
        print("  5. Click 'Run' button")
        print()
        print("OPTION 2: Quick SQL Command")
        print("  Run this in Supabase SQL Editor:")
        print()
        print("  ALTER TABLE jobs ADD COLUMN IF NOT EXISTS video_url TEXT;")
        print("  CREATE INDEX IF NOT EXISTS idx_jobs_video_url ON jobs(video_url) WHERE video_url IS NOT NULL;")
        print()
        print("="*60)
        print()
        
        # Try to verify if column exists (read-only check)
        print("🔍 Checking if migration is needed...")
        try:
            # Try to query video_url column
            test_query = supabase.table("jobs").select("video_url").limit(1).execute()
            print("✅ Migration already applied! video_url column exists.")
            print()
            return True
        except Exception as e:
            error_msg = str(e)
            if "video_url" in error_msg and ("not found" in error_msg.lower() or "does not exist" in error_msg.lower()):
                print("❌ Migration NOT applied yet - video_url column doesn't exist")
                print()
                print("👉 Please run the SQL migration manually using one of the options above")
                print()
                return False
            else:
                print(f"⚠️  Unexpected error: {error_msg}")
                print()
                return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        return False

if __name__ == "__main__":
    success = run_migration()
    
    if success:
        print("="*60)
        print("✅ MIGRATION STATUS: Applied")
        print("="*60)
        print()
        print("Next steps:")
        print("  1. Test video generation from frontend")
        print("  2. Check backend logs for '📹 Saving video URL'")
        print("  3. Verify videos appear in gallery")
        print()
    else:
        print("="*60)
        print("⚠️  MIGRATION STATUS: Pending")
        print("="*60)
        print()
        print("Action required:")
        print("  Run the SQL migration in Supabase Dashboard")
        print("  See: MIGRATION_013_INSTRUCTIONS.md for details")
        print()
