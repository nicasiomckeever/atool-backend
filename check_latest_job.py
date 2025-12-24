import os
import sys
from dotenv import load_dotenv
from supabase import create_client

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

# Get the latest completed job with Cloudinary URL
print("Checking latest completed jobs with Cloudinary URLs...")
print("=" * 60)

jobs = supabase.table('jobs')\
    .select('*')\
    .eq('user_id', 'a4c3f07f-b07f-4205-8bac-5287ba228c07')\
    .eq('status', 'completed')\
    .order('created_at', desc=True)\
    .limit(5)\
    .execute()

print(f"Found {len(jobs.data)} completed jobs for this user")
print()

for job in jobs.data:
    has_cloudinary = 'cloudinary.com' in (job.get('image_url') or '')
    emoji = "✅" if has_cloudinary else "❌"
    
    print(f"{emoji} Job: {job['job_id']}")
    print(f"   Prompt: {job['prompt'][:50]}...")
    print(f"   Status: {job['status']}")
    print(f"   Image URL: {job.get('image_url', 'NO URL')[:80]}...")
    print(f"   Has Cloudinary: {has_cloudinary}")
    print(f"   Created: {job['created_at']}")
    print()

# Check if frontend query would find these
print("\n" + "=" * 60)
print("Testing frontend gallery query...")
print("=" * 60)

# Simulate frontend query
gallery_jobs = supabase.table('jobs')\
    .select('*')\
    .eq('user_id', 'a4c3f07f-b07f-4205-8bac-5287ba228c07')\
    .eq('status', 'completed')\
    .order('created_at', desc=True)\
    .limit(20)\
    .execute()

images_only = [j for j in gallery_jobs.data if j.get('image_url')]

print(f"Gallery query found: {len(gallery_jobs.data)} completed jobs")
print(f"With image URLs: {len(images_only)}")

if images_only:
    print("\nGallery SHOULD show these images:")
    for img in images_only[:5]:
        has_cloudinary = 'cloudinary.com' in img['image_url']
        emoji = "✅" if has_cloudinary else "⚠️"
        print(f"  {emoji} {img['prompt'][:40]}...")
else:
    print("\n❌ No images found for gallery!")
