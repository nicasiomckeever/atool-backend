"""
Quick script to check modal_deployments table status
"""

from supabase_client import supabase
from datetime import datetime

print("\n" + "="*60)
print("CHECKING MODAL DEPLOYMENTS TABLE")
print("="*60 + "\n")

try:
    # Get all deployments
    response = supabase.table("modal_deployments").select("*").order("deployment_number", desc=False).execute()
    
    if not response.data:
        print("ERROR: No deployments found in table!")
    else:
        print(f"Found {len(response.data)} deployments:\n")
        
        for deployment in response.data:
            status = "ACTIVE" if deployment.get("is_active") else "INACTIVE"
            
            print(f"Deployment #{deployment.get('deployment_number')} - {status}")
            print(f"   ID: {deployment.get('id')}")
            print(f"   Image URL: {deployment.get('image_url')}")
            print(f"   Video URL: {deployment.get('video_url')}")
            print(f"   Created: {deployment.get('created_at')}")
            print(f"   Last Used: {deployment.get('last_used_at')}")
            print(f"   Updated: {deployment.get('updated_at')}")
            print()
        
        # Show which one would be fetched by get_active_deployment
        active_deployments = [d for d in response.data if d.get("is_active")]
        
        if active_deployments:
            # Sort by created_at (oldest first)
            active_sorted = sorted(active_deployments, key=lambda x: x.get("created_at", ""))
            oldest = active_sorted[0]
            
            print(f"NEXT ACTIVE DEPLOYMENT (oldest first):")
            print(f"   Deployment #{oldest.get('deployment_number')}")
            print(f"   Image URL: {oldest.get('image_url')}")
            print(f"   Video URL: {oldest.get('video_url')}")
        else:
            print("WARNING: NO ACTIVE DEPLOYMENTS!")
        
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
