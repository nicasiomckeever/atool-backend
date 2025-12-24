"""
Migrate Existing Users to Coin System
Creates coin wallets for all users who don't have one yet
"""

from supabase_client import supabase
import coins

def migrate_users_to_coin_system():
    """Create coin wallets for all existing users"""
    print("=" * 60)
    print("MIGRATING EXISTING USERS TO COIN SYSTEM")
    print("=" * 60)
    
    try:
        # Get all users
        users_response = supabase.table('users').select('id, email').execute()
        users = users_response.data
        
        if not users:
            print("No users found in database")
            return
        
        print(f"\nFound {len(users)} users")
        print("\nChecking which users need coin wallets...")
        
        # Get all user IDs who already have wallets
        existing_wallets_response = supabase.table('user_coins').select('user_id').execute()
        existing_wallet_user_ids = {wallet['user_id'] for wallet in existing_wallets_response.data}
        
        # Find users without wallets
        users_without_wallets = [user for user in users if user['id'] not in existing_wallet_user_ids]
        
        if not users_without_wallets:
            print("[OK] All users already have coin wallets!")
            return
        
        print(f"\n[!] Found {len(users_without_wallets)} users without coin wallets")
        print("\nCreating wallets...")
        
        success_count = 0
        error_count = 0
        
        for user in users_without_wallets:
            user_id = user['id']
            email = user['email']
            
            try:
                # Initialize wallet with 0 coins (they can earn coins by watching ads)
                success = coins.initialize_user_wallet(user_id, initial_balance=0)
                
                if success:
                    print(f"  [OK] {email[:30]:30} - Wallet created (0 coins)")
                    success_count += 1
                else:
                    print(f"  [ERR] {email[:30]:30} - Failed to create wallet")
                    error_count += 1
            except Exception as e:
                print(f"  [ERR] {email[:30]:30} - Error: {e}")
                error_count += 1
        
        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE")
        print("=" * 60)
        print(f"[OK] Success: {success_count} wallets created")
        if error_count > 0:
            print(f"[ERR] Errors: {error_count} failures")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERR] Migration failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    migrate_users_to_coin_system()
