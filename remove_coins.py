"""
Remove Coins Script
Purpose: Deduct coins from a user's wallet for testing or admin purposes
Usage: python remove_coins.py <user_email> <coins_amount>
Example: python remove_coins.py user@example.com 50
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import coin system
import coins
from supabase_client import supabase

def get_user_by_email(email: str):
    """Get user by email from Supabase"""
    try:
        response = supabase.table('users').select('*').eq('email', email).execute()
        if response.data and len(response.data) > 0:
            return response.data[0]
        return None
    except Exception as e:
        print(f"❌ Error fetching user: {e}")
        return None

def remove_coins(user_email: str, coins_amount: int):
    """Remove coins from a user"""
    print(f"\n{'='*60}")
    print(f"REMOVING COINS")
    print(f"{'='*60}")
    print(f"Email: {user_email}")
    print(f"Amount: {coins_amount} coins")
    print(f"{'='*60}\n")
    
    # Get user
    user = get_user_by_email(user_email)
    if not user:
        print(f"❌ User not found with email: {user_email}")
        print(f"💡 Make sure the user has signed up first")
        return False
    
    user_id = user['id']
    print(f"✅ Found user: {user_id}")
    print(f"   Name: {user.get('email')}")
    
    # Check current balance
    current_balance = coins.get_coin_balance(user_id)
    print(f"\n💰 Current balance: {current_balance} coins")
    
    # Check if user has enough coins
    if current_balance < coins_amount:
        print(f"\n⚠️  Warning: User only has {current_balance} coins but you're trying to remove {coins_amount}")
        confirm = input("   Continue anyway? This will result in negative balance (y/n): ")
        if confirm.lower() != 'y':
            print("❌ Operation cancelled")
            return False
    
    # Deduct coins
    print(f"\n🔻 Removing {coins_amount} coins...")
    success = coins.deduct_coins(
        user_id=user_id,
        coins_amount=coins_amount,
        reference_id=None,
        description=f"Admin removed {coins_amount} coins"
    )
    
    if success:
        # Get new balance
        new_stats = coins.get_coin_stats(user_id)
        print(f"✅ Success!")
        print(f"\n📊 Updated Balance:")
        print(f"   Balance: {new_stats['balance']} coins")
        print(f"   Lifetime Earned: {new_stats['lifetime_earned']} coins")
        print(f"   Lifetime Spent: {new_stats['lifetime_spent']} coins")
        print(f"   Generations Available: {new_stats['generations_available']}")
        print(f"\n{'='*60}\n")
        return True
    else:
        print(f"❌ Failed to remove coins")
        print(f"\n{'='*60}\n")
        return False

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("\n❌ Usage: python remove_coins.py <user_email> <coins_amount>")
        print("Example: python remove_coins.py user@example.com 50\n")
        sys.exit(1)
    
    user_email = sys.argv[1]
    try:
        coins_amount = int(sys.argv[2])
        if coins_amount <= 0:
            print("\n❌ Coins amount must be positive\n")
            sys.exit(1)
    except ValueError:
        print("\n❌ Coins amount must be a number\n")
        sys.exit(1)
    
    success = remove_coins(user_email, coins_amount)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
