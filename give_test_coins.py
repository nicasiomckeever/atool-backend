"""
Give Test Coins Script
Purpose: Add coins to a user's wallet for testing the coin system
Usage: python give_test_coins.py <user_email> <coins_amount>
Example: python give_test_coins.py user@example.com 100
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

def give_coins(user_email: str, coins_amount: int):
    """Give coins to a user"""
    print(f"\n{'='*60}")
    print(f"GIVING TEST COINS")
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
    
    # Award coins
    print(f"\n🎁 Awarding {coins_amount} coins...")
    success = coins.award_coins(
        user_id=user_id,
        coins_amount=coins_amount,
        source='admin_bonus',
        description=f"Test coins granted by admin"
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
        print(f"❌ Failed to award coins")
        print(f"\n{'='*60}\n")
        return False

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("\n❌ Usage: python give_test_coins.py <user_email> <coins_amount>")
        print("Example: python give_test_coins.py user@example.com 100\n")
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
    
    success = give_coins(user_email, coins_amount)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
