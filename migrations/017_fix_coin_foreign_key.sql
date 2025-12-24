-- Migration 017: Fix Coin System Foreign Key Constraint
-- Purpose: Change foreign key from auth.users to public.users
-- Date: 2025-12-07

-- Drop the old constraint
ALTER TABLE user_coins DROP CONSTRAINT IF EXISTS user_coins_user_id_fkey;

-- Add the corrected constraint referencing public.users
ALTER TABLE user_coins
ADD CONSTRAINT user_coins_user_id_fkey
FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

-- Do the same for coin_transactions table
ALTER TABLE coin_transactions DROP CONSTRAINT IF EXISTS coin_transactions_user_id_fkey;

ALTER TABLE coin_transactions
ADD CONSTRAINT coin_transactions_user_id_fkey
FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

-- Do the same for ad_completions table
ALTER TABLE ad_completions DROP CONSTRAINT IF EXISTS ad_completions_user_id_fkey;

ALTER TABLE ad_completions
ADD CONSTRAINT ad_completions_user_id_fkey
FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

-- Success message
COMMENT ON CONSTRAINT user_coins_user_id_fkey ON user_coins IS 'Fixed to reference public.users instead of auth.users';
