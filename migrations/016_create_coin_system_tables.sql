-- Migration 016: Create Coin System Tables
-- Purpose: Implement coin-based monetization system
-- Date: 2025-12-07
-- Status: Hybrid Coins + Ads monetization model

-- ============================================================================
-- Table 1: user_coins (Wallet Storage)
-- ============================================================================
-- Stores each user's coin balance and lifetime statistics

CREATE TABLE IF NOT EXISTS user_coins (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID UNIQUE NOT NULL,
  balance INTEGER DEFAULT 0 CHECK (balance >= 0),              -- Current coin balance (cannot be negative)
  lifetime_earned INTEGER DEFAULT 0 CHECK (lifetime_earned >= 0), -- Total coins earned from ads
  lifetime_spent INTEGER DEFAULT 0 CHECK (lifetime_spent >= 0),   -- Total coins spent on generations
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),         -- When balance last changed
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_user_coins_user_id ON user_coins(user_id);
CREATE INDEX IF NOT EXISTS idx_user_coins_balance ON user_coins(balance);

-- Add comment for documentation
COMMENT ON TABLE user_coins IS 'Stores user coin balances for the monetization system';
COMMENT ON COLUMN user_coins.balance IS 'Current available coins for spending';
COMMENT ON COLUMN user_coins.lifetime_earned IS 'Total coins earned from watching ads';
COMMENT ON COLUMN user_coins.lifetime_spent IS 'Total coins spent on image/video generation';

-- ============================================================================
-- Table 2: coin_transactions (Audit Log)
-- ============================================================================
-- Records every coin transaction for auditing and fraud detection

CREATE TABLE IF NOT EXISTS coin_transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN (
    'ad_watched',              -- User watched ad
    'generation_used',         -- User spent coins on generation
    'admin_bonus',             -- Manual admin adjustment
    'refund',                  -- Refund/dispute resolution
    'initial_bonus'            -- Starting bonus for new users (if applicable)
  )),
  coins_delta INTEGER NOT NULL,         -- Positive (earned) or negative (spent)
  balance_after INTEGER NOT NULL CHECK (balance_after >= 0), -- Balance after transaction
  reference_id UUID,                     -- Links to generation_job or ad_completion
  description TEXT,                      -- For admin notes or additional context
  metadata JSONB,                        -- Additional data (ad_network_id, job details, etc.)
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE
);

-- Indexes for analytics and queries
CREATE INDEX IF NOT EXISTS idx_coin_transactions_user_id ON coin_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_coin_transactions_type ON coin_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_coin_transactions_created_at ON coin_transactions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_coin_transactions_reference_id ON coin_transactions(reference_id);

-- Add comments
COMMENT ON TABLE coin_transactions IS 'Audit log of all coin transactions for fraud detection and analytics';
COMMENT ON COLUMN coin_transactions.coins_delta IS 'Positive for earning, negative for spending';
COMMENT ON COLUMN coin_transactions.balance_after IS 'User balance immediately after this transaction';
COMMENT ON COLUMN coin_transactions.reference_id IS 'UUID of related job or ad completion';

-- ============================================================================
-- Table 3: ad_completions (Ad Tracking)
-- ============================================================================
-- Tracks ad watches for fraud detection and revenue analytics

CREATE TABLE IF NOT EXISTS ad_completions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  ad_network_id VARCHAR(255) NOT NULL,        -- From Google AdMob, Unity Ads, etc.
  ad_type VARCHAR(50) DEFAULT 'rewarded',     -- 'rewarded', 'interstitial', 'banner'
  coins_awarded INTEGER DEFAULT 5 CHECK (coins_awarded > 0), -- Coins given for watching
  watched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),    -- When ad was watched
  duration_seconds INTEGER,                   -- How long user watched
  ip_address INET,                            -- For fraud detection
  user_agent TEXT,                            -- Browser/device info
  device_fingerprint VARCHAR(255),            -- For fraud detection
  verified BOOLEAN DEFAULT false,             -- Whether ad network confirmed completion
  metadata JSONB,                             -- Additional ad network data
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE
);

-- Prevent duplicate rewards for same ad (5-minute window)
-- Note: This is enforced by application logic, not DB constraint
-- CREATE UNIQUE INDEX idx_ad_completions_unique ON ad_completions(user_id, ad_network_id, date_trunc('minute', watched_at));

-- Indexes for fraud detection & analytics
CREATE INDEX IF NOT EXISTS idx_ad_completions_user_id ON ad_completions(user_id);
CREATE INDEX IF NOT EXISTS idx_ad_completions_watched_at ON ad_completions(watched_at DESC);
CREATE INDEX IF NOT EXISTS idx_ad_completions_ip_address ON ad_completions(ip_address);
CREATE INDEX IF NOT EXISTS idx_ad_completions_device ON ad_completions(device_fingerprint);

-- Add comments
COMMENT ON TABLE ad_completions IS 'Tracks ad watches for fraud detection and revenue analytics';
COMMENT ON COLUMN ad_completions.verified IS 'Whether ad network API confirmed the completion';
COMMENT ON COLUMN ad_completions.ip_address IS 'Used for fraud detection - multiple accounts from same IP';
COMMENT ON COLUMN ad_completions.device_fingerprint IS 'Used for fraud detection - same device = suspicious';

-- ============================================================================
-- Table 4: Modify existing jobs table
-- ============================================================================
-- Add coin cost tracking to generation jobs

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS coins_cost INTEGER DEFAULT 5;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS coins_deducted_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS requires_coin_check BOOLEAN DEFAULT true;

-- Add comments
COMMENT ON COLUMN jobs.coins_cost IS 'How many coins this generation cost (default: 5)';
COMMENT ON COLUMN jobs.coins_deducted_at IS 'When coins were deducted from user balance';
COMMENT ON COLUMN jobs.requires_coin_check IS 'Whether to enforce coin payment (false for admin/test jobs)';

-- ============================================================================
-- Row Level Security (RLS) Policies
-- ============================================================================

-- Enable RLS on coin tables
ALTER TABLE user_coins ENABLE ROW LEVEL SECURITY;
ALTER TABLE coin_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ad_completions ENABLE ROW LEVEL SECURITY;

-- Policy: Users can view their own coin balance
CREATE POLICY user_coins_select_own ON user_coins
  FOR SELECT
  USING (auth.uid() = user_id);

-- Policy: Users can view their own transactions
CREATE POLICY coin_transactions_select_own ON coin_transactions
  FOR SELECT
  USING (auth.uid() = user_id);

-- Policy: Users can view their own ad completions
CREATE POLICY ad_completions_select_own ON ad_completions
  FOR SELECT
  USING (auth.uid() = user_id);

-- Note: INSERT/UPDATE policies handled by backend service role
-- Backend has elevated permissions to award/deduct coins

-- ============================================================================
-- Helper Function: Initialize user coins on signup
-- ============================================================================

CREATE OR REPLACE FUNCTION initialize_user_coins()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO user_coins (user_id, balance, lifetime_earned, lifetime_spent)
  VALUES (NEW.id, 0, 0, 0)
  ON CONFLICT (user_id) DO NOTHING;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger: Auto-create coin wallet when new user signs up
DROP TRIGGER IF EXISTS trigger_initialize_user_coins ON auth.users;
CREATE TRIGGER trigger_initialize_user_coins
  AFTER INSERT ON auth.users
  FOR EACH ROW
  EXECUTE FUNCTION initialize_user_coins();

-- ============================================================================
-- Configuration Constants (for reference)
-- ============================================================================
-- GENERATION_COST = 5 coins per generation
-- AD_REWARD = 5 coins per ad watched
-- AD_DURATION = 30-60 seconds per ad
-- MAX_ADS_PER_DAY = 50 ads (250 coins daily limit)
-- DUPLICATE_CHECK_WINDOW = 5 minutes

-- ============================================================================
-- Verification Queries
-- ============================================================================
-- Run these after migration to verify:
-- SELECT * FROM user_coins LIMIT 5;
-- SELECT * FROM coin_transactions LIMIT 5;
-- SELECT * FROM ad_completions LIMIT 5;
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'jobs' AND column_name LIKE 'coins%';
