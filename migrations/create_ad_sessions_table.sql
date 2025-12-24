-- Migration: Create ad_sessions table for Monetag verification
-- Run this in Supabase SQL Editor

-- Create ad_sessions table if it doesn't exist
CREATE TABLE IF NOT EXISTS ad_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    monetag_click_id TEXT,
    zone_id TEXT,
    ad_type TEXT,
    ip_address TEXT,
    user_agent TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add missing columns if they don't exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'ad_sessions' AND column_name = 'monetag_verified') THEN
        ALTER TABLE ad_sessions ADD COLUMN monetag_verified BOOLEAN DEFAULT FALSE;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'ad_sessions' AND column_name = 'monetag_revenue') THEN
        ALTER TABLE ad_sessions ADD COLUMN monetag_revenue DECIMAL(10, 4) DEFAULT 0;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'ad_sessions' AND column_name = 'completed_at') THEN
        ALTER TABLE ad_sessions ADD COLUMN completed_at TIMESTAMP WITH TIME ZONE;
    END IF;
END $$;

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_ad_sessions_user_id ON ad_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_ad_sessions_monetag_click_id ON ad_sessions(monetag_click_id);
CREATE INDEX IF NOT EXISTS idx_ad_sessions_status ON ad_sessions(status);
CREATE INDEX IF NOT EXISTS idx_ad_sessions_created_at ON ad_sessions(created_at);

-- Enable Row Level Security
ALTER TABLE ad_sessions ENABLE ROW LEVEL SECURITY;

-- Drop and recreate policies to avoid conflicts
DROP POLICY IF EXISTS "Users can view own ad sessions" ON ad_sessions;
DROP POLICY IF EXISTS "Service role can manage all ad sessions" ON ad_sessions;

CREATE POLICY "Users can view own ad sessions" ON ad_sessions
    FOR SELECT USING (auth.uid() = user_id);

CREATE POLICY "Service role can manage all ad sessions" ON ad_sessions
    FOR ALL USING (true);

-- Grant permissions
GRANT ALL ON ad_sessions TO service_role;
GRANT SELECT, INSERT, UPDATE ON ad_sessions TO authenticated;
