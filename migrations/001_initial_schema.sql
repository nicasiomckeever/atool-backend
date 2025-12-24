-- =====================================================
-- AI Image Generation Platform - Database Schema
-- Run this in Supabase SQL Editor
-- =====================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- Table: users
-- =====================================================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    credits INTEGER DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Index for faster email lookups
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- =====================================================
-- Table: magic_links
-- =====================================================
CREATE TABLE IF NOT EXISTS magic_links (
    token UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    used_at TIMESTAMP WITH TIME ZONE
);

-- Index for faster token lookups and cleanup
CREATE INDEX IF NOT EXISTS idx_magic_links_token ON magic_links(token);
CREATE INDEX IF NOT EXISTS idx_magic_links_expires ON magic_links(expires_at);

-- =====================================================
-- Table: jobs
-- =====================================================
CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    prompt TEXT NOT NULL,
    model TEXT DEFAULT 'flux1-krea-dev.safetensors',
    aspect_ratio TEXT DEFAULT '1:1',
    image_url TEXT,
    thumbnail_url TEXT,
    error_message TEXT,
    progress INTEGER DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_user_status ON jobs(user_id, status);

-- =====================================================
-- Table: sessions
-- =====================================================
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token TEXT UNIQUE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    user_agent TEXT,
    ip_address INET
);

-- Index for session lookups
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

-- =====================================================
-- Table: usage_logs (for rate limiting and analytics)
-- =====================================================
CREATE TABLE IF NOT EXISTS usage_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    job_id UUID REFERENCES jobs(job_id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    credits_used INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_usage_logs_user_id ON usage_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_logs_created_at ON usage_logs(created_at DESC);

-- =====================================================
-- Row Level Security (RLS) Policies
-- =====================================================

-- Enable RLS on all tables
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE usage_logs ENABLE ROW LEVEL SECURITY;

-- Users can read their own data
CREATE POLICY "Users can view own data" ON users
    FOR SELECT USING (auth.uid() = id);

-- Users can update their own data
CREATE POLICY "Users can update own data" ON users
    FOR UPDATE USING (auth.uid() = id);

-- Jobs policies
CREATE POLICY "Users can view own jobs" ON jobs
    FOR SELECT USING (user_id = auth.uid());

CREATE POLICY "Users can create own jobs" ON jobs
    FOR INSERT WITH CHECK (user_id = auth.uid());

CREATE POLICY "Users can update own jobs" ON jobs
    FOR UPDATE USING (user_id = auth.uid());

CREATE POLICY "Users can delete own jobs" ON jobs
    FOR DELETE USING (user_id = auth.uid());

-- Sessions policies
CREATE POLICY "Users can view own sessions" ON sessions
    FOR SELECT USING (user_id = auth.uid());

-- Usage logs policies
CREATE POLICY "Users can view own usage logs" ON usage_logs
    FOR SELECT USING (user_id = auth.uid());

-- =====================================================
-- Functions
-- =====================================================

-- Function to clean up expired magic links
CREATE OR REPLACE FUNCTION cleanup_expired_magic_links()
RETURNS void AS $$
BEGIN
    DELETE FROM magic_links 
    WHERE expires_at < NOW() OR (used = TRUE AND created_at < NOW() - INTERVAL '24 hours');
END;
$$ LANGUAGE plpgsql;

-- Function to clean up expired sessions
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS void AS $$
BEGIN
    DELETE FROM sessions WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to update user last login
CREATE OR REPLACE FUNCTION update_user_last_login(user_uuid UUID)
RETURNS void AS $$
BEGIN
    UPDATE users SET last_login = NOW() WHERE id = user_uuid;
END;
$$ LANGUAGE plpgsql;

-- Function to get user stats
CREATE OR REPLACE FUNCTION get_user_stats(user_uuid UUID)
RETURNS TABLE (
    total_jobs BIGINT,
    completed_jobs BIGINT,
    failed_jobs BIGINT,
    pending_jobs BIGINT,
    total_credits_used BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT as total_jobs,
        COUNT(*) FILTER (WHERE status = 'completed')::BIGINT as completed_jobs,
        COUNT(*) FILTER (WHERE status = 'failed')::BIGINT as failed_jobs,
        COUNT(*) FILTER (WHERE status IN ('pending', 'running'))::BIGINT as pending_jobs,
        COALESCE(SUM(ul.credits_used), 0)::BIGINT as total_credits_used
    FROM jobs j
    LEFT JOIN usage_logs ul ON j.job_id = ul.job_id
    WHERE j.user_id = user_uuid;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Triggers
-- =====================================================

-- Trigger to auto-update last_activity in sessions
CREATE OR REPLACE FUNCTION update_session_activity()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_activity = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_session_activity
BEFORE UPDATE ON sessions
FOR EACH ROW
EXECUTE FUNCTION update_session_activity();

-- =====================================================
-- Initial Data (Optional)
-- =====================================================

-- Create a test user for development (optional - remove in production)
-- INSERT INTO users (email, credits) 
-- VALUES ('test@example.com', 1000)
-- ON CONFLICT (email) DO NOTHING;

-- =====================================================
-- Storage Buckets (Run separately in Supabase Dashboard)
-- =====================================================

-- Go to Storage → Create new bucket:
-- Bucket name: user-images
-- Public: false (users access via authenticated URLs)
-- Allowed MIME types: image/png, image/jpeg, image/webp
-- Max file size: 10MB

-- Then create this policy in the bucket:
-- Name: "Users can upload own images"
-- Policy: bucket_id = 'user-images' AND auth.uid()::text = (storage.foldername(name))[1]

-- =====================================================
-- Indexes for Performance
-- =====================================================

-- Composite index for common job queries
CREATE INDEX IF NOT EXISTS idx_jobs_user_created ON jobs(user_id, created_at DESC);

-- Index for job status filtering
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at DESC);

-- =====================================================
-- Grant Permissions
-- =====================================================

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO anon, authenticated;

-- Grant permissions on tables
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO authenticated;
GRANT SELECT, INSERT ON magic_links TO anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON jobs TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON sessions TO authenticated;
GRANT SELECT, INSERT ON usage_logs TO authenticated;

-- Grant execute on functions
GRANT EXECUTE ON FUNCTION cleanup_expired_magic_links() TO authenticated;
GRANT EXECUTE ON FUNCTION cleanup_expired_sessions() TO authenticated;
GRANT EXECUTE ON FUNCTION update_user_last_login(UUID) TO authenticated;
GRANT EXECUTE ON FUNCTION get_user_stats(UUID) TO authenticated;

-- =====================================================
-- Completed!
-- =====================================================

-- To verify installation:
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
