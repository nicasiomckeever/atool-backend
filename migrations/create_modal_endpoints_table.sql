-- Create modal_endpoints table for hybrid image/video routing
-- Run this in your Supabase SQL editor

CREATE TABLE IF NOT EXISTS modal_endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_type TEXT NOT NULL CHECK (endpoint_type IN ('image', 'video')),
    url TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    description TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for fast lookups
CREATE INDEX IF NOT EXISTS idx_modal_endpoints_type_active 
    ON modal_endpoints(endpoint_type, is_active);

-- Create unique constraint: only one active endpoint per type
CREATE UNIQUE INDEX IF NOT EXISTS idx_modal_endpoints_unique_active 
    ON modal_endpoints(endpoint_type) 
    WHERE is_active = true;

-- Insert initial placeholder endpoints (update URLs after deployment)
INSERT INTO modal_endpoints (endpoint_type, url, description, metadata) VALUES
('image', 'https://placeholder-image-endpoint.modal.run', 'Fast FLUX image generation endpoint (25s startup)', '{"models": ["flux1-schnell", "flux1-krea-dev"], "estimated_startup_seconds": 25}'::jsonb),
('video', 'https://placeholder-video-endpoint.modal.run', 'Full video generation endpoint with Wan models (60s startup)', '{"models": ["wan2.2_t2v", "wan2.2_i2v"], "estimated_startup_seconds": 60}'::jsonb)
ON CONFLICT DO NOTHING;

-- Add comment
COMMENT ON TABLE modal_endpoints IS 'Stores Modal endpoint URLs for hybrid image/video routing';
COMMENT ON COLUMN modal_endpoints.endpoint_type IS 'Type of endpoint: image or video';
COMMENT ON COLUMN modal_endpoints.is_active IS 'Whether this endpoint is currently active and should be used';
COMMENT ON COLUMN modal_endpoints.metadata IS 'Additional metadata like supported models, startup time, etc.';
