-- Enable Realtime for jobs table
-- This allows Supabase Realtime to publish changes to the jobs table

-- Set replica identity to FULL so all columns are included in realtime updates
ALTER TABLE jobs REPLICA IDENTITY FULL;

-- Enable realtime publication for jobs table
-- Note: Run this via Supabase dashboard SQL editor if needed
-- ALTER PUBLICATION supabase_realtime ADD TABLE jobs;

-- For Supabase, you need to enable realtime via the dashboard:
-- 1. Go to Database > Replication
-- 2. Enable replication for the "jobs" table
-- 3. Check "UPDATE" events

-- Alternatively, you can run this in SQL Editor:
DO $$
BEGIN
    -- Enable realtime for jobs table
    PERFORM net.http_post(
        url := 'https://gtgnwrwbcxvasgetfzby.supabase.co/rest/v1/rpc/enable_realtime',
        headers := jsonb_build_object('apikey', current_setting('request.jwt.claim.sub'))
    );
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Realtime enablement requires manual configuration in Supabase Dashboard';
END $$;

-- Grant necessary permissions for realtime
GRANT SELECT ON jobs TO anon;
GRANT SELECT ON jobs TO authenticated;

-- Important notes:
-- Realtime must be enabled in the Supabase Dashboard at:
-- Database > Publications > supabase_realtime
-- Add the "jobs" table to the publication
