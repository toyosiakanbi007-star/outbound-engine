-- 0014_create_news_notify_trigger.sql
--
-- Optional: Database-level trigger that sends NOTIFY on company_news insert.
-- This is an ALTERNATIVE to the application-level NOTIFY in db_writer.py.
-- Use this if you want the database itself to notify, regardless of which application inserts.
--
-- The application-level NOTIFY (in db_writer.py) is preferred because:
-- 1. It bundles multiple inserts into one notification with count
-- 2. It includes more context (client_id, timestamp)
-- 3. It only fires after successful commit
--
-- Enable this trigger only if you need DB-level notification as a backup.

-- Function to send notification on insert
CREATE OR REPLACE FUNCTION notify_news_insert()
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
BEGIN
    payload := json_build_object(
        'company_id', NEW.company_id,
        'client_id', NEW.client_id,
        'title', LEFT(NEW.title, 100),
        'source', 'trigger'
    );
    
    PERFORM pg_notify('news_ready', payload::text);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger (disabled by default - enable if needed)
-- DROP TRIGGER IF EXISTS news_insert_notify ON company_news;
-- CREATE TRIGGER news_insert_notify
--     AFTER INSERT ON company_news
--     FOR EACH ROW
--     EXECUTE FUNCTION notify_news_insert();

-- To enable the trigger:
-- CREATE TRIGGER news_insert_notify
--     AFTER INSERT ON company_news
--     FOR EACH ROW
--     EXECUTE FUNCTION notify_news_insert();

-- To disable the trigger:
-- DROP TRIGGER IF EXISTS news_insert_notify ON company_news;

-- Note: The application-level NOTIFY in db_writer.py is preferred.
-- This trigger is provided as a fallback option.
