-- Disposable database only. Load a bounded, deterministic planning dataset.
\set ON_ERROR_STOP on
INSERT INTO ingress_messages
SELECT md5(n::text)::uuid, 't'||(n%10), 'p', 'd'||(n%1000), 's'||n,
       '{}'::jsonb, repeat('x',128)::bytea, 10240, 1000, 100000+n
FROM generate_series(1,10000) n;
INSERT INTO delivery_jobs(message_id,next_attempt_at,expires_at,done,lease_owner,lease_expiry)
SELECT md5(n::text)::uuid, n, 200000, n%4=0,
       CASE WHEN n%7=0 THEN md5('worker')::uuid END,
       CASE WHEN n%7=0 THEN 20000 END
FROM generate_series(1,10000) n;
INSERT INTO commands
SELECT md5('command'||n)::uuid, 't'||(n%10), 'p', 'd'||(n%1000), '{}'::jsonb,
       n,100000+n,200000+n,false FROM generate_series(1,1024) n;
ANALYZE;
\echo 'DEDUP LOOKUP'
EXPLAIN (ANALYZE,BUFFERS) SELECT message_id,canonical,accepted_at FROM ingress_messages WHERE tenant_id='t1' AND product_id='p' AND device_id='d1' AND source_message_id='s1';
\echo 'OUTBOX CLAIM'
BEGIN;
EXPLAIN (ANALYZE,BUFFERS) WITH selected AS (SELECT message_id FROM delivery_jobs WHERE NOT done AND next_attempt_at<=10000 AND expires_at>10000 AND attempts<5 AND (lease_expiry IS NULL OR lease_expiry<=10000) ORDER BY next_attempt_at LIMIT 1 FOR UPDATE SKIP LOCKED), claimed AS (UPDATE delivery_jobs j SET attempts=attempts+1,lease_owner=md5('audit')::uuid,lease_expiry=40000 FROM selected s WHERE j.message_id=s.message_id RETURNING j.message_id,j.attempts,j.expires_at) SELECT c.attempts,c.expires_at,m.message FROM claimed c JOIN ingress_messages m USING(message_id);
ROLLBACK;
\echo 'COMMAND BATCH CLAIM'
EXPLAIN (ANALYZE,BUFFERS) SELECT record FROM commands WHERE NOT terminal AND expires_at>10000 AND next_attempt_at<=10000 AND (tenant_id,product_id,device_id) IN (SELECT d.tenant_id,d.product_id,d.device_id FROM jsonb_to_recordset('[{"tenant_id":"t1","product_id":"p","device_id":"d1"}]') AS d(tenant_id text,product_id text,device_id text)) ORDER BY next_attempt_at LIMIT 16 FOR UPDATE SKIP LOCKED;
\echo 'INGRESS CLEANUP'
EXPLAIN (ANALYZE,BUFFERS) SELECT message_id FROM ingress_messages WHERE expires_at<=105000 ORDER BY expires_at LIMIT 16 FOR UPDATE SKIP LOCKED;
\echo 'COMMAND RETENTION'
EXPLAIN (ANALYZE,BUFFERS) SELECT command_id FROM commands WHERE retain_until<=201000 ORDER BY retain_until LIMIT 16 FOR UPDATE SKIP LOCKED;
\echo 'EXPIRED COMMANDS'
EXPLAIN (ANALYZE,BUFFERS) SELECT record FROM commands WHERE NOT terminal AND expires_at<=101000 LIMIT 16 FOR UPDATE SKIP LOCKED;
\echo 'CAPACITY ACCOUNTING'
EXPLAIN (ANALYZE,BUFFERS) SELECT count(*) n,coalesce(sum(charge),0)::bigint bytes,coalesce(sum(charge) FILTER (WHERE tenant_id='t1'),0)::bigint tenant_bytes,coalesce(sum(charge) FILTER (WHERE tenant_id='t1' AND product_id='p' AND device_id='d1'),0)::bigint device_bytes,count(*) FILTER (WHERE tenant_id='t1') tenant,count(*) FILTER (WHERE tenant_id='t1' AND product_id='p' AND device_id='d1') device FROM ingress_messages;
