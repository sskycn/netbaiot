#!/usr/bin/env python3
"""Offline, disposable planning fixture. 100K/1M exceed some runtime quotas;
these rows measure query scaling, never claim application acceptance capacity.
No service points at these databases. All queries match production query shapes.
"""
import json, pathlib, statistics, subprocess, sys, time
from run_case import pg, PG, ENV, ROOT

def seed_sql(rows, commands=True, done=False, now=None):
    assert 0<rows<=1000000
    now=int(time.time()*1000) if now is None else now
    sql=f"""
INSERT INTO ingress_messages
SELECT md5('message'||n)::uuid, 'seed'||(n%10), 'p', 'd'||(n%1000), 's'||n,
 jsonb_build_object('message_id',md5('message'||n)::uuid,'source_message_id','s'||n,'device',jsonb_build_object('tenant_id','seed'||(n%10),'product_id','p','device_id','d'||(n%1000)), 'received_at',{now},'occurred_at',NULL,'payload',jsonb_build_object('kind','telemetry','data',jsonb_build_object('temperature',21.5,'padding',repeat('x',128)))),
 convert_to(jsonb_build_array(jsonb_build_object('tenant_id','seed'||(n%10),'product_id','p','device_id','d'||(n%1000)),'s'||n,NULL,jsonb_build_object('kind','telemetry','data',jsonb_build_object('temperature',21.5,'padding',repeat('x',128))))::text,'UTF8'), 16384, {now}, {now}+86400000
FROM generate_series(1,{rows}) n;
INSERT INTO delivery_jobs(message_id,next_attempt_at,expires_at,done,lease_owner,lease_expiry)
SELECT md5('message'||n)::uuid, {now}-1000+n%1000, {now}+3600000, {'true' if done else 'n%4=0'}, CASE WHEN n%7=0 THEN md5('worker')::uuid END, CASE WHEN n%7=0 THEN {now}+30000 END
FROM generate_series(1,{rows}) n;
"""
    if commands:
        sql+=f"""
INSERT INTO commands SELECT md5('command'||n)::uuid,'seed'||(n%10),'p','d'||(n%1000),
 jsonb_build_object('command',jsonb_build_object('command_id',md5('command'||n)::uuid,'device',jsonb_build_object('tenant_id','seed'||(n%10),'product_id','p','device_id','d'||(n%1000)),'expires_at',{now}+240000,'payload',jsonb_build_object('name','set','arguments',jsonb_build_object('value',42))), 'delivery','dispatching','execution','unknown','attempts',1,'lease_expires_at',{now}-2000),
 {now}-1000+n%1000,{now}+240000,{now}+540000,false FROM generate_series(1,{rows}) n;
INSERT INTO command_attempts SELECT md5('command'||n)::uuid,1,{now}-32000,'dispatching' FROM generate_series(1,{rows}) n;
"""
    return sql+'ANALYZE;'

def queries(now):
    quota="SELECT count(*) n,coalesce(sum(charge),0)::bigint bytes,coalesce(sum(charge) FILTER (WHERE tenant_id='seed1'),0)::bigint tenant_bytes,coalesce(sum(charge) FILTER (WHERE tenant_id='seed1' AND product_id='p' AND device_id='d1'),0)::bigint device_bytes,count(*) FILTER (WHERE tenant_id='seed1') tenant,count(*) FILTER (WHERE tenant_id='seed1' AND product_id='p' AND device_id='d1') device FROM ingress_messages"
    devices=json.dumps([dict(tenant_id=f'seed{i%10}',product_id='p',device_id=f'd{i}') for i in range(1,17)],separators=(',',':'))
    return {
      'quota_aggregation':quota,
      'command_quota':"SELECT count(*) n,count(*) FILTER (WHERE tenant_id='seed1') tenant,count(*) FILTER (WHERE tenant_id='seed1' AND product_id='p' AND device_id='d1') device FROM commands",
      'dedup':"SELECT message_id,canonical,accepted_at FROM ingress_messages WHERE tenant_id='seed1' AND product_id='p' AND device_id='d1' AND source_message_id='s1'",
      'outbox_claim':f"WITH selected AS (SELECT message_id FROM delivery_jobs WHERE NOT done AND next_attempt_at<={now} AND expires_at>{now} AND attempts<5 AND (lease_expiry IS NULL OR lease_expiry<={now}) ORDER BY next_attempt_at LIMIT 1 FOR UPDATE SKIP LOCKED), claimed AS (UPDATE delivery_jobs j SET attempts=attempts+1,lease_owner=md5('audit')::uuid,lease_expiry={now}+30000 FROM selected s WHERE j.message_id=s.message_id RETURNING j.message_id,j.attempts,j.expires_at) SELECT c.attempts,c.expires_at,m.message FROM claimed c JOIN ingress_messages m USING(message_id)",
      'command_device':f"SELECT record FROM commands WHERE NOT terminal AND expires_at>{now} AND next_attempt_at<={now} AND (tenant_id,product_id,device_id) IN (SELECT d.tenant_id,d.product_id,d.device_id FROM jsonb_to_recordset('[{{\"tenant_id\":\"seed1\",\"product_id\":\"p\",\"device_id\":\"d1\"}}]') AS d(tenant_id text,product_id text,device_id text)) ORDER BY next_attempt_at LIMIT 16 FOR UPDATE SKIP LOCKED",
      'command_batch':f"SELECT record FROM commands WHERE NOT terminal AND expires_at>{now} AND next_attempt_at<={now} AND (tenant_id,product_id,device_id) IN (SELECT d.tenant_id,d.product_id,d.device_id FROM jsonb_to_recordset('{devices}') AS d(tenant_id text,product_id text,device_id text)) ORDER BY next_attempt_at LIMIT 16 FOR UPDATE SKIP LOCKED",
      'ingress_retention':f"DELETE FROM ingress_messages WHERE message_id IN (SELECT message_id FROM ingress_messages WHERE expires_at<={now}+86400001 ORDER BY expires_at LIMIT 16 FOR UPDATE SKIP LOCKED)",
      'command_retention':f"DELETE FROM commands WHERE command_id IN (SELECT command_id FROM commands WHERE retain_until<={now}+600001 ORDER BY retain_until LIMIT 16 FOR UPDATE SKIP LOCKED)",
      'attempt_lookup':"SELECT state FROM command_attempts WHERE command_id=md5('command1')::uuid AND attempt=1",
      'expired_commands':f"SELECT record FROM commands WHERE NOT terminal AND expires_at<={now}-1 LIMIT 16 FOR UPDATE SKIP LOCKED",
    }

def run_scale(rows):
    database=f'cap_dataset_{rows}';subprocess.run([PG+'/createdb',database],env=ENV,check=True,capture_output=True)
    pg(database,(ROOT/'migrations/0001_foundation.sql').read_text())
    start=time.monotonic();fixture_time=int(time.time()*1000);pg(database,seed_sql(rows,now=fixture_time),timeout=600)
    result=dict(rows_per_table=rows,load_seconds=time.monotonic()-start,database_bytes=int(pg(database,'SELECT pg_database_size(current_database())')),scope='offline structurally representative JSON fixture; direct SQL bypasses runtime quota; not a capacity claim',queries={})
    result["query_as_of_ms"]=fixture_time+10000
    for name,query in queries(fixture_time+10000).items():
        runs=[]
        prefix="SELECT pg_advisory_xact_lock(782634291); " if name in ('quota_aggregation','command_quota','dedup') else ''
        if name in ('quota_aggregation','dedup'):prefix+=f"DELETE FROM ingress_messages WHERE tenant_id='seed1' AND product_id='p' AND device_id='d1' AND source_message_id='s1' AND expires_at<={fixture_time+10000}; "
        for repetition in range(4):
            raw=pg(database,'BEGIN; '+prefix+'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query+'; ROLLBACK;',timeout=120)
            plan=json.loads(raw[raw.index('['):raw.rindex(']')+1])[0]
            if repetition:runs.append(plan)
        result['queries'][name]=dict(sql=query,transaction_prefix=prefix,execution_ms=[r['Execution Time'] for r in runs],median_ms=statistics.median(r['Execution Time'] for r in runs),plans=runs)
    path=ROOT/'docs/performance'/f'dataset_{rows}.json';path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(dict(rows=rows,bytes=result['database_bytes'],queries={k:v['median_ms'] for k,v in result['queries'].items()})),flush=True)

if __name__=='__main__':
    for rows in ([int(sys.argv[1])] if len(sys.argv)>1 else [10000,100000,1000000]):run_scale(rows)
