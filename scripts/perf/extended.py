#!/usr/bin/env python3
"""Bounded independent scenarios; run groups serially after the throughput matrix."""
import sys
from matrix import execute

def case(name, duration=35, rate=25, **kw):
    load=dict(connections=100,publish_rate=rate,qos=1,duration_secs=duration,warmup_secs=4,cooldown_secs=5,ramp_per_sec=500)
    load.update(kw.pop('load',{}))
    return execute(dict(name=name,load=load,sample_secs=2,cooldown_secs=8,**kw))

def commands():
    for rate in [5,25,100,250]:
        r=case(f'downlink_{rate}',duration=45,rate=0,load=dict(command_rate=rate,command_concurrency=8))
        f=next((v for v in reversed(r.get('generator',[])) if v['event']=='final'),{})
        c=f.get('stats',{}).get('counters',{})
        if c.get('client_errors',0) or c.get('commands_rejected',0) or c.get('command_acks',0)<.99*c.get('commands_queued',1):break
    for rate in [25,10,5]:
        passed=True
        for rep in range(1,4):
            r=case(f'downlink_stable_{rate}_r{rep}',duration=60,rate=0,load=dict(command_rate=rate,command_concurrency=8))
            f=next((v['stats'] for v in reversed(r.get('generator',[])) if v['event']=='final'),{});c=f.get('counters',{})
            if r.get('error') or c.get('client_errors',0) or c.get('commands_rejected',0) or c.get('command_errors',0) or c.get('command_acks',0)!=c.get('commands_queued',-1) or c.get('commands_queued',0)<rate*59*.99:
                passed=False;break
        if passed:break
    case('downlink_sql_profile',duration=25,rate=0,load=dict(command_rate=10,command_concurrency=4),sql_trace=True,rust_log='warn,sqlx::pool::acquire=debug,netbaiot_transports::mqtt=debug',profile=True,profile_at=12,vmmap=True)

def protocols():
    for transport in ['http','tcp','udp']:
        for size in ([256,1024] if transport=='udp' else [256,4096]):
            for rate in [25,100,250]:
                r=case(f'{transport}_{size}_{rate}',duration=30,rate=rate,load=dict(transport=transport,payload_bytes=size))
                f=next((v for v in reversed(r.get('generator',[])) if v['event']=='final'),{})
                c=f.get('stats',{}).get('counters',{})
                accepted=r.get('cooldown',{}).get('metrics',{}).get('netbaiot_ingress_accepted_total',0)
                if c.get('client_errors',0) or accepted<.99*c.get('published',1):break
    case('tcp_idle_1000',rate=0,duration=25,load=dict(transport='tcp',connections=1000,subscribe=False),vmmap=True)
    case('tcp_partial_1000',rate=0,duration=40,load=dict(transport='tcp',connections=1000,partial_frame=True,subscribe=False))
    case('udp_overload_recovery',load=dict(transport='udp',phases=[dict(seconds=20,rate=25),dict(seconds=20,rate=2000),dict(seconds=30,rate=25)]),extra_loads=[dict(transport='http',offset=112,connections=16,publish_rate=10,duration_secs=70,warmup_secs=4,cooldown_secs=5)])
    case('udp_replay',duration=25,rate=100,load=dict(transport='udp',udp_replay_every=2))
    case('udp_oversize',duration=10,rate=25,load=dict(transport='udp',payload_bytes=4096))

def downstream_delay():
    return case('sink_delay_200ms_verified',duration=90,rate=25,load=dict(phases=[dict(seconds=20,rate=25),dict(seconds=25,rate=25),dict(seconds=45,rate=25)]),events=[dict(at=24,kind='sink',settings=dict(delay=.2)),dict(at=49,kind='sink',settings={})])

def database_slowdown(seconds,label):
    extra=[dict(transport=p,offset=o,connections=n,publish_rate=rate,duration_secs=65,warmup_secs=4,cooldown_secs=5,retry_connections=True,phases=[dict(seconds=15,rate=rate),dict(seconds=20,rate=rate),dict(seconds=30,rate=rate)]) for p,o,n,rate in [('http',112,15,15),('tcp',128,10,10),('udp',144,5,5)]]
    return case(f'db_{label}',duration=65,rate=50,load=dict(retry_connections=True,phases=[dict(seconds=15,rate=50),dict(seconds=20,rate=50),dict(seconds=30,rate=50)]),extra_loads=extra,events=[dict(at=20,kind='db_lock',seconds=seconds)],rust_log='warn,sqlx::pool::acquire=debug,netbaiot_transports::common=debug')

def database_unavailable():
    return case('db_unavailable_verified',duration=70,rate=25,load=dict(retry_connections=True,phases=[dict(seconds=15,rate=25),dict(seconds=20,rate=25),dict(seconds=35,rate=25)]),events=[dict(at=20,kind='db_unavailable'),dict(at=32,kind='db_available'),dict(at=36,kind='restart')],extra_loads=[dict(transport=p,offset=o,connections=8,publish_rate=8,duration_secs=70,warmup_secs=4,cooldown_secs=5,retry_connections=True) for p,o in [('http',112),('tcp',128),('udp',144)]])

def recovery():
    # Fixed active population; retries are explicit and included in error counts.
    case('uplink_overload_recovery',load=dict(phases=[dict(seconds=20,rate=25),dict(seconds=20,rate=1000),dict(seconds=40,rate=25)],retry_connections=True))
    downstream_delay()
    case('sink_failure_recovery',duration=80,rate=25,load=dict(phases=[dict(seconds=20,rate=25),dict(seconds=20,rate=25),dict(seconds=40,rate=25)]),events=[dict(at=24,kind='sink',settings=dict(status=503)),dict(at=44,kind='sink',settings={})])
    # Advisory-lock stalls occupy application connections, unlike unrelated pg_sleep sessions.
    for seconds,label in [(.25,'moderate'),(2,'severe'),(8,'lock_8s_no_restart')]:
        database_slowdown(seconds,label)
    database_unavailable()

def churn():
    for tls in [False,True]:
        for fraction in [.1,.5,1.0]:
            case(f'churn_{"tls" if tls else "plain"}_{int(fraction*100)}',tls=tls,rate=0,duration=45,load=dict(connections=1000,reconnect_every_secs=10,reconnect_fraction=fraction,retry_connections=True),vmmap=fraction==1.0)
    for clean in [True,False]:
        case(f'cleanup_{"clean" if clean else "abrupt"}',duration=100,rate=0,load=dict(connections=1000,reconnect_every_secs=5,reconnect_fraction=1,clean_disconnect=clean,retry_connections=True),vmmap=True)

def fairness():
    healthy=dict(offset=32,connections=100,publish_rate=25,command_rate=2,command_concurrency=2,duration_secs=65,warmup_secs=4,cooldown_secs=5,retry_connections=True)
    for n in [1,16]:
        case(f'slow_consumers_{n}',duration=65,rate=0,load=dict(connections=n,slow_fraction=1,command_rate=n*5,command_concurrency=8,command_padding=256),extra_loads=[healthy],vmmap=True)
    case('slow_consumers_byte_caps',duration=65,rate=0,load=dict(connections=16,slow_fraction=1,command_rate=40,command_concurrency=8,command_padding=256),extra_loads=[healthy],limits=dict(max_outbound_bytes_per_connection=1024,max_outbound_bytes_per_tenant=4096,max_outbound_bytes=32768),vmmap=True)
    for n,label in [(1,'device'),(16,'tenant')]:
        case(f'noisy_{label}',duration=65,rate=1000,load=dict(connections=n,retry_connections=True),extra_loads=[healthy],limits=dict(messages_per_device_second=16,messages_per_tenant_second=128))
    case('bad_auth_flood',duration=45,rate=0,load=dict(connections=100,bad_auth=True,retry_connections=True),extra_loads=[dict(healthy,duration_secs=45)],tls=True)
    case('noisy_connections',duration=25,rate=0,load=dict(connections=128,tenant_width=128),limits=dict(max_connections=128,max_connections_per_ip=128,max_connections_per_tenant=64),extra_loads=[dict(connections=32,offset=128,tenant_width=128,publish_rate=25,duration_secs=60,warmup_secs=4,cooldown_secs=5,reconnect_every_secs=10,reconnect_fraction=.25,retry_connections=True)])
    # Match extra_loads' default 100/s ramp as well as its rate and identities.
    case('healthy_tls_control',duration=45,rate=25,tls=True,load=dict(offset=32,ramp_per_sec=100,command_rate=2,command_concurrency=2,retry_connections=True))
    case('healthy_fairness_control',duration=65,rate=25,load=dict(offset=32,ramp_per_sec=100,command_rate=2,command_concurrency=2,retry_connections=True))

def database_growth():
    for pool in [1,8,32]:
        case(f'preload_10k_pool_{pool}',duration=45,rate=100,preload=10000,limits=dict(max_database_connections=pool),rust_log='warn,sqlx::pool::acquire=debug,netbaiot_transports::common=debug')
    case('preload_30k',duration=40,rate=25,preload=30000,rust_log='warn,sqlx::pool::acquire=debug,netbaiot_transports::common=debug')
    case('storage_quota_full',load=dict(phases=[dict(seconds=10,rate=25),dict(seconds=20,rate=100),dict(seconds=20,rate=10)],retry_connections=True),limits=dict(max_stored_messages=500,max_stored_messages_per_tenant=250,max_stored_messages_per_device=50),rust_log='warn,netbaiot_transports::common=debug')
    # Recheck the first saturation point with the final independent observer.
    for qos in [0,1]:
        for rate in [250,500,1000]:
            r=case(f'knee_clean_q{qos}_{rate}',duration=60,rate=rate,load=dict(qos=qos),rust_log='warn,netbaiot_transports::common=debug')
            f=next((v['stats'] for v in reversed(r.get('generator',[])) if v['event']=='final'),{});c=f.get('counters',{})
            if r.get('error') or c.get('client_errors',0) or c.get('accepted',0)<rate*60*.99 or r.get('cooldown',{}).get('postgres',{}).get('outbox_pending',0)>0:break
    # Arrival-shape diagnostic: change only the initial connection/PING spacing.
    # Preserve the failed 500/s-ramp cases; this is a different operating profile,
    # not a server optimization or replacement for unfavorable measurements.
    for rate in [50,100]:
        passed=True
        for rep in range(1,4):
            r=case(f'q1_spaced_ramp20_rate{rate}_r{rep}',duration=120,rate=rate,load=dict(ramp_per_sec=20),rust_log='warn,netbaiot_transports::common=debug')
            f=next((v['stats'] for v in reversed(r.get('generator',[])) if v['event']=='final'),{});c=f.get('counters',{})
            if r.get('error') or c.get('client_errors',0) or c.get('accepted',0)!=c.get('published',-1) or c.get('accepted',0)<rate*120*.99 or r.get('cooldown',{}).get('postgres',{}).get('outbox_pending',0)>0:
                passed=False;break
        if not passed:break


def outbound_global_boundary():
    # Four tenants fill a deliberately small node budget before every tenant or
    # connection fills its own budget. This is a boundary probe, not a default.
    return case('slow_consumers_global_cap',duration=65,rate=0,
                load=dict(connections=64,slow_fraction=1,command_rate=64,command_concurrency=8,command_padding=256),
                limits=dict(max_outbound_bytes_per_connection=1024,max_outbound_bytes_per_tenant=4096,max_outbound_bytes=8192),vmmap=True)

def profiles():
    outbound_global_boundary()
    # Also covers continuation from the earlier run with an invalid delay_ms key.
    downstream_delay()
    database_slowdown(8,'lock_8s_no_restart')
    database_unavailable()
    case('downlink_timing',duration=40,rate=0,load=dict(command_rate=10,command_concurrency=8),rust_log='warn,netbaiot_transports::mqtt=debug')
    case('profile_downlink_clean',duration=40,rate=0,load=dict(command_rate=10,command_concurrency=8),profile=True,profile_at=15,sample_seconds=10)
    case('profile_idle_2000',duration=30,rate=0,load=dict(connections=2000,subscribe=False),profile=True,profile_at=15,sample_seconds=10,vmmap=True)
    case('profile_idle_tls_3400',duration=30,rate=0,tls=True,load=dict(connections=3400,subscribe=False),limits=dict(max_mqtt_packet_size=8192,max_http_body_size=8192,max_tcp_frame_size=8192,max_command_bytes=4096,connection_memory_reservation=65536),profile=True,profile_at=15,sample_seconds=10,vmmap=True)
    case('profile_churn_tls',duration=30,rate=0,tls=True,load=dict(connections=1000,reconnect_every_secs=5,retry_connections=True),profile=True,profile_at=15,sample_seconds=10,vmmap=True)
    case('profile_uplink',duration=40,rate=250,profile=True,profile_at=15,sample_seconds=10,vmmap=True)
    case('mixed_70_15_10_5',duration=120,rate=35,load=dict(connections=70,heartbeat_every=5,command_rate=1,command_concurrency=2,reconnect_every_secs=30,reconnect_fraction=.1,retry_connections=True),extra_loads=[dict(transport=p,offset=o,connections=n,publish_rate=rate,duration_secs=120,warmup_secs=4,cooldown_secs=5,heartbeat_every=5,retry_connections=True) for p,o,n,rate in [('http',80,15,7.5),('tcp',96,10,5),('udp',112,5,2.5)]])

if __name__=='__main__':
    {f.__name__:f for f in [commands,protocols,recovery,churn,fairness,profiles,database_growth]}[sys.argv[1]]()
