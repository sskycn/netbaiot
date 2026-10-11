#!/usr/bin/env python3
"""Four native payload codecs through real Mosquitto clients and a confirmed webhook.
Run: cargo build -p netbaiot-server; python3 tests/multi_codec_interop.py
Uses public development fixtures only. No external runtime broker is started.
"""
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
ROOT=Path(__file__).resolve().parents[1]
ADMIN='d'*64
FORMATS=('json','cbor','msgpack','protobuf')

def main():
    pub=shutil.which('mosquitto_pub')
    if not pub: raise RuntimeError('mosquitto_pub is required')
    received=[]
    class Receiver(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n=int(self.headers['Content-Length']);assert 0<n<=16384
            event=json.loads(self.rfile.read(n));assert len(received)<128
            received.append(event)
            self.send_response(204);self.end_headers()
        def log_message(self,*args): pass
    webhook=http.server.HTTPServer(('127.0.0.1',0),Receiver)
    owner=threading.Thread(target=webhook.serve_forever,daemon=True);owner.start()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with tempfile.TemporaryDirectory(prefix='netbaiot-multi-codec-') as tmp:
        config=json.loads((ROOT/'configs/multi-codec.json').read_text())
        reservations=[socket.socket() for _ in range(2)]
        for s in reservations:s.bind(('127.0.0.1',0))
        udp=socket.socket(type=socket.SOCK_DGRAM);udp.bind(reservations[0].getsockname())
        for field,s in zip(('device_ingress','management_http'),reservations):config[field]=f'127.0.0.1:{s.getsockname()[1]}'
        port=reservations[0].getsockname()[1]
        config['limits']={'requests_per_second':10000,'requests_per_ip_second':10000,'messages_per_device_second':10000,'messages_per_tenant_second':10000}
        config['delivery_url']=f'http://127.0.0.1:{webhook.server_port}/events'
        config['spool_directory']=str(Path(tmp)/'spool')
        path=Path(tmp)/'config.json';path.write_text(json.dumps(config))
        for s in [*reservations,udp]:s.close()
        env=os.environ.copy();env['NETBAIOT_ADMIN_SECRET']=ADMIN;env['RUST_LOG']='netbaiot_transports=debug,info'
        with (Path(tmp)/'server.log').open('w') as log:
            server=subprocess.Popen([str(ROOT/'target/debug/netbaiot-server'),str(path)],cwd=ROOT,env=env,stdout=log,stderr=log)
            try:
                req=urllib.request.Request(f"http://{config['management_http']}/api/v1/ready",headers={'Authorization':f'Bearer {ADMIN}'})
                deadline=time.monotonic()+8
                while True:
                    try:
                        with opener.open(req,timeout=.2) as response:
                            if response.status==200:break
                    except OSError:pass
                    if time.monotonic()>deadline:raise AssertionError('gateway readiness timed out')
                    time.sleep(.03)
                for version in ['mqttv311','mqttv5']:
                    for f in FORMATS:
                        credential=next(c for c in config['credentials'] if c['identity']['codec_id']==f'netbaiot-{f}')
                        base=[pub,'-h','127.0.0.1','-p',str(port),'-V',version,'-u',credential['credential_id'],'-P',credential['secret_hex'],'-t',f'v1/t/demo/p/{f}/d/device-1/up']
                        for qos in [0,1,2]:
                            result=subprocess.run([*base,'-q',str(qos),'-f',str(ROOT/f'crates/netbaiot-codecs/tests/fixtures/telemetry.{f}')],capture_output=True,timeout=5)
                            assert result.returncode==0, 'Mosquitto publish failed'
                for f in FORMATS:
                    credential=next(c for c in config['credentials'] if c['identity']['codec_id']==f'netbaiot-{f}')
                    for kind in ['types','event','heartbeat','command_ack']:
                        result=subprocess.run([pub,'-h','127.0.0.1','-p',str(port),'-V','mqttv311','-u',credential['credential_id'],'-P',credential['secret_hex'],'-t',f'v1/t/demo/p/{f}/d/device-1/up','-q','1','-f',str(ROOT/f'crates/netbaiot-codecs/tests/fixtures/{kind}.{f}')],capture_output=True,timeout=5)
                        assert result.returncode==0, f'Mosquitto typed publish failed: {f}/{kind}: {result.stderr.decode(errors="replace")}'
                deadline=time.monotonic()+5
                while len(received)<40 and time.monotonic()<deadline:time.sleep(.01)
                assert len(received)==40,len(received)
                expected={}
                for event in received:
                    assert event['source_message_id']=='sample:1'
                    assert event['tenant_id']=='demo' and event['device_id']=='device-1'
                    assert event['product_id'] in FORMATS
                    payload=event['payload'];key=(payload['kind'],'types' if 'boolean' in payload['data'] else 'normal')
                    if key in expected:assert payload==expected[key],payload
                    else:expected[key]=payload
                assert expected[('telemetry','normal')]['data']=={'temperature':25.3,'humidity':61.2}
                assert len(expected)==5
                server.terminate();assert server.wait(timeout=8)==0
                print(json.dumps({'accepted_webhooks':40,'codecs':list(FORMATS),'mqtt_versions':['3.1.1','5.0'],'qos':[0,1,2],'result':'PASS'}))
            except Exception:
                log.flush()
                print((Path(tmp)/'server.log').read_text()[-4096:])
                raise
            finally:
                if server.poll() is None:server.kill();server.wait(timeout=3)
    webhook.shutdown();owner.join(timeout=2);webhook.server_close()
if __name__=='__main__':main()
