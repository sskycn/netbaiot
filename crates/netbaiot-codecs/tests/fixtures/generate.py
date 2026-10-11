#!/usr/bin/env python3
"""Independent fixed vectors, encoded directly from the wire specifications.
No NetbaIoT codec or Rust serialization implementation is used here.
"""
import json
import struct
from pathlib import Path
ROOT = Path(__file__).parent

def cbor_head(major, n):
    for info, limit, fmt in [(None,24,None),(24,256,">B"),(25,65536,">H"),(26,2**32,">I"),(27,2**64,">Q")]:
        if n < limit:
            return bytes([(major << 5) | (n if info is None else info)]) + (struct.pack(fmt,n) if fmt else b"")
    raise ValueError("integer too large")
def cbor(v):
    if v is None: return b"\xf6"
    if isinstance(v,bool): return b"\xf5" if v else b"\xf4"
    if isinstance(v,int): return cbor_head(0 if v>=0 else 1,v if v>=0 else -1-v)
    if isinstance(v,float): return b"\xfb"+struct.pack(">d",v)
    if isinstance(v,str):
        s=v.encode(); return cbor_head(3,len(s))+s
    if isinstance(v,dict): return cbor_head(5,len(v))+b"".join(cbor(k)+cbor(n) for k,n in v.items())
    raise ValueError("unsupported")
def msgpack(v):
    if v is None: return b"\xc0"
    if isinstance(v,bool): return b"\xc3" if v else b"\xc2"
    if isinstance(v,int):
        if 0<=v<128 or -32<=v<0: return bytes([v&255])
        return (b"\xcf"+struct.pack(">Q",v)) if v>=0 else (b"\xd3"+struct.pack(">q",v))
    if isinstance(v,float): return b"\xcb"+struct.pack(">d",v)
    if isinstance(v,str):
        s=v.encode(); return (bytes([0xa0|len(s)]) if len(s)<32 else b"\xda"+struct.pack(">H",len(s)))+s
    if isinstance(v,dict): return (bytes([0x80|len(v)]) if len(v)<16 else b"\xde"+struct.pack(">H",len(v)))+b"".join(msgpack(k)+msgpack(n) for k,n in v.items())
    raise ValueError("unsupported")
def varint(n):
    out=bytearray()
    while n>127: out.append((n&127)|128);n>>=7
    out.append(n);return bytes(out)
def pfield(n,v,wire=2):
    if wire==0: return varint(n<<3)+varint(v)
    if wire==1: return varint((n<<3)|1)+struct.pack("<d",v)
    v=v.encode() if isinstance(v,str) else v
    return varint((n<<3)|2)+varint(len(v))+v

def proto_scalar(v):
    if isinstance(v,bool):return pfield(2,int(v),0)
    if isinstance(v,str):return pfield(3,v)
    if isinstance(v,int):return pfield(5,v,0) if v>=0 else pfield(4,(-v*2)-1,0)
    return pfield(1,v,1)
def protobuf(v):
    data=v['data'];kind=v['kind']
    if kind=='telemetry':
        payload=b''.join(pfield(1,pfield(1,k)+pfield(2,proto_scalar(n))) for k,n in data.items());tag=10
    elif kind=='event': payload=pfield(1,data['name'])+(pfield(2,proto_scalar(data['value'])) if data.get('value') is not None else b'');tag=11
    elif kind=='heartbeat':payload=pfield(1,data['sequence'],0);tag=12
    else:payload=pfield(1,data['command_id'])+pfield(2,{'running':1,'succeeded':2,'failed':3}.get(data['execution'],99),0);tag=13
    return pfield(1,v['schema_version'],0)+pfield(2,v['source_message_id'])+(pfield(3,v['occurred_at'],0) if v.get('occurred_at') is not None else b'')+pfield(tag,payload)

def main():
    cases={
        'telemetry':('telemetry',{'temperature':25.3,'humidity':61.2}),
        'types':('telemetry',{'number':-42,'boolean':True,'text':'温度'}),
        'event':('event',{'name':'boot','value':True}),
        'heartbeat':('heartbeat',{'sequence':2**64-1}),
        'command_ack':('command_ack',{'command_id':'00000000-0000-0000-0000-000000000001','execution':'succeeded'}),
        'integer_overflow':('telemetry',{'n':2**53+1}),
        'nonfinite':('telemetry',{'n':float('inf')}),
        'unknown_execution':('command_ack',{'command_id':'00000000-0000-0000-0000-000000000001','execution':'unknown'}),
    }
    for name,(kind,data) in cases.items():
        v={'schema_version':1,'source_message_id':'sample:1','occurred_at':1000,'kind':kind,'data':data}
        for ext,encoder in [('json',lambda v:json.dumps(v,ensure_ascii=False,separators=(',',':')).encode()),('cbor',cbor),('msgpack',msgpack),('protobuf',protobuf)]:
            (ROOT/f'{name}.{ext}').write_bytes(encoder(v))
    # All sizes carry identical values across formats for reproducible benchmarks.
    for count in [2,16,64]:
        v={'schema_version':1,'source_message_id':'bench:1','kind':'telemetry','data':{f'field{i}':i+0.5 for i in range(count)}}
        for ext,encoder in [('json',lambda v:json.dumps(v,separators=(',',':')).encode()),('cbor',cbor),('msgpack',msgpack),('protobuf',protobuf)]:
            (ROOT/f'bench{count}.{ext}').write_bytes(encoder(v))

# Exact downlink vectors use each format's smallest encoding where its Rust writer
# does so. They are still generated solely from the external wire specifications.
def cbor_command(v):
    if isinstance(v,float):
        for code,fmt in [(b'\xf9','>e'),(b'\xfa','>f'),(b'\xfb','>d')]:
            packed=struct.pack(fmt,v)
            if struct.unpack(fmt,packed)[0]==v:return code+packed
    if isinstance(v,dict):return cbor_head(5,len(v))+b''.join(cbor_command(k)+cbor_command(n) for k,n in v.items())
    return cbor(v)
def msgpack_command(v):
    if isinstance(v,str):
        b=v.encode()
        return (bytes([0xa0|len(b)]) if len(b)<32 else b'\xd9'+bytes([len(b)]) if len(b)<256 else b'\xda'+struct.pack('>H',len(b)))+b
    if isinstance(v,int) and not isinstance(v,bool) and v>=128:
        for tag,limit,fmt in [(0xcc,256,'>B'),(0xcd,65536,'>H'),(0xce,2**32,'>I'),(0xcf,2**64,'>Q')]:
            if v<limit:return bytes([tag])+struct.pack(fmt,v)
    if isinstance(v,dict):return bytes([0x80|len(v)])+b''.join(msgpack_command(k)+msgpack_command(n) for k,n in v.items())
    return msgpack(v)
def command_vectors():
    cmd={'command_id':'00000000-0000-0000-0000-000000000001','device':{'tenant_id':'t','product_id':'p','device_id':'d'},'expires_at':1000,'payload':{'name':'set','arguments':{'boolean':True,'number':42.0,'text':'温度'}}}
    (ROOT/'command.json').write_bytes(json.dumps(cmd,ensure_ascii=False,separators=(',',':')).encode())
    binary={'schema_version':1,**cmd}
    (ROOT/'command.cbor').write_bytes(cbor_command(binary))
    (ROOT/'command.msgpack').write_bytes(msgpack_command(binary))
    device=b''.join(pfield(i,cmd['device'][k]) for i,k in enumerate(['tenant_id','product_id','device_id'],1))
    args=b''.join(pfield(6,pfield(1,k)+pfield(2,proto_scalar(v))) for k,v in cmd['payload']['arguments'].items())
    (ROOT/'command.protobuf').write_bytes(pfield(1,1,0)+pfield(2,cmd['command_id'])+pfield(3,device)+pfield(4,1000,0)+pfield(5,'set')+args)
if __name__=='__main__':
    main()
    command_vectors()
