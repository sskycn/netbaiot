import pathlib,subprocess,json,resource
out=pathlib.Path('docs/eventbus-lock-evidence/completion-retirement')
bins={'before':'/tmp/netbaiot-eventbus-lock/baseline-target/release/examples/eventbus_probe','after':'/tmp/netbaiot-eventbus-lock/completion-retirement-probe'}
for i in range(1,4):
 for side in (('before','after') if i%2 else ('after','before')):
  print(side,'micro',i,flush=True)
  with (out/f'{side}-micro-{i}.jsonl').open('w') as f:
   subprocess.run([bins[side]],stdout=f,check=True,timeout=120)
for side,binary in bins.items():
 print(side,'overload isolation',flush=True)
 with (out/f'{side}-isolation.json').open('w') as f:
  subprocess.run([binary,'--overload'],stdout=f,check=True,timeout=30)
