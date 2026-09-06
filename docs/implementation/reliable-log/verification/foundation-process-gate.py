from pathlib import Path
import hashlib, json, os, shutil, signal, subprocess, sys, tempfile, time

# Root acceptance helper. Existing checked JS runner owns wire assertions.
repo, input_binary, configs, foundation = map(Path, sys.argv[1:5])
root = Path(tempfile.mkdtemp(prefix='comb-foundation-verified-'))
Path('/tmp/comb-foundation-verified-path.txt').write_text(str(root)+'\n')
binary = root/'comb-bridge-immutable'
shutil.copy2(input_binary, binary)
binary.chmod(0o500)
sha = hashlib.sha256(binary.read_bytes()).hexdigest()

def run(command, cwd, log):
    with log.open('w') as output:
        child = subprocess.Popen(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return child.wait(timeout=240)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try: child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            return 124

results=[]
for backend in ['local','minio','s3']:
    for order in ['forward','reverse']:
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == sha
        name=backend+'-'+order
        capture=root/name
        start=time.monotonic()
        code=run(['node',str(repo/'scripts/foundation-bridge-acceptance.mjs'),str(binary),str(configs/backend),str(capture),order],repo,root/(name+'-capture.log'))
        item={'backend':backend,'order':order,'capture_exit':code,'capture_seconds':round(time.monotonic()-start,3)}
        if code==0:
            receipt=json.loads((capture/'receipt.json').read_text())
            assert receipt['ok'] is True and receipt['binary_sha256']==sha
            verify_code=run(['pnpm','exec','tsx',str(repo/'scripts/foundation-bridge-verify.mjs'),str(foundation),str(capture/'captured-feed.json')],foundation,root/(name+'-verify.log'))
            item.update(verify_exit=verify_code,receipt=receipt,capture_sha256=hashlib.sha256((capture/'captured-feed.json').read_bytes()).hexdigest())
            if verify_code==0:
                item['reconstruction']=json.loads((root/(name+'-verify.log')).read_text().splitlines()[-1])
        results.append(item)
        print(json.dumps({k:v for k,v in item.items() if k not in ['receipt','reconstruction']}),flush=True)
        (root/'progress.json').write_text(json.dumps(results,indent=2)+'\n')
        if code!=0 or item.get('verify_exit')!=0:
            print('Gate failed; logs:',root,flush=True)
            sys.exit(1)
runner_files=sorted(str(path.relative_to(repo)) for path in (repo/'scripts').glob('foundation-bridge-*.mjs'))+['docs/testing/fixtures/foundation-bridge.json']
summary={'ok':True,'binary_sha256':sha,'bridge_commit':sys.argv[5],'acceptance_commit':sys.argv[6] if len(sys.argv)>6 else sys.argv[5],'runner_sha256':{name:hashlib.sha256((repo/name).read_bytes()).hexdigest() for name in runner_files},'foundation_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=foundation,text=True).strip(),'runs':results}
(root/'receipt.json').write_text(json.dumps(summary,indent=2)+'\n')
print('All six capture/reconstruction runs passed:',root,flush=True)
