import json, os, pathlib, shutil, socket, subprocess, sys, tempfile, time

binary_source = pathlib.Path(sys.argv[1]).resolve()
root = pathlib.Path(tempfile.mkdtemp(prefix='pher-comb-smoke-', dir='/tmp'))
binary = root / 'pher'
shutil.copy2(binary_source, binary)
home = root / 'home'
home.mkdir()
env = {**os.environ, 'PHEROMONE_HOME': str(home), 'PHER_EMBED': 'off'}
for key in ['PHER_HTTP', 'PHER_HTTP_TOKEN', 'PHER_HTTP_SINK_TOKEN', 'PHER_RETENTION']:
    env.pop(key, None)
process = None
streams = []

def connect(request):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(str(home / 'pherd.sock'))
    s.sendall(json.dumps(request).encode() + b'\n')
    f = s.makefile('rb')
    streams.append((s, f))
    return s, f

def rpc(request):
    s, f = connect(request)
    try:
        value = json.loads(f.readline())
        assert value.get('ok'), value
        return value
    finally:
        f.close(); s.close(); streams.remove((s, f))

def start():
    global process
    out = open(root / 'daemon.log', 'ab')
    process = subprocess.Popen([str(binary), 'daemon', 'run'], env=env, stdout=out, stderr=out)
    out.close()
    until = time.monotonic() + 20
    while time.monotonic() < until:
        if process.poll() is not None: raise RuntimeError((root / 'daemon.log').read_text())
        try: return rpc({'op': 'status'})
        except (FileNotFoundError, ConnectionRefusedError, socket.timeout): time.sleep(.05)
    raise TimeoutError('isolated daemon readiness')

def emit(i):
    return rpc({'op': 'emit', 'event': {'subject': 'comb.verify', 'payload': {'i': i}, 'source': 'comb-smoke'}})

def stop():
    global process
    for s, f in streams[:]:
        f.close(); s.close(); streams.remove((s, f))
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    process = None

try:
    status = start()
    first_cursor = status['nextSeq'] - 1
    acks = [emit(i) for i in range(520)]
    positions = [a['seq'] for a in acks]
    assert positions == list(range(positions[0], positions[0] + 520)), positions
    tail, reader = connect({'op': 'tail', 'after': first_cursor, 'subject': 'comb.verify'})
    frames = [json.loads(reader.readline()) for _ in range(520)]
    assert [f['seq'] for f in frames] == positions
    assert [f['event']['payload']['i'] for f in frames] == list(range(520))
    live = emit(520)
    frame = json.loads(reader.readline())
    assert frame['seq'] == live['seq'] and frame['event']['payload']['i'] == 520
    cursor = positions[-1]
    rpc({'op': 'cursorCommit', 'name': 'comb-smoke', 'seq': cursor})
    stop()
    restart = start()
    assert restart['nextSeq'] > live['seq']
    tail, reader = connect({'op': 'tail', 'after': cursor, 'subject': 'comb.verify'})
    recovered = json.loads(reader.readline())
    assert recovered['seq'] == live['seq'] and recovered['event']['payload']['i'] == 520
    after_restart = emit(521)
    frame = json.loads(reader.readline())
    assert frame['seq'] == after_restart['seq'] and after_restart['seq'] > live['seq']
    cursors = rpc({'op': 'cursorLs'})
    assert any(c['name'] == 'comb-smoke' and c['seq'] == cursor for c in cursors['cursors'])
    result = {'passed': True, 'historical_events': 520, 'live_events': 2, 'crash_restart': True, 'cursor_preserved': True, 'first_seq': positions[0], 'last_seq': after_restart['seq'], 'home': str(home)}
    (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'artifact': str(root / 'result.json'), **result}))
finally:
    stop()
