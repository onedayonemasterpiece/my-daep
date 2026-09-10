from __future__ import annotations

from .config import Settings


def render_worker_bundle(settings: Settings) -> str:
    """Return a self-contained stdlib worker executed inside a private Kaggle notebook."""
    return f'''import base64, json, os, pathlib, subprocess, sys, threading, time, urllib.parse, urllib.request, uuid
SUP=os.environ["DAEP_SUPERVISOR_URL"].rstrip("/")
ATT=os.environ["DAEP_ATTEMPT_ID"]
TOK=os.environ["DAEP_ATTEMPT_TOKEN"]
WID="worker_"+uuid.uuid4().hex[:16]
HEARTBEAT={int(settings.heartbeat_seconds)}
CHECKPOINT={int(settings.checkpoint_seconds)}
STOP=threading.Event()
SEQ=0
BOOT=None

def api(method, path, body=None):
    data=None if body is None else json.dumps(body).encode()
    req=urllib.request.Request(SUP+path, data=data, method=method, headers={{"Authorization":"Bearer "+TOK,"Content-Type":"application/json"}})
    with urllib.request.urlopen(req, timeout=40) as r:
        raw=r.read()
        return json.loads(raw.decode()) if raw else {{}}

def run(args, cwd=None, check=True):
    p=subprocess.run(args,cwd=cwd,text=True,capture_output=True)
    if check and p.returncode:
        raise RuntimeError((p.stderr or p.stdout)[-4000:])
    return p

def token():
    q=urllib.parse.urlencode({{"worker_id":WID}})
    return api("GET",f"/v1/worker/{{ATT}}/github-token?{{q}}")

def git(args,cwd,check=True):
    t=token()["token"]
    auth=base64.b64encode(("x-access-token:"+t).encode()).decode()
    return run(["git","-c",f"http.extraHeader=Authorization: Basic {{auth}}",*args],cwd=cwd,check=check)

def emit(kind,payload=None,significant=False):
    global SEQ
    SEQ+=1
    api("POST",f"/v1/worker/{{ATT}}/event",{{"worker_id":WID,"event_key":f"{{ATT}}:{{kind}}:{{SEQ}}","seq":SEQ,"kind":kind,"significant":significant,"payload":payload or {{}}}})

def checkpoint(repo, reason):
    global SEQ
    p=run(["git","status","--porcelain"],cwd=repo,check=True)
    if p.stdout.strip():
        run(["git","add","-A"],cwd=repo)
        run(["git","-c","user.name=DAEP Worker","-c","user.email=daep-worker@local","commit","-m",f"daep checkpoint: {{reason}}"],cwd=repo)
    sha=run(["git","rev-parse","HEAD"],cwd=repo).stdout.strip()
    git(["push","origin",f"HEAD:refs/heads/{{BOOT['branch']}}"],repo)
    read=git(["ls-remote","origin",f"refs/heads/{{BOOT['branch']}}"],repo).stdout.split()[0]
    SEQ+=1
    api("POST",f"/v1/worker/{{ATT}}/checkpoint",{{"worker_id":WID,"seq":SEQ,"sha":sha,"branch":BOOT["branch"],"readback_sha":read}})
    return sha

def heartbeat_loop(repo):
    while not STOP.wait(HEARTBEAT):
        try:
            api("POST",f"/v1/worker/{{ATT}}/heartbeat",{{"worker_id":WID,"seq":SEQ,"payload":{{"phase":"coding"}}}})
            q=urllib.parse.urlencode({{"worker_id":WID}})
            for c in api("GET",f"/v1/worker/{{ATT}}/commands?{{q}}").get("commands",[]):
                if c["kind"]=="stop":
                    STOP.set()
                api("POST",f"/v1/worker/{{ATT}}/commands/{{c['id']}}/ack",{{"worker_id":WID,"result":{{"seen":True}}}})
        except Exception:
            pass

def checkpoint_loop(repo):
    while not STOP.wait(CHECKPOINT):
        try: checkpoint(repo,"periodic")
        except Exception as e:
            try: emit("checkpoint_failed",{{"error":str(e)}},True)
            except Exception: pass

def main():
    global BOOT
    BOOT=api("POST",f"/v1/worker/{{ATT}}/register",{{"worker_id":WID,"seq":0,"payload":{{}}}})
    root=pathlib.Path("/kaggle/working/daep-work")
    root.mkdir(parents=True,exist_ok=True)
    repo=root/"repo"
    if repo.exists():
        run(["rm","-rf",str(repo)])
    repo.mkdir()
    git(["init"],repo)
    git(["remote","add","origin",f"https://github.com/{{BOOT['repository']}}.git"],repo)
    git(["fetch","--depth=80","origin",BOOT["base_sha"]],repo)
    run(["git","checkout","-B",BOOT["branch"],BOOT["base_sha"]],cwd=repo)
    emit("worker_ready",{{"base_sha":BOOT["base_sha"],"branch":BOOT["branch"],"model":BOOT["model"]}},True)
    threading.Thread(target=heartbeat_loop,args=(repo,),daemon=True).start()
    threading.Thread(target=checkpoint_loop,args=(repo,),daemon=True).start()
    try:
        if {bool(settings.opencode_install)!r}:
            run(["npm","install","-g",{settings.opencode_package!r}],check=True)
        cmd=["opencode","run","--model",BOOT["model"],BOOT["instruction"]]
        emit("opencode_started",{{"command":["opencode","run","--model",BOOT["model"],"<instruction>"]}},True)
        p=subprocess.Popen(cmd,cwd=repo,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        tail=[]
        while p.poll() is None and not STOP.wait(2):
            if p.stdout:
                line=p.stdout.readline()
                if line: tail=(tail+[line[-1000:]])[-40:]
        if STOP.is_set() and p.poll() is None:
            p.terminate()
            try:p.wait(15)
            except subprocess.TimeoutExpired:p.kill()
            sha=checkpoint(repo,"stop")
            api("POST",f"/v1/worker/{{ATT}}/stopped",{{"worker_id":WID,"seq":SEQ,"payload":{{"cancelled":True,"reason":"stop command","checkpoint_sha":sha}}}})
            return
        rc=p.wait()
        output="".join(tail)
        if rc!=0:
            sha=checkpoint(repo,"opencode-failure")
            low=output.lower()
            if "free usage exceeded" in low or "usage limit" in low or "rate limit" in low:
                api("POST",f"/v1/worker/{{ATT}}/model-quota",{{"worker_id":WID,"result_sha":sha}})
                return
            emit("opencode_failed",{{"returncode":rc,"tail":output[-3000:],"checkpoint_sha":sha}},True)
            api("POST",f"/v1/worker/{{ATT}}/stopped",{{"worker_id":WID,"seq":SEQ,"payload":{{"cancelled":False,"reason":"opencode exit %s"%rc}}}})
            return
        sha=checkpoint(repo,"final")
        emit("opencode_completed",{{"result_sha":sha}},True)
        api("POST",f"/v1/worker/{{ATT}}/complete",{{"worker_id":WID,"result_sha":sha}})
    except Exception as e:
        try:
            sha=checkpoint(repo,"exception") if repo.exists() else None
            emit("worker_exception",{{"error":str(e),"checkpoint_sha":sha}},True)
            api("POST",f"/v1/worker/{{ATT}}/stopped",{{"worker_id":WID,"seq":SEQ,"payload":{{"cancelled":False,"reason":str(e)}}}})
        except Exception: pass
        raise
    finally:
        STOP.set()

main()
'''
