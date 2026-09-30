"""Verify Langfuse cloud received traces."""
import base64
import json
import urllib.request

pk = "pk-lf-661d8d9f-d5e1-4bd4-ae9f-68c7f1c93b8a"
sk = "sk-lf-ff9d6ca0-4eb8-4288-9839-eca9e5a12a8f"
auth = base64.b64encode(f"{pk}:{sk}".encode()).decode()

req = urllib.request.Request(
    "https://us.cloud.langfuse.com/api/public/traces?limit=5",
    headers={"Authorization": f"Basic {auth}"},
)
with urllib.request.urlopen(req, timeout=30) as r:
    data = json.loads(r.read())
    traces = data.get("data", [])
    print(f"traces in Langfuse cloud: {len(traces)}")
    for t in traces[:5]:
        tid = t.get("id", "?")
        name = t.get("name", "?")
        ts = t.get("timestamp", "?")
        print(f"  trace_id={tid} name={name} timestamp={ts}")
