import json
import os

path = "provisioning/dashboards/multi-format-showcase.json"
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    print("Panel count:", len(data["panels"]))
    print("Titles:", [p["title"] for p in data["panels"]])
    print("Requires:", [r["id"] for r in data["__requires"]])
    print("DONE OK")
else:
    print("File not found")
