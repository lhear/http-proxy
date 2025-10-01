import json
import datetime

with open('list.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

now = datetime.datetime.now()

def relative_time(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp)
    delta = now - dt
    if delta.days > 0:
        return f"{delta.days} days ago"
    elif delta.seconds // 3600 > 0:
        return f"{delta.seconds // 3600} hours ago"
    else:
        return f"{delta.seconds // 60} minutes ago"

sorted_items = sorted(data.items(), key=lambda x: x[1]['added_at'])

print("| Proxy | Location | Added Time |")
print("|---------|----------|------------|")
for service, info in sorted_items:
    rel_time = relative_time(info['added_at'])
    print(f"| {service} | {info['location']} | {rel_time} |")
