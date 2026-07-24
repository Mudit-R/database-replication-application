import json, os, glob

samples_dir = 'src/main/resources/samples'
lines = []

for path in sorted(glob.glob(os.path.join(samples_dir, '*.json'))):
    with open(path, encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception as e:
            continue
    fname = os.path.basename(path)
    htype = data.get('header', {}).get('type', '')
    topic = 'nosql-replication' if htype == 'invoice' else 'nosql-orders'
    line = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    lines.append(f'# [{fname}]  topic: {topic}')
    lines.append(line)
    lines.append('')

with open('demo-all-messages.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

count = sum(1 for l in lines if l.startswith('{'))
print(f'Done — {count} messages written to demo-all-messages.txt')
