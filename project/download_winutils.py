import urllib.request
import pathlib
import os

urls = [
    'https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.1/bin/winutils.exe',
    'https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe',
    'https://github.com/kontext-tech/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe',
    'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe'
]

out_dir = pathlib.Path('C:/hadoop/bin')
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / 'winutils.exe'

for url in urls:
    try:
        print('trying', url)
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                print('status', response.status)
                continue
            with open(out_path, 'wb') as f:
                f.write(response.read())
            print('downloaded to', out_path)
            break
    except Exception as exc:
        print('failed', url, type(exc).__name__, exc)
else:
    print('all urls failed')
