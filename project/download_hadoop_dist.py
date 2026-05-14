import urllib.request
import tarfile
import pathlib
import os

url = 'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz'
archive_path = pathlib.Path('C:/hadoop/hadoop-3.3.1.tar.gz')
output_dir = pathlib.Path('C:/hadoop')
output_dir.mkdir(parents=True, exist_ok=True)

print('Downloading', url)
urllib.request.urlretrieve(url, archive_path)
print('Downloaded to', archive_path)

print('Extracting...')
with tarfile.open(archive_path, 'r:gz') as tar:
    tar.extractall(path=output_dir)
print('Extracted')

# Move extracted contents up if needed
root_dir = output_dir / 'hadoop-3.3.1'
if root_dir.exists():
    for item in root_dir.iterdir():
        target = output_dir / item.name
        if target.exists():
            if target.is_dir():
                continue
        item.replace(target)
    root_dir.rmdir()
print('Hadoop distribution installed in', output_dir)
