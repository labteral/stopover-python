<p align="center">
<img src="misc/stopover.svg" alt="Stopover Logo" width="150"/></a>
</p>

<p align="center">
    <a href="https://pepy.tech/project/stopover/"><img alt="Downloads" src="https://img.shields.io/badge/dynamic/json?style=flat-square&maxAge=3600&label=downloads&query=$.total_downloads&url=https://api.pepy.tech/api/projects/stopover"></a>
    <a href="https://pypi.python.org/pypi/stopover/"><img alt="PyPi" src="https://img.shields.io/pypi/v/stopover.svg?style=flat-square"></a>
    <!--<a href="https://github.com/labteral/pygram/releases"><img alt="GitHub releases" src="https://img.shields.io/github/release/labteral/stopover.svg?style=flat-square"></a>-->
    <a href="https://github.com/labteral/stopover/blob/master/LICENSE"><img alt="License" src="https://img.shields.io/github/license/labteral/stopover.svg?style=flat-square&color=green"></a>
</p>

<h3 align="center">
<b>Python client for Stopover</b>
</h3>

<p align="center">
    <a href="https://www.buymeacoffee.com/brunneis" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" alt="Buy Me A Coffee" height="35px"></a>
</p>

# Installation

```bash
pip install stopover
```

# Usage

```python
from stopover import Stopover

endpoint = 'http://localhost:5704'
receiver_group = 'group0'
stream = 'stream0'

stopover = Stopover(endpoint)

index = 0
stopover.put(f'hi {index}', stream)
for message in stopover.listen(stream, receiver_group):
    stopover.commit(message, receiver_group)
    stopover.put(f'hi {index}', stream)
    index += 1
```
