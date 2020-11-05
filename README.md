<p align="center">
<img src="misc/stopover.svg" alt="Stopover Logo" width="150"/></a>
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
