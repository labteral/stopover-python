# Installation

```bash
pip install stopover
```

# Usage

```python
from stopover import Stopover

endpoint = 'http://localhost:8080'
receiver_group = 'group0'
stream = 'stream0'

stopover = Stopover(endpoint)

stopover.put(f'hi {index}', stream)
for message in stopover.listen(stream, receiver_group):
    stopover.commit(message, receiver_group)
    stopover.put(f'hi {index}', stream)
```
