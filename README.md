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
