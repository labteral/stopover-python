# Installation
```bash
pip install stopover
```

# Usage
## Sender
```python
from stopover import Receiver

sender = Sender('http://localhost:8080', 'stream0')

index = 0
while True:
  sender.put(f'hello world #{index}')
  index += 1
```

## Receiver
```python
from stopover import Receiver

receiver = Receiver('http://localhost:8080', 'stream0', 'receiver1')

for message in receiver.listen():
    print(message.index, message.value)
    receiver.commit(message)
```
