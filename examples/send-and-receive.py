#!/usr/bin/env python
# -*- coding: utf-8 -*-+

from stopover import Receiver, Sender

endpoint = 'http://localhost:8080'

stream = 'stream0'

sender = Sender(endpoint, stream)
receiver = Receiver(endpoint, stream, 'receiver1')
key = 'key1'

index = 0
sender.put(f'hi {index}', key=key)
for message in receiver.get():
    print(message.index, message.value)
    receiver.commit(message)
    sender.put(f'hi {index}', key=key)
    index += 1
