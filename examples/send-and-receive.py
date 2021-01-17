#!/usr/bin/env python
# -*- coding: utf-8 -*-+

from stopover import Stopover

endpoint = 'http://localhost:5704'
receiver_group = 'group0'
stream = 'stream0'
key = None  # all the messages with the same key will fall under the same partition

stopover = Stopover(endpoint)

stopover.put('hi 0', stream, key=key)
index = 1
for message in stopover.listen(stream, receiver_group):
    print(message.index, message.value)
    stopover.commit(message, receiver_group)
    stopover.put(f'hi {index}', stream, key=key)
    index += 1