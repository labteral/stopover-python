#!/usr/bin/env python
# -*- coding: utf-8 -*-+

from stopover import Stopover

endpoint = 'http://localhost:5704'
receiver_group = 'group1'
stream = 'stream0'

stopover = Stopover(endpoint)


for index in range(1):
    stopover.put(f'message #{index}', stream)

for message in stopover.listen(stream, receiver_group,
                               progress_without_commit=True):
    print(message.index, message.value)
