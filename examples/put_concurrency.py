from stopover import Stopover
from threading import Thread, Lock


def thread_func(name):
    for i in range(100):
        with lock:
            message = f'{name}_{i}'
            stopover.put(message, topic)
            print(f'sent: {message.value}')
            data.add(message)


topic = 'test_topic'
group = 'test_group'
stopover = Stopover('http://10.0.0.53:5704')
lock = Lock()
data = set()

threads = []
for i in range(32):
    name = f'thread_{i}'
    thread = Thread(target=thread_func, args=[name])
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

for message in stopover.listen(topic, group):
    print(f'received: {message.value}')
    data.remove(message.value)
    print(f'remaining: {len(data)}')
    stopover.commit(message, group)
