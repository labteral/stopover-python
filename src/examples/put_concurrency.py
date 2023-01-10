from stopover import Stopover
from threading import Thread, Lock


def thread_func(name):
    for i in range(1000):
        with lock:
            message = f'{name}_{i}'
            stopover.put(message, topic)
            print(f'sent: {message}')
            data.add(message)

endpoint = 'http://localhost:5704'
stopover = Stopover(
    endpoint,
    client_id='guest',
    client_secret='guest',
)

topic = 'test_topic1'
group = 'test_group1'

lock = Lock()
data = set()

threads = []
for i in range(8):
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
