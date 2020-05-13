from stopover import Receiver, Sender

endpoint = 'http://localhost:8080'

stream = 'hehehe'

sender = Sender(endpoint, stream)
receiver = Receiver(endpoint, stream, 'receiver1')

index = 0
sender.put(f'hi {index}')
for message in receiver.get():
    print(message.index, message.value)
    receiver.commit(message)
    sender.put(f'hi {index}')
