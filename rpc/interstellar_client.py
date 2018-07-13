import grpc

import interstellar_pb2 as pb2
import interstellar_pb2_grpc


def get_message_iterator():
    earth_message = 'Live on Earth in fantastic!'

    for word in earth_message.split(' '):
        yield pb2.StreamMessageFromEarth(message=word)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = interstellar_pb2_grpc.InterstellarCommunicationStub(channel)

    # say hello
    print('Calling SayHello...')
    print(stub.SayHello(pb2.HelloRequest(hello_from_earth='Hello!')))

    # get a message from Mars
    print('Calling GetMessageFromMars...')
    for msg in stub.GetMessageFromMars(pb2.MessageRequest(request='Requesting Mars message')):
        print(msg)

    # send message from Earth to Mars asynchronously
    print('Calling SendMessageFromEarth...')
    print(stub.SendMessageFromEarth(get_message_iterator()))

    # send and receive messages between Earth and Mars asynchronously
    print('Calling SendAndReceiveMessage...')
    for msg in stub.SendAndReceiveMessage(get_message_iterator()):
        print(msg)


if __name__ == '__main__':
    run()

