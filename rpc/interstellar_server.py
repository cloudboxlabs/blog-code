import time

import grpc
from concurrent import futures

import interstellar_pb2 as pb2
import interstellar_pb2_grpc


class InterstellarServer(interstellar_pb2_grpc.InterstellarCommunicationServicer):

    def __init__(self):
        self.earth_message_history = []

    def SayHello(self, request, context):
        """
        Receive a hello request from Earth and returns a hello from Mars
        """
        self.earth_message_history.append(request.hello_from_earth)
        return pb2.HelloResponse(hello_from_mars='Hello from Mars!')

    def GetMessageFromMars(self, request, context):
        """
        Receive a message request from Earth and returns a iterator of words from Mars
        """
        self.earth_message_history.append(request.request)
        for word in 'Message from Mars!'.split(' '):
            yield pb2.StreamMessageFromMars(message=word)

    def SendMessageFromEarth(self, request_iterator, context):
        """
        Receive an iterator of requests with words from Earth and returns a simple acknowledgement
        """
        for earth_request in request_iterator:
            self.earth_message_history.append(earth_request.message)

        return pb2.ReplyFromMars(reply='Copy!')

    def SendAndReceiveMessage(self, request_iterator, context):
        """
        Receive an iterator of requests with words from Earth and
        returns an iterator of words from Mars
        """
        for earth_request in request_iterator:
            self.earth_message_history.append(earth_request.message)

        for word in 'Live on Mars is great!'.split(' '):
            yield pb2.StreamMessageFromMars(message=word)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    interstellar_pb2_grpc.add_InterstellarCommunicationServicer_to_server(
        InterstellarServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
