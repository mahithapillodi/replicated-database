import grpc
import kv_pb2
import kv_pb2_grpc
from concurrent import futures

# in-memory key-value store
store = {}

class KeyValueServicer(kv_pb2_grpc.KeyValueServiceServicer):
    def Get(self, request, context):
        key = request.key
        value = store.get(key, None)
        if value is not None:
            return kv_pb2.Value(value=value)
        else:
            context.set_details('Key not found')
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return kv_pb2.Value()

    def Put(self, request, context):
        key = request.key
        value = request.value
        store[key] = value
        return kv_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kv_pb2_grpc.add_KeyValueServiceServicer_to_server(KeyValueServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
