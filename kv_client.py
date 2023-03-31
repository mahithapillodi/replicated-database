import grpc
import kv_pb2
import kv_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = kv_pb2_grpc.KeyValueServiceStub(channel)

def put(key, value):
    request = kv_pb2.KeyValue(key=key, value=value)
    response = stub.Put(request)
    return response

def get(key):
    request = kv_pb2.Key(key=key)
    try:
        response = stub.Get(request)
        return response.value
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            print('Key not found')
        else:
            print('Error: %s' % e)

if __name__ == '__main__':
    put('foo', 'value')
    value = get('foo')
    print(value)
