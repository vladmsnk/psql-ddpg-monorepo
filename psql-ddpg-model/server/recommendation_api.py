import grpc
from concurrent import futures
from proto import recommandations_api_pb2

def NewRecommendationAPI(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    recommandations_api_pb2.Add_RecommendationsAPIServicer_to_server(recommandations_api_pb2.RecommendationsAPIServicer(), server)

    
    return pb.RecommendationsAPIStub(channel)