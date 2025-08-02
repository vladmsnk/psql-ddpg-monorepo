import grpc
import api.environment_pb2 as environment_pb2
import  api.environment_pb2_grpc as environment_pb2_grpc

def RunClient(host, port) -> environment_pb2_grpc.EnvironmentStub:
    # Establish a connection to the server using the provided host and port
    channel = grpc.insecure_channel(f'{host}:{port}')
    return environment_pb2_grpc.EnvironmentStub(channel)


class Environment:
    def __init__(self, client : environment_pb2_grpc.EnvironmentStub, dryrun=False):
        self.client = client
        self.dryrun = dryrun
        self.initiated = False
        self.perofrmance_increased = False
        self.start_score = 0
        

    def calculate_reward(self, initial_latency, initial_tps, previous_latency, previous_tps, current_latency, current_tps):
        if 0.6 * current_tps + 0.4*current_latency > self.start_score:
            self.perofrmance_increased = True

        delta_0_latency = (initial_latency - current_latency ) / current_latency if current_latency != 0 else 0
        delta_0_tps = (current_tps - initial_tps) / initial_tps if initial_tps != 0 else 0

        delta_t_tps = (current_tps - previous_tps) / previous_tps if previous_tps != 0 else 0
        delta_t_latency = (previous_latency - current_latency) / current_latency if current_latency != 0 else 0
        
        if delta_0_tps > 0:
            tps_reward = ((1 + delta_0_tps) ** 2 - 1) * abs(1 + delta_t_tps)
        else:
            tps_reward = -((1 - delta_0_tps) ** 2 - 1) * abs(1 - delta_t_tps)

        if delta_0_latency > 0:
            qps_reward = ((1 + delta_0_latency) ** 2 - 1) * abs(1 + delta_t_latency)
        else:
            qps_reward = -((1 - delta_0_latency) ** 2 - 1) * abs(1 - delta_t_latency)

        if tps_reward > 0 and delta_t_tps < 0:
            tps_reward = 0
        if qps_reward > 0 and delta_t_latency < 0:
            qps_reward = 0

        total_reward = 0.6 * tps_reward + 0.4 * qps_reward
        return total_reward



    def step(self, instance_name, knobs, initial_latency, initial_tps, 
             previous_latency, previous_tps):
        if self.dryrun:
            return
        

        self.apply_actions(instance_name, knobs)

        latency, tps = self.get_reward_metrics(instance_name)

        next_state = self.get_states(instance_name)
 
        reward = self.calculate_reward(initial_latency, initial_tps, 
                                       previous_latency, previous_tps, latency, tps)
        
        return next_state, reward, (tps, latency)
 
    def get_states(self, instance_name):
        return self.client.GetStates(environment_pb2.GetStatesRequest(instance_name=instance_name)).metrics
    
    def apply_actions(self, instance_name, knobs):
      
        desc_actions = []

        for knob in knobs:
            desc_actions.append({
                "name": knob,
                "value": knobs[knob]["value"]
            })
    
        return self.client.ApplyActions(environment_pb2.ApplyActionsRequest(instance_name=instance_name, actions=desc_actions))
    
    def init_environment(self, instance_name):
        return self.client.InitEnvironment(environment_pb2.InitEnvironmentRequest(instance_name=instance_name))
    
    def get_reward_metrics(self, instance_name):
        metrics = self.client.GetRewardMetrics(environment_pb2.GetRewardMetricsRequest(instance_name=instance_name))
        if not self.initiated:
            self.initiated = True
            self.start_score = 0.6 * metrics.tps + 0.4 * metrics.latency

        return metrics.latency, metrics.tps
    
    def get_action_state(self, instance_name, knobs):
        return self.client.GetActionState(environment_pb2.GetActionStateRequest(instance_name=instance_name, knobs=knobs))

