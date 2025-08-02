import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np

from environment.environment import Environment
from model.ddpg import DDPG
from replay_memory.replay_memory import PrioritizedReplayMemory

instance_name = "test"

# training DDPG model
class Trainer:
    def __init__(self, model : DDPG, environment : Environment, knobs):
        self.environment = environment
        self.model = model
        self.knobs = knobs

    @staticmethod
    def update_knob_values(knobs, actions, scale=0.1):
        updated_knobs = {}
        knob_keys = list(knobs.keys())

        for index, action in enumerate(actions):
            knob_key = knob_keys[index]
            knob_info = knobs[knob_key]
            current_val = knob_info['value']
            min_val = knob_info['min_value']
            max_val = knob_info['max_value']

            adjustment = (max_val - min_val) * action * scale
            new_value = current_val + adjustment
            new_value = max(min_val, min(max_val, new_value)) 
            
            if new_value > 0:
                new_value = round(new_value)

            updated_knobs[knob_key] = {
                'value': new_value,
                'min_value': min_val,
                'max_value': max_val
            }

        return updated_knobs



    def train(self, num_episodes, batch_size):
        fine_state_actions = []


        for episode in range(num_episodes):
            print(f"Episode {episode + 1}/{num_episodes}")

            self.environment.init_environment(instance_name)

            current_state = np.array(self.environment.get_states(instance_name))

            initial_latency, initial_tps = self.environment.get_reward_metrics(instance_name)

            previous_latency, previous_tps = initial_latency, initial_tps

            i = 0
            while i < 10:
                
                action = self.model.choose_action(current_state)

                knobs_to_set = self.update_knob_values(self.knobs, action)

                next_state, reward, ext_metrics = self.environment.step(instance_name=instance_name, knobs=knobs_to_set, initial_latency=initial_latency, initial_tps=initial_tps, previous_latency=previous_latency, previous_tps=previous_tps)


                self.model.add_sample(current_state, action, reward, np.array(next_state))
    
                current_state = next_state

                previous_tps, previous_latency = ext_metrics
                print(f"TPS: {ext_metrics[0]}, Latency: {ext_metrics[1]}")

                if i > 0:
                    self.model.update()

                if self.environment.perofrmance_increased:
                    self.knobs = knobs_to_set
                    print("Performance increased")
                i+=1


# # Example knobs and actions
# knobs = {
#     "autovacuum_max_workers": {"value": 3, "min_value": 1, "max_value": 262143},
#     "checkpoint_completion_target": {"value": 0.9, "min_value": 0, "max_value": 1},
#     "checkpoint_timeout": {"value": 300, "min_value": 30, "max_value": 86400},
#     "effective_cache_size": {"value": 524288, "min_value": 1, "max_value": 2.14748365e+09},
#     "maintenance_work_mem": {"value": 65536, "min_value": 1024, "max_value": 2.14748365e+09},
#     "max_connections": {"value": 100, "min_value": 1, "max_value": 262143},
#     "shared_buffers": {"value": 16384, "min_value": 16, "max_value": 1.07374182e+09},
#     "wal_buffers": {"value": 512, "min_value": -1, "max_value": 262143},
#     "wal_writer_delay": {"value": 200, "min_value": 1, "max_value": 10000},
#     "work_mem": {"value": 4096, "min_value": 60, "max_value": 2.14748365e+09}
# }

# # Example actions from the actor network
# actions = [0.01, 0.09, 0.05, 0.075, 0.03, 0.06, 0.02, 0.04, 0.08, 0.0001]

# # Update knobs based on actions
# updated_knobs = update_knob_values(knobs, actions)

# # Print updated knobs
# for key, value in updated_knobs.items():
#     print(f"{key}: {value['value']}")