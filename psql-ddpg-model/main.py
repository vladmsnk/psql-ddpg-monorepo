import environment.environment as env
# from replay_memory.replay_memory import PrioritizedReplayMemory

from model.ddpg import DDPG
from train.train import Trainer

host = "localhost"
port = 7003
instance_name = "test"

knobs = [
    "work_mem",
    "maintenance_work_mem",
    "checkpoint_completion_target",
    "effective_cache_size",
    "wal_writer_delay",
    "checkpoint_timeout"
]

batchSize = 1



def fromDescKnobs(descKnobs):
    knobs = {}

    for knob in descKnobs:
        knobs[knob.name] = {"min_value" : knob.min_value, "max_value" : knob.max_value, "value" : knob.value}


    knobNames = list(knobs.keys())
    knobNames.sort()
    sortedKnobs = {i: knobs[i] for i in knobNames}
    return sortedKnobs


if __name__ == '__main__':
    client = env.RunClient(host=host, port=port)
    environment = env.Environment(client=client)

    actionState = environment.get_action_state(instance_name=instance_name, knobs=knobs)
    actions = fromDescKnobs(actionState.knobs)

    metricState = environment.get_states(instance_name=instance_name)
    states = list(metricState)

    modelOpts = {
        "alr": 0.001,
        "clr": 0.001,
        "model" : "ddpg",
        "batch_size" : batchSize,
        "gamma" : 0.99,
        "tau" : 0.001,
        "memory_size" : 10000,
    }

    model = DDPG(len(states), len(actions))

    trainer = Trainer(model, environment, actions)
    trainer.train(1, batchSize)
