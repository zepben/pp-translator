from pp_translator.studies.utils import create_pp_model
import pandapower as pp

from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, ConnectivityNode, \
    Junction, EnergyConsumer, Switch, Connector, PowerTransformer, PowerTransformerEnd, Conductor
from pp_translator.studies.utils import SimpleNodeBreakerFeeder, network_service_inventory
from pp_translator.mappers.evolve_to_pandapower.mappings import EvolveToPandaPowerMap

network_service = SimpleNodeBreakerFeeder().network_service
list_tx = list(network_service.objects(PowerTransformer))
tx: PowerTransformer = list_tx[0]
list_ends = list(tx.ends)
list_ends[0].rated_u = 20000
end2 = PowerTransformerEnd()
end2.rated_u = 400
end2.power_transformer = tx
network_service.add(end2)

# NetworkService Inventory
network_service_inventory(network_service)

# Create PandaPower model
pp_net = pp.create_empty_network()
EvolveToPandaPowerMap(network_service=network_service, pp_net=pp_net)
pp.to_sqlite(net=pp_net, filename='example_pp_net.sqlite', include_results=False)
