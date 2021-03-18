from pp_translator.studies.utils import create_pp_model
import pandapower as pp

from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, ConnectivityNode, \
    Junction, EnergyConsumer, Switch, Connector, PowerTransformer, PowerTransformerEnd
from pp_translator.studies.utils import SimpleNodeBreakerFeeder
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

cns_count = len(list(network_service.objects(ConnectivityNode)))
print(f' - Connectivity Nodes: {cns_count}')
connector_count = len(list(network_service.objects(Connector)))
print(f' - Connectors: {connector_count}')
list_sw = list(network_service.objects(Switch))
sw_count = len(list_sw)
print(f' - Switches: {sw_count}')
closed_switches_count = len([sw for sw in list_sw if sw.is_open() is False])
print(f' - Closed Switches: {closed_switches_count}')
print(f' - ConnectivityNodes + Junctions + closed Switches: '
      f'{connector_count + cns_count + closed_switches_count}')
print(f' - Energy Consumers: {len(list(network_service.objects(EnergyConsumer)))}')

pp_net = pp.create_empty_network()
evolve_mapping = EvolveToPandaPowerMap(evolve_service=network_service, pp_net=pp_net)
bus_df = evolve_mapping.conductors_to_lines()
cn_mrid = bus_df[bus_df['name'] == 'cn1'].name.values
print(cn_mrid)
