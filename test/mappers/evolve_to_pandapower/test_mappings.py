import pandapower as pp
from zepben.evolve import NetworkService

from pp_translator.mappers.evolve_to_pandapower.mappings import EvolveToPandaPowerMap


def test_conductors_to_lines():
    network_service = NetworkService()
    net = pp.create_empty_network()
    EvolveToPandaPowerMap(pp_net=net, evolve_service=network_service)
