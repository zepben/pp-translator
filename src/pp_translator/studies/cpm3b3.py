import asyncio

import pandapower as pp
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, ConnectivityNode, \
    Junction, EnergyConsumer, Switch, Connector

from pp_translator.mappers.evolve_to_pandapower.mappings import EvolveToPandaPowerMap


def create_pp_model(network_service) -> pp.pandapowerNet:
    pp_net = pp.create_empty_network()
    return EvolveToPandaPowerMap(evolve_service=network_service, pp_net=pp_net).pp_net


async def main():
    feeder_mrid = "CPM3B3"
    rpc_port = 9014
    host_url = "ewb.essentialenergy.zepben.com"

    async with connect_async(host=host_url, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network_service = NetworkService()
        (await client.get_feeder(network_service, mrid=feeder_mrid)).throw_on_error()
        print(f'Connecting host: {host_url}, rpc_port: {rpc_port}')
        if network_service.get("CPM3B3").mrid == feeder_mrid:
            print(f'Successful connection to {host_url}, rpc port: {rpc_port}')
            print(f'NetworkService loaded for the Feeder mRID: {feeder_mrid}, with the following data:')
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
        else:
            raise Exception(f'Any Feeder was found with mRID {feeder_mrid}')
        pp_net = create_pp_model(network_service)
        print(pp_net)
        pp.to_sqlite(net=pp_net, filename='cpm3b3_pp_net.sqlite', include_results=False)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
