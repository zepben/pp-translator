import asyncio

import pandapower as pp
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, Disconnector
from zepben.evolve import create_bus_branch_model

from pp_translator.pp_creators.creators import create_pp_bus, create_pp_line, create_pp_load, create_pp_line_type, \
    get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection


def get_panda_power_network(node_breaker_model):
    panda_power_network = create_bus_branch_model(
        node_breaker_model,
        pp.create_empty_network,
        create_pp_bus,
        create_pp_line,
        create_pp_line_type,
        get_line_type_id,
        create_pp_transformer,
        create_pp_transformer_type,
        get_transformer_type_id,
        create_pp_grid_connection,
        create_pp_load
    )
    return panda_power_network


async def main():
    feeder_mrid = "CPM3B3"
    rpc_port = 9014
    server_url = "ewb.essentialenergy.zepben.com"

    async with connect_async(host=server_url, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network_service = NetworkService()
        (await client.get_feeder(network_service, mrid=feeder_mrid)).throw_on_error()
        print(network_service.get(feeder_mrid))
        result = get_panda_power_network(network_service)
        errors = result.errors
        pp_net = result.bus_branch_model
        print(errors)
        print(pp_net)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
