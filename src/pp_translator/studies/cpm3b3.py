import asyncio

from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService

from pp_translator.studies.utils import network_service_inventory


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
            # NetworkService Inventory
            network_service_inventory(network_service)
        else:
            raise Exception(f'Any Feeder was found with mRID {feeder_mrid}')
        print(network_service.get("node22959534"))
        # TODO: Check disconnected sections connected to juctions such as Junction{node22959534|15084A}
        # # Create PandaPower model
        # pp_net = pp.create_empty_network()
        # EvolveToPandaPowerMap(network_service=network_service, pp_net=pp_net, feeder_mrid=feeder_mrid)
        # print(pp.diagnostic(pp_net))
        # pp.to_sqlite(net=pp_net, filename='cpm3b3_pp_net.sqlite', include_results=False)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
