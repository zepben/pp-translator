import asyncio

import pandapower as pp
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, Disconnector


async def main():
    feeder_mrid = "CPM3B3"
    rpc_port = 9014
    host_url = "ewb.essentialenergy.zepben.com"

    async with connect_async(host=host_url, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network_service = NetworkService()
        (await client.get_feeder(network_service, mrid=feeder_mrid)).throw_on_error()
        print(network_service.get("CPM3B3"))
        if network_service.get("CPM3B3").mrid == "CPM3B3":
            print(f'Successful connection to {host_url}, rpc port: {rpc_port}')
            print(f'NetworkService loaded for Feeder mRID: {feeder_mrid}')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())