import asyncio

import pandapower as pp
from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService


async def main():
    feeder_mrid = "CPM3B3"
    host = "localhost"
    rpc_port = 9014

    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network = NetworkService()
        result = (await client.get_feeder(network, mrid=feeder_mrid)).throw_on_error()

        pp_network = pp.create_empty_network()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
