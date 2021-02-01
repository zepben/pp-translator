import asyncio
import sys
import getopt

from zepben.evolve import connect_async, NetworkConsumerClient
from zepben.evolve import NetworkService, Equipment


def print_feeder_eq(service):
    for equip in service.objects(Equipment):
        print(equip.mrid, equip.name, type(equip).__name__, equip.get_base_voltage())


async def main(argv):
    feeder_mrid = 'CPM3B3'
    rpc_port = 50052
    host = "localhost"
    try:
        opts, args = getopt.getopt(argv, "h:i:p:u:", ["mrid=", "port=", "host="])
    except getopt.GetoptError:
        print('get_feeder.py -i <feeder_mrid> -p <rpc_port> -u <host>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('get_feeder.py -p <rpc_port> -i <feeder_mrid>')
            sys.exit()
        elif opt in ("-i", "--mrid"):
            feeder_mrid = arg
        elif opt in ("-p", "--port"):
            rpc_port = arg
        elif opt in ("-u", "--host"):
            host = arg
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        result = (await client.retrieve_network()).throw_on_error()
        service = result.result.network_service
        print(feeder_mrid)
        print(service.get(feeder_mrid))
        print_feeder_eq(service)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv[1:]))