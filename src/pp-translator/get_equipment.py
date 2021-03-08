import asyncio
import argparse

from zepben.evolve import connect_async, NetworkConsumerClient
from zepben.evolve import NetworkService, Terminal


def get_terminals(eq):
    print(f'Equipment Terminals:')
    for i in range(1, eq.num_terminals() + 1):
        t: Terminal = eq.get_terminal_by_sn(i)
        print(f'Terminal: {t}: {t.phases}')



async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='Host and port of grpc server', default="localhost")
    parser.add_argument('--rpc-port', help='The gRPC port for the serve', default="50052")
    parser.add_argument('--eq-mrid', help='The mRID of the Equipment', default="")
    parser.add_argument('--feeder-mrid', help='The mRID of the Feeder', default="")
    args = parser.parse_args()

    async with connect_async(host=args.host, rpc_port=args.rpc_port) as channel:
        service = NetworkService()
        client = NetworkConsumerClient(channel)
        result = (await client.get_identified_object(service, args.eq_mrid)).throw_on_error()
        print(args.eq_mrid)
        eq = service.get(args.eq_mrid)
        print(f'Equipment name: {eq.name}, Base Voltage: {eq.base_voltage}')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
