import asyncio
from typing import List

from zepben.evolve import connect_async, NetworkConsumerClient, NetworkService, PhotoVoltaicUnit

from geojson_utils import to_geojson_feature_collection, write_geojson_file


async def main():
    feeder_mrid = "CPM3B3"
    host = "ewb.zepben.com"
    rpc_port = 9014

    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        client = NetworkConsumerClient(channel)
        network = NetworkService()
        (await client.get_feeder(network, mrid=feeder_mrid)).throw_on_error()
        # noinspection PyTypeChecker
        pv_list: List[PhotoVoltaicUnit] = [pv for pv in network.objects(PhotoVoltaicUnit)]
        write_pv_penetration_study(pv_list)


def write_pv_penetration_study(pvs: List[PhotoVoltaicUnit]) -> None:
    class_to_properties = {
        PhotoVoltaicUnit: {"rated_s": get_rated_s_property}
    }
    feature_collection = to_geojson_feature_collection(pvs, class_to_properties)
    write_geojson_file("../csiro/pv-penetration/pv_penetration_study.json", feature_collection)


def get_rated_s_property(pv: PhotoVoltaicUnit):
    return pv.power_electronics_connection.rated_s


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
