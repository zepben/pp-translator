import asyncio
from typing import List

from zepben.evolve import connect_async, PhotoVoltaicUnit

from utils.geojson_utils import to_geojson_feature_collection, write_geojson_file
from utils.utils import get_feeder_network


async def main():
    host = "ewb.zepben.com"
    rpc_port = 9014

    print("Connecting to Server")
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        print("Requesting Feeder")
        feeder_mrid = "CPM3B3"
        network = await get_feeder_network(channel, feeder_mrid)

        print("Processing Study")
        # noinspection PyTypeChecker
        pv_list: List[PhotoVoltaicUnit] = [pv for pv in network.objects(PhotoVoltaicUnit)]

        print("Writing Study")
        write_pv_penetration_study(pv_list)
        print("Write Completed")


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
