import asyncio
from typing import List

from zepben.evolve import connect_async, AcLineSegment

from utils.geojson_utils import to_geojson_feature_collection, write_geojson_file
from utils.utils import get_feeder_network


async def main():
    feeder_mrid = "CPM3B3"
    host = "ewb.zepben.com"
    rpc_port = 9014

    print("Connecting to Server")
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        print("Requesting Feeder")
        network = await get_feeder_network(channel, feeder_mrid)

        print("Processing Study")
        long_acls = list()
        for io in network.objects(AcLineSegment):
            # noinspection PyTypeChecker
            acls: AcLineSegment = io
            if acls.length > 100:
                long_acls.append(acls)

        print("Writing Study")
        write_line_length_study(long_acls)
        print("Write Completed")


def write_line_length_study(acls: List[AcLineSegment]) -> None:
    class_to_properties = {
        AcLineSegment: {
            "length": get_length_property,
            "name": get_name_property
        }
    }
    feature_collection = to_geojson_feature_collection(acls, class_to_properties)
    write_geojson_file("../csiro/acls-length/line_length_study.json", feature_collection)


def get_length_property(acls: AcLineSegment):
    return round(acls.length)


def get_name_property(acls: AcLineSegment):
    return acls.name


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
