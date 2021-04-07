import asyncio
from typing import List

from zepben.evolve import connect_async, PhotoVoltaicUnit, NetworkService, EnergyConsumer, PowerElectronicsUnit, \
    ConductingEquipment, PhaseCode, Terminal, PowerElectronicsConnection

from utils.geojson_utils import to_geojson_feature_collection, write_geojson_file
from utils.utils import get_feeder_network


async def main():
    host = "localhost"
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
    write_geojson_file("../demo/pv_penetration/pv_penetration_result.json", feature_collection)


def get_rated_s_property(pv: PhotoVoltaicUnit):
    return pv.power_electronics_connection.rated_s / 1000


def add_more_pv(network: NetworkService):
    for ec in network.objects(EnergyConsumer):
        connected_equipment = [o_t.conducting_equipment for t in ec.terminals for o_t in t.connectivity_node.terminals
                               if o_t != t]
        connected_pv = [eq for eq in connected_equipment if isinstance(eq, PowerElectronicsConnection)]

        if len(connected_pv) == 0:
            cnn = next(ec.terminals).connectivity_node
            pec = PowerElectronicsConnection(mrid=ec.mrid + "_pec_new", rated_s=2)
            pec.location = ec.location
            network.add(pec)
            pvu = PhotoVoltaicUnit(mrid=pec.mrid + "_pv")
            pvu.location = ec.location
            network.add(pvu)
            pvu.power_electronics_connection = pec
            pec.add_unit(pvu)
            terminal = create_terminals(network, pec, 1)[0]
            network.connect_by_mrid(terminal, cnn.mrid)


def create_terminals(network: NetworkService, ce: ConductingEquipment, num_terms: int,
                     phases: PhaseCode = PhaseCode.ABCN) -> List[Terminal]:
    terms = []
    for i in range(1, num_terms + 1):
        term = Terminal(mrid=f"{ce.mrid}_t{i}", conducting_equipment=ce, phases=phases, sequence_number=i)
        ce.add_terminal(term)
        assert network.add(term)
        terms.append(term)

    return terms


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
