import asyncio
from typing import List, Dict

from zepben.evolve import connect_async, Traversal, \
    PowerTransformer, ConductingEquipment, EnergyConsumer, LifoQueue, SinglePhaseKind, PhaseDirection, AcLineSegment

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
        transformer_to_eq: Dict[str, List[EnergyConsumer]] = {}
        for io in (pt for pt in network.objects(PowerTransformer)):
            pt: PowerTransformer = io
            downstream_consumers = await get_downstream_eq(pt)
            transformer_to_eq[pt.mrid] = downstream_consumers

        eqs = [eq for (k, eq_list) in transformer_to_eq.items() for eq in eq_list]

        print("Writing Study")
        write_tracing_customers_study(eqs, transformer_to_eq)
        print("Write Completed")


def write_tracing_customers_study(pts: List[PowerTransformer],
                                  transformer_to_consumers: Dict[str, List[EnergyConsumer]]) -> None:
    class_to_properties = {
        EnergyConsumer: {"name": lambda ec: ec.name, "type": lambda x: "ec"},
        PowerTransformer: {"consumer_count": consumer_count_from(transformer_to_consumers), "type": lambda x: "pt"},
        AcLineSegment: {"name": lambda ec: ec.name},
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    write_geojson_file("../csiro/tracing-consumers/tracing_consumers_study.json", feature_collection)


async def get_downstream_eq(ce: ConductingEquipment) -> List[EnergyConsumer]:
    eqs: List[ConductingEquipment] = []
    trace = Traversal(
        start_item=ce,
        queue_next=queue_downstream_equipment,
        process_queue=LifoQueue(),
        step_actions=[collect_eq_in(eqs)]
    )
    await trace.trace()
    return eqs


def queue_downstream_equipment(ce: ConductingEquipment, exclude=None):
    downstream_equipment = []
    for t in ce.terminals:

        for ot in t.connectivity_node.terminals:
            is_downstream = ot.traced_phases.direction_normal(SinglePhaseKind.A).has(PhaseDirection.IN)
            if ot != t and is_downstream:
                downstream_equipment.append(ot.conducting_equipment)

    return downstream_equipment


def collect_eq_in(collection: List[EnergyConsumer]):
    async def add_eq(ce, _):
        collection.append(ce)

    return add_eq


def consumer_count_from(pt_to_eq: Dict[str, List[ConductingEquipment]]):
    def fun(pt: PowerTransformer):
        downstream_eq = pt_to_eq.get(pt.mrid)
        consumers = [eq for eq in downstream_eq if isinstance(eq, EnergyConsumer)]
        return len(consumers) if consumers is not None else 0

    return fun


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
