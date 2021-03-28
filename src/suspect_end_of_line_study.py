import asyncio
from typing import List, Dict, Tuple

from zepben.evolve import connect_async, Traversal, \
    PowerTransformer, ConductingEquipment, EnergyConsumer, LifoQueue, SinglePhaseKind, PhaseDirection, AcLineSegment, \
    Junction

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
        transformer_to_eq: Dict[str, List[ConductingEquipment]] = {}
        for io in (pt for pt in network.objects(PowerTransformer)):
            pt: PowerTransformer = io
            downstream_equipment = await get_downstream_eq(pt)
            transformer_to_eq[pt.mrid] = downstream_equipment

        transformer_to_suspect_end = await get_transformer_to_suspect_end(transformer_to_eq)
        all_traced_equipment = [eq for (k, (count, sus_eq_list)) in transformer_to_suspect_end.items() for eq in
                                sus_eq_list]

        print("Writing Study")
        write_suspect_end_of_line_study(all_traced_equipment, transformer_to_suspect_end)
        print("Write Completed")


async def get_transformer_to_suspect_end(transformer_to_eq: Dict[str, List[ConductingEquipment]]) \
        -> Dict[str, Tuple[int, List[ConductingEquipment]]]:
    transformer_to_suspect_end: Dict[str, (int, List[ConductingEquipment])] = {}
    for pt_mrid, eq_list in transformer_to_eq.items():
        single_terminal_junctions = [eq for eq in eq_list if isinstance(eq, Junction) and len(list(eq.terminals)) == 1]

        upstream_eq = set()
        for stj in single_terminal_junctions:
            upstream_eq_up_to_pt = await get_upstream_eq_up_to_transformer(stj)
            upstream_eq.update(upstream_eq_up_to_pt)

        transformer_to_suspect_end[pt_mrid] = (len(single_terminal_junctions), list(upstream_eq))

    return transformer_to_suspect_end


def write_suspect_end_of_line_study(pts: List[PowerTransformer],
                                    transformer_to_suspect_end: Dict[
                                        str, Tuple[int, List[ConductingEquipment]]]) -> None:
    class_to_properties = {
        EnergyConsumer: {
            "name": lambda ec: ec.name,
            "type": lambda x: "ec"
        },
        PowerTransformer: {
            "consumer_count": suspect_end_count_from(transformer_to_suspect_end),
            "type": lambda x: "pt"
        },
        AcLineSegment: {"name": lambda ec: ec.name},
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    write_geojson_file("../demo/suspect_end_of_line/suspect_end_of_line_result.json", feature_collection)


async def get_downstream_eq(ce: ConductingEquipment) -> List[ConductingEquipment]:
    eqs: List[ConductingEquipment] = []
    trace = Traversal(
        start_item=ce,
        queue_next=queue_downstream_equipment,
        process_queue=LifoQueue(),
        step_actions=[collect_eq_in(eqs)]
    )
    await trace.trace()
    return eqs


async def get_upstream_eq_up_to_transformer(ce: ConductingEquipment) -> List[ConductingEquipment]:
    eqs: List[ConductingEquipment] = []
    trace = Traversal(
        start_item=ce,
        queue_next=queue_upstream_equipment,
        process_queue=LifoQueue(),
        step_actions=[collect_eq_in(eqs)],
        stop_conditions=[is_transformer]
    )
    await trace.trace()
    return eqs


async def is_transformer(ce: ConductingEquipment):
    return isinstance(ce, PowerTransformer)


def queue_downstream_equipment(ce: ConductingEquipment, exclude=None):
    downstream_equipment = []
    for t in ce.terminals:

        for ot in t.connectivity_node.terminals:
            is_downstream = ot.traced_phases.direction_normal(SinglePhaseKind.A).has(PhaseDirection.IN)
            if ot != t and is_downstream:
                downstream_equipment.append(ot.conducting_equipment)

    return downstream_equipment


def queue_upstream_equipment(ce: ConductingEquipment, exclude=None):
    upstream_equipment = []
    for t in ce.terminals:

        for ot in t.connectivity_node.terminals:
            is_upstream = ot.traced_phases.direction_normal(SinglePhaseKind.A).has(PhaseDirection.OUT)
            if ot != t and is_upstream:
                upstream_equipment.append(ot.conducting_equipment)

    return upstream_equipment


def collect_eq_in(collection: List[ConductingEquipment]):
    async def add_eq(ce, _):
        collection.append(ce)

    return add_eq


def suspect_end_count_from(pt_to_sus_end: Dict[str, Tuple[int, List[ConductingEquipment]]]):
    def fun(pt: PowerTransformer):
        count, suspect_eq = pt_to_sus_end.get(pt.mrid)
        return count if count else 0

    return fun


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
