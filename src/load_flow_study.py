import asyncio
from typing import List, Dict, Set

import pandapower as pp
from geojson import Feature, LineString, Point, FeatureCollection
from zepben.evolve import connect_async, AcLineSegment, Feeder, NetworkService, EnergySource, \
    Terminal, PhaseCode, ConductingEquipment, PowerTransformer, \
    create_bus_branch_model, ErrorType, ErrorInfo

from pp_creators.creators import create_pp_line, create_pp_line_type, get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection, \
    create_pp_load_from_energy_consumer, create_pp_load_from_power_electronics_connection, create_pp_bus
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
        topological_island = _get_topological_island_and_fix_network_issues(network, network.get(feeder_mrid))

        print("Processing Study")
        result = create_bus_branch_model(
            lambda: topological_island,
            pp.create_empty_network,
            create_pp_bus,
            create_pp_line,
            create_pp_line_type,
            get_line_type_id,
            create_pp_transformer,
            create_pp_transformer_type,
            get_transformer_type_id,
            create_pp_grid_connection,
            create_pp_load_from_energy_consumer,
            create_pp_load_from_power_electronics_connection
        )

        if result.was_successful:
            # Run Diagnostics
            pp.diagnostic(result.bus_branch_model)

            # Run Load Flow
            pp.runpp(result.bus_branch_model)
            write_load_flow_result_to_geojson(result.bus_branch_model)
        else:
            # Log errors
            log_validation_errors(result.errors)


def log_validation_errors(errors: Dict[ErrorType, Set[ErrorInfo]]):
    print("Failed to generate bus-branch-model due to the following problems:")
    for error_type, errors in errors.items():
        print(f"The following objects failed the '{error_type.name}' validation:")
        for error in errors:
            print(f"{error.io.mrid}")


def write_load_flow_result_to_geojson(result: pp.pandapowerNet):
    bus_geodata: Dict = result.bus_geodata.to_dict()
    bus_results = result.res_bus.to_dict()

    line_geodata = result.line_geodata.to_dict()
    line_results = result.res_line.to_dict()

    bus_geojson = []
    line_geojson = []

    for idx in range(len(bus_geodata["x"])):
        x = bus_geodata["x"][idx]
        y = bus_geodata["y"][idx]
        vm_pu = bus_results["vm_pu"][idx]
        bus_geojson.append(
            Feature(
                idx,
                Point((x, y)),
                {
                    "name": idx,
                    "type": "bus",
                    "vm_pu": _format_decimal_value(vm_pu)
                }))

    for idx in range(len(line_geodata["coords"])):
        coords = line_geodata["coords"][idx]
        p_from_kw = line_results["p_from_mw"][idx] * 1000
        p_to_kw = line_results["p_to_mw"][idx] * 1000
        line_geojson.append(
            Feature(
                idx,
                LineString(coords),
                {
                    "name": idx,
                    "p_from_kw": _format_decimal_value(p_from_kw),
                    "p_to_kw": _format_decimal_value(p_to_kw)
                }))

    features = []
    for bus_f in bus_geojson:
        features.append(bus_f)

    for line_f in line_geojson:
        features.append(line_f)

    feature_collection = FeatureCollection(features)
    write_geojson_file("./load_flow_result.json", feature_collection)


def _write_load_flow_study(pts: List[ConductingEquipment]) -> None:
    class_to_properties = {
        ConductingEquipment: {"name": lambda ec: ec.name, "type": lambda x: "ec"},
        AcLineSegment: {"name": lambda ec: ec.name},
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    write_geojson_file("./feeder_for_load_flow.json", feature_collection)


def _format_decimal_value(f: float):
    return float("{:.4f}".format(f))


def _get_topological_island_and_fix_network_issues(network: NetworkService, feeder: Feeder) -> NetworkService:
    stuff_to_remove = []
    for f in network.objects(Feeder):
        if f.mrid != feeder.mrid:
            for eq in f.equipment:
                for t in eq.terminals:
                    if t.connectivity_node is not None:
                        stuff_to_remove.append(t.connectivity_node)
                    stuff_to_remove.append(t)
                stuff_to_remove.append(eq)
            stuff_to_remove.append(f)

    for s in stuff_to_remove:
        network.remove(s)

    bad_transformers = []
    for pt in network.objects(PowerTransformer):
        if len(list(pt.terminals)) != 2:
            bad_transformers.append(pt)

    for pt in bad_transformers:
        network.remove(pt)
        for t in pt.terminals:
            network.remove(t)

    # Add Energy Source
    es = EnergySource(mrid=f"{feeder.mrid}_es")
    es.base_voltage = feeder.normal_head_terminal.conducting_equipment.base_voltage
    es_t = Terminal(conducting_equipment=es, phases=PhaseCode.ABCN, sequence_number=1)
    es.add_terminal(es_t)
    network.add(es)
    network.add(es_t)
    network.connect_by_mrid(es_t, feeder.normal_head_terminal.connectivity_node_id)

    return network


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
