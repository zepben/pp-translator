import asyncio
from typing import Dict

import pandapower as pp
from geojson import Feature, LineString, Point, FeatureCollection
from zepben.evolve import connect_async, Feeder, NetworkService, EnergySource, \
    Terminal, PhaseCode, PowerTransformer, \
    create_bus_branch_model, PowerElectronicsConnection, PowerElectronicsUnit

from pp_creators.creators import create_pp_bus, create_pp_line, create_pp_line_type, get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection, \
    create_pp_load_from_energy_consumer, create_pp_load_from_power_electronics_connection
from utils.geojson_utils import write_geojson_file
from utils.utils import get_feeder_network


async def main():
    host = "ewb.essentialenergy.zepben.com"
    rpc_port = 9014

    print("Connecting to Server")
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        print("Requesting Feeder")
        feeder_mrid = "CPM3B3"
        network = await get_feeder_network(channel, feeder_mrid)

        print("Running Load Flow")
        pp_model = create_pp_network(network, network.get(feeder_mrid))
        pp.runpp(pp_model)

        print("Writing Results")
        write_load_flow_result_to_geojson(pp_model)


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
                    "vm_pu": num_of_decimals(vm_pu, 3)
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
                    "p_from_kw": num_of_decimals(p_from_kw, 3),
                    "p_to_kw": num_of_decimals(p_to_kw, 3)
                }))

    features = []
    for bus_f in bus_geojson:
        features.append(bus_f)

    for line_f in line_geojson:
        features.append(line_f)

    feature_collection = FeatureCollection(features)
    write_geojson_file("../demo/load_flow/load_flow_result.json", feature_collection)


def num_of_decimals(f: float, num: int):
    return float(("{:." + str(num) + "f}").format(f))


def create_pp_network(network: NetworkService, feeder: Feeder) -> pp.pandapowerNet:
    preprocess(network, feeder)

    result = create_bus_branch_model(
        network,
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
    return result.bus_branch_model


def preprocess(network: NetworkService, feeder: Feeder):
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

    power_electronics_connections = []
    for pec in network.objects(PowerElectronicsConnection):
        power_electronics_connections.append(pec)

    for pec in power_electronics_connections:
        for t in pec.terminals:
            for other_t in t.connectivity_node.terminals:
                other_t.conducting_equipment.remove_terminal(other_t)
                try:
                    network.remove(other_t.connectivity_node)
                except:
                    pass
                network.remove(other_t)
        network.remove(pec)

    power_electronics_units = []
    for peu in network.objects(PowerElectronicsUnit):
        power_electronics_units.append(peu)

    for peu in power_electronics_units:
        network.remove(peu)

    # Add Energy Source
    es = EnergySource(mrid=f"{feeder.mrid}_es")
    es.base_voltage = feeder.normal_head_terminal.conducting_equipment.base_voltage
    es_t = Terminal(conducting_equipment=es, phases=PhaseCode.ABCN, sequence_number=1)
    es.add_terminal(es_t)
    network.add(es)
    network.add(es_t)
    network.connect_by_mrid(es_t, feeder.normal_head_terminal.connectivity_node_id)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
