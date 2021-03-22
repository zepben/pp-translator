import asyncio
from typing import List

import pandapower as pp
from zepben.evolve import connect_async, AcLineSegment, Feeder, NetworkService, EnergySource, \
    Terminal, PhaseCode, PhaseDirection, SinglePhaseKind, ConductingEquipment, PowerTransformer, create_bus_branch_model

from pp_creators.creators import create_pp_bus, create_pp_line, create_pp_line_type, get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection, \
    create_pp_load
from utils.geojson_utils import to_geojson_feature_collection, write_geojson_file
from utils.utils import get_feeder_network


async def main():
    host = "ewb.essentialenergy.zepben.com"
    rpc_port = 9014

    print("Connecting to Server")
    async with connect_async(host=host, rpc_port=rpc_port) as channel:
        print("Requesting Feeder")
        feeder_mrid = "CPM3B3"
        network = await get_feeder_network(channel, feeder_mrid)

        stuff_to_remove = []
        for f in network.objects(Feeder):
            if f.mrid != feeder_mrid:
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

        _add_energy_source(network, network.get("CPM3B3"))

        # write_load_flow_study([eq for eq in network.objects(ConductingEquipment)])

        print("Processing Study")
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
            create_pp_load
        )

        # Run Load Flow
        diagnostic = pp.diagnostic(result.bus_branch_model)
        print(diagnostic)

        pp.runpp(result.bus_branch_model)
        pp_network = result.bus_branch_model


def write_load_flow_study(pts: List[ConductingEquipment]) -> None:
    class_to_properties = {
        ConductingEquipment: {"name": lambda ec: ec.name, "type": lambda x: "ec"},
        AcLineSegment: {"name": lambda ec: ec.name},
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    write_geojson_file("./feeder_for_load_flow.json", feature_collection)


def _add_energy_source(network: NetworkService, feeder: Feeder):
    # Add Energy Source
    es = EnergySource(mrid=f"{feeder.mrid}_es")
    es.base_voltage = feeder.normal_head_terminal.conducting_equipment.base_voltage
    es_t = Terminal(conducting_equipment=es, phases=PhaseCode.ABCN, sequence_number=1)
    es.add_terminal(es_t)
    network.add(es)
    network.add(es_t)
    network.connect_by_mrid(es_t, feeder.normal_head_terminal.connectivity_node_id)


def _create_feeder_network(feeder: Feeder):
    network = NetworkService()

    # for eq in feeder.equipment:
    #     for t in eq.terminals:
    #         t.traced_phases = TracedPhases()
    for eq in feeder.equipment:
        is_off_supply = True
        for t in eq.terminals:
            direction = t.traced_phases.direction_normal(SinglePhaseKind.A)
            is_off_supply = direction.has(PhaseDirection.NONE)
            if not is_off_supply:
                network.add(t.connectivity_node)
                network.add(t)
        if not is_off_supply:
            network.add(eq)

    # Add Energy Source
    es = EnergySource(mrid=f"{feeder.mrid}_es")
    es.base_voltage = feeder.normal_head_terminal.conducting_equipment.base_voltage
    es_t = Terminal(conducting_equipment=es, phases=PhaseCode.ABCN, sequence_number=1)
    es.add_terminal(es_t)
    network.add(es)
    network.add(es_t)
    network.connect_by_mrid(es_t, feeder.normal_head_terminal.connectivity_node_id)

    # disconnected_eq = []
    # for eq in network.objects(ConductingEquipment):
    #     terminals = list(eq.terminals)
    #
    #     for t in terminals:
    #         direction = t.traced_phases.direction_normal(SinglePhaseKind.A)
    #         is_off_supply = direction.has(PhaseDirection.NONE)
    #         if is_off_supply:
    #             disconnected_eq.append(eq)

    return network


def get_length_property(acls: AcLineSegment):
    return round(acls.length)


def get_name_property(acls: AcLineSegment):
    return acls.name


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
