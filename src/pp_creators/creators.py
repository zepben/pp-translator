#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from typing import FrozenSet, Tuple, Iterable, List

import pandapower as pp
from zepben.evolve import Terminal, NetworkService, AcLineSegment, PerLengthSequenceImpedance, WireInfo, \
    PowerTransformer, EnergySource, EnergyConsumer, BusBranchNetworkCreator, BusBranchNetworkCreationLogger, \
    PowerTransformerEnd, ConductingEquipment, PowerElectronicsConnection

__all__ = ["PandaPowerNetworkCreator"]


class PandaPowerNetworkCreator(BusBranchNetworkCreator[pp.pandapowerNet, int, int, int, int, int, int, int, int]):

    def bus_branch_network_creator(self, node_breaker_network: NetworkService) -> pp.pandapowerNet:
        return pp.create_empty_network()

    def topological_node_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            base_voltage: int,
            collapsed_conducting_equipment: FrozenSet[ConductingEquipment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        bus_idx = pp.create_bus(
            bus_branch_network,
            vn_kv=base_voltage / 1000,
            name=f"bus_{_create_id_from_terminals(border_terminals)}"
        )
        return bus_idx, bus_idx

    def topological_branch_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            connected_topological_nodes: Tuple[int, int],
            length: float,
            topological_branch_type: str,
            collapsed_ac_line_segments: FrozenSet[AcLineSegment],
            border_terminals: FrozenSet[Terminal],
            inner_terminals: FrozenSet[Terminal],
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        line_idx = pp.create_line(
            bus_branch_network,
            from_bus=connected_topological_nodes[0],
            to_bus=connected_topological_nodes[1],
            length_km=length / 1000,
            name=f"line_{_create_id_from_terminals(border_terminals)}",
            std_type=topological_branch_type
        )
        return line_idx, line_idx

    def topological_branch_type_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            per_length_sequence_impedance: PerLengthSequenceImpedance,
            wire_info: WireInfo,
            base_voltage: int,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[str, str]:
        # TODO: This needs to be implemented properly to generate a unique key for each line type
        return "NAYY 4x50 SE", "NAYY 4x50 SE"

    def power_transformer_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_transformer: PowerTransformer,
            ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, int]],
            power_transformer_type: str,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        tx_idx = pp.create_transformer(
            bus_branch_network,
            hv_bus=ends_to_topological_nodes[0][1],
            lv_bus=ends_to_topological_nodes[1][1],
            std_type=power_transformer_type,
            name=power_transformer.name
        )
        return tx_idx, tx_idx

    def power_transformer_type_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_transformer: PowerTransformer,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[str, str]:
        # TODO: This needs to be implemented properly to generate a unique key for each transformer type
        return "0.4 MVA 20/0.4 kV", "0.4 MVA 20/0.4 kV"

    def energy_source_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            energy_source: EnergySource,
            connected_topological_node: int,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        ext_grid_idx = pp.create_ext_grid(
            bus_branch_network,
            bus=connected_topological_node,
            vm_pu=1.02,
            name=energy_source.name
        )
        return ext_grid_idx, ext_grid_idx

    def energy_consumer_creator(
            self, bus_branch_network: pp.pandapowerNet,
            energy_consumer: EnergyConsumer,
            connected_topological_node: int,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        load_idx = pp.create_load(
            bus_branch_network,
            bus=connected_topological_node,
            p_mw=energy_consumer.p / 1000000,
            q_mvar=energy_consumer.q / 1000000,
            name=energy_consumer.name
        )
        return load_idx, load_idx

    def power_electronics_connection_creator(
            self,
            bus_branch_network: pp.pandapowerNet,
            power_electronics_connection: PowerElectronicsConnection,
            connected_topological_node: int,
            node_breaker_network: NetworkService,
            logger: BusBranchNetworkCreationLogger
    ) -> Tuple[int, int]:
        load_idx = pp.create_load(
            bus_branch_network,
            bus=connected_topological_node,
            p_mw=power_electronics_connection.p / 1000000,
            q_mvar=power_electronics_connection.q / 1000000,
            name=power_electronics_connection.name
        )
        return load_idx, load_idx


def _create_id_from_terminals(ts: Iterable[Terminal]):
    "_".join(sorted((t.mrid for t in ts)))
    pass
