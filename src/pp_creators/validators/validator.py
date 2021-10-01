#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import logging
from typing import List, Tuple, Optional, FrozenSet

import pandapower as pp
from zepben.evolve import BusBranchNetworkCreationValidator, NetworkService, EnergyConsumer, EnergySource, \
    PowerTransformer, PowerTransformerEnd, AcLineSegment, Terminal, \
    ConductingEquipment, PowerElectronicsConnection

from pp_creators.utils import get_upstream_end_to_tns

__all__ = ["PandaPowerNetworkValidator"]


class PandaPowerNetworkValidator(BusBranchNetworkCreationValidator[pp.pandapowerNet, int, int, int, int, int, int]):
    logger: logging.Logger

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def is_valid_network_data(self, node_breaker_network: NetworkService) -> bool:
        return True

    def is_valid_topological_node_data(self, bus_branch_network: pp.pandapowerNet, base_voltage: Optional[int],
                                       collapsed_conducting_equipment: FrozenSet[ConductingEquipment],
                                       border_terminals: FrozenSet[Terminal], inner_terminals: FrozenSet[Terminal],
                                       node_breaker_network: NetworkService) -> bool:
        if base_voltage is None:
            self.logger.error(
                f"Cannot create bus due to missing base voltage: {_format_topological_node(base_voltage, border_terminals, inner_terminals, collapsed_conducting_equipment)}")
            return False
        return True

    def is_valid_topological_branch_data(self, bus_branch_network: pp.pandapowerNet,
                                         connected_topological_nodes: Tuple[int, int],
                                         length: Optional[float], collapsed_ac_line_segments: FrozenSet[AcLineSegment],
                                         border_terminals: FrozenSet[Terminal], inner_terminals: FrozenSet[Terminal],
                                         node_breaker_network: NetworkService) -> bool:
        if length is None:
            self.logger.error(
                f"Cannot create branch due to missing length: {[acls.mrid for acls in collapsed_ac_line_segments]}")
            return False
        if length == 0:
            self.logger.warning(f"Branch with total length of 0: {[acls.mrid for acls in collapsed_ac_line_segments]}")
        return True

    def is_valid_power_transformer_data(self, bus_branch_network: pp.pandapowerNet, power_transformer: PowerTransformer,
                                        ends_to_topological_nodes: List[Tuple[PowerTransformerEnd, Optional[int]]],
                                        node_breaker_network: NetworkService) -> bool:
        has_single_upstream_connection = len(get_upstream_end_to_tns(ends_to_topological_nodes)) == 1
        if not has_single_upstream_connection:
            self.logger.error(
                f"PowerTransformer '{power_transformer.name}' doesn't have a single upstream connection to a network")
            return False
        return True

    def is_valid_energy_source_data(self, bus_branch_network: pp.pandapowerNet, energy_source: EnergySource,
                                    connected_topological_node: int, node_breaker_network: NetworkService) -> bool:
        return True

    def is_valid_energy_consumer_data(self, bus_branch_network: pp.pandapowerNet, energy_consumer: EnergyConsumer,
                                      connected_topological_node: int, node_breaker_network: NetworkService) -> bool:
        return True

    def is_valid_power_electronics_connection_data(self, bus_branch_network: pp.pandapowerNet,
                                                   power_electronics_connection: PowerElectronicsConnection,
                                                   connected_topological_node: int,
                                                   node_breaker_network: NetworkService) -> bool:
        return True


def _format_topological_node(base_voltage: Optional[int],
                             border_terminals: FrozenSet[Terminal],
                             inner_terminals: FrozenSet[Terminal],
                             collapsed_conducting_equipment: FrozenSet[ConductingEquipment]) -> str:
    return "{ " \
           f"base_voltage: {base_voltage}, " \
           f"inner_terminals: {inner_terminals}, " \
           f"border_terminals: {border_terminals}, " \
           f"collapsed_equipment: {collapsed_conducting_equipment} " \
           "}"
