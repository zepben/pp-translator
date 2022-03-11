#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

# Refer to pandapower's simple 3-bus network example: https://www.pandapower.org/start/#a-short-introduction-

# TODO: This test cannot run until the python sdk setPhases() method is fixed
# @pytest.mark.asyncio
# async def test_create_pp_bus_branch_model(simple_node_breaker_network):
#     node_breaker_model = simple_node_breaker_network
#     creator = PandaPowerNetworkCreator(vm_pu=1.02, logger=logging.getLogger())
#     result = await creator.create(node_breaker_model)
#
#     assert result.was_successful
#
#     # Run Load Flow
#     pp.diagnostic(result.network)
#     pp.runpp(result.network)
#     pp_network = result.network
#
#     # Determine busses
#     tx_terminals_sorted_by_voltage = [end.terminal
#                                       for end in sorted(list(simple_node_breaker_network.get("transformer").ends),
#                                                         key=lambda x: x.rated_u,
#                                                         reverse=True)]
#     tx_hv_terminal = tx_terminals_sorted_by_voltage[0]
#     tx_lv_terminal = tx_terminals_sorted_by_voltage[1]
#     tx_hv_bus = next(iter(result.mappings.to_bbn.objects[tx_hv_terminal.mrid]))
#     tx_lv_bus = next(iter(result.mappings.to_bbn.objects[tx_lv_terminal.mrid]))
#     load_bus = next(
#         iter(result.mappings.to_bbn.objects[list(simple_node_breaker_network.get("load").terminals)[0].mrid]))
#
#     # Validate and Log
#     validate_pp_load_flow_results(
#         pp_network.bus,
#         [
#             {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
#             {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
#             {"name": "bus_None", "vn_kv": 20.0, "type": "b", "in_service": True, "zone": None},
#         ],
#         "bus",
#         log=True
#     )
#
#     validate_pp_load_flow_results(
#         pp_network.trafo,
#         [
#             {
#                 "name": "Transformer",
#                 "std_type": "0.4 MVA 20/0.4 kV",
#                 "hv_bus": tx_hv_bus,
#                 "lv_bus": tx_lv_bus,
#                 "sn_mva": 0.4,
#                 "vn_hv_kv": 20.0,
#                 "vn_lv_kv": 0.4,
#                 "vk_percent": 6.0,
#                 "vkr_percent": 1.425,
#                 "pfe_kw": 1.35,
#                 "i0_percent": 0.3375,
#                 "shift_degree": 150,
#                 "tap_side": "hv",
#                 "tap_neutral": 0,
#                 "tap_min": -2,
#                 "tap_max": 2,
#                 "tap_step_percent": 2.5,
#                 "tap_step_degree": 0.0,
#                 "tap_pos": 0,
#                 "tap_phase_shifter": False,
#                 "parallel": 1,
#                 "df": 1.0,
#                 "in_service": True
#             }
#         ],
#         "trafo",
#         log=True
#     )
#
#     validate_pp_load_flow_results(
#         pp_network.line,
#         [
#             {
#                 "name": "line_None",
#                 "std_type": "NAYY 4x50 SE",
#                 "from_bus": [tx_lv_bus, load_bus],
#                 "to_bus": [tx_lv_bus, load_bus],
#                 "length_km": 0.1,
#                 "r_ohm_per_km": 0.642,
#                 "x_ohm_per_km": 0.083,
#                 "c_nf_per_km": 210,
#                 "g_us_per_km": 0.0,
#                 "max_i_ka": 0.142,
#                 "df": 1.0,
#                 "parallel": 1,
#                 "type": "cs",
#                 "in_service": True
#             }
#         ],
#         "line",
#         log=True
#     )
#
#     validate_pp_load_flow_results(
#         pp_network.load,
#         [
#             {
#                 "name": "Load",
#                 "bus": load_bus,
#                 "p_mw": 0.1,
#                 "q_mvar": 0.05,
#                 "const_z_percent": 0.0,
#                 "const_i_percent": 0.0,
#                 "sn_mva": pp.nan,
#                 "scaling": 1.0,
#                 "in_service": True,
#                 "type": "wye"
#             }
#         ],
#         "load",
#         log=True
#     )
#
#     validate_pp_load_flow_results(
#         pp_network.ext_grid,
#         [
#             {
#                 "name": "Grid Connection",
#                 "bus": tx_hv_bus,
#                 "vm_pu": 1.02,
#                 "va_degree": 0.0,
#                 "in_service": True
#             }
#         ],
#         "ext_grid",
#         log=True
#     )
