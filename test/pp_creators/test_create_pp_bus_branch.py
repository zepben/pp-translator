#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import pandapower as pp
# Refer to pandapower's simple 3-bus network example: https://www.pandapower.org/start/#a-short-introduction-
from test.pp_test_utils import validate_pp_load_flow_results, assert_no_creation_result_errors
# noinspection PyPackageRequirements
from zepben.evolve.model.busbranch.bus_branch import create_bus_branch_model

from src.pp_creators.creators import create_pp_bus, create_pp_line, create_pp_load, create_pp_line_type, get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection


def test_create_pp_bus_branch_model(simple_node_breaker_network):
    node_breaker_model = simple_node_breaker_network

    result = create_bus_branch_model(
        node_breaker_model,
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
    assert_no_creation_result_errors(result)

    # Run Load Flow
    pp.runpp(result.bus_branch_model)
    pp_network = result.bus_branch_model

    # Validate and Log
    validate_pp_load_flow_results(
        pp_network.bus,
        [
            {"name": "bus_None", "vn_kv": 20.0, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None}
        ],
        "bus",
        log=True
    )

    validate_pp_load_flow_results(
        pp_network.trafo,
        [
            {
                "name": "Transformer",
                "std_type": "0.4 MVA 20/0.4 kV",
                "hv_bus": 0,
                "lv_bus": 1,
                "sn_mva": 0.4,
                "vn_hv_kv": 20.0,
                "vn_lv_kv": 0.4,
                "vk_percent": 6.0,
                "vkr_percent": 1.425,
                "pfe_kw": 1.35,
                "i0_percent": 0.3375,
                "shift_degree": 150,
                "tap_side": "hv",
                "tap_neutral": 0,
                "tap_min": -2,
                "tap_max": 2,
                "tap_step_percent": 2.5,
                "tap_step_degree": 0.0,
                "tap_pos": 0,
                "tap_phase_shifter": False,
                "parallel": 1,
                "df": 1.0,
                "in_service": True
            }
        ],
        "trafo",
        log=True
    )

    validate_pp_load_flow_results(
        pp_network.line,
        [
            {
                "name": "line_None",
                "std_type": "NAYY 4x50 SE",
                "from_bus": 1,
                "to_bus": 2,
                "length_km": 0.1,
                "r_ohm_per_km": 0.642,
                "x_ohm_per_km": 0.083,
                "c_nf_per_km": 210,
                "g_us_per_km": 0.0,
                "max_i_ka": 0.142,
                "df": 1.0,
                "parallel": 1,
                "type": "cs",
                "in_service": True
            }
        ],
        "line",
        log=True
    )

    validate_pp_load_flow_results(
        pp_network.load,
        [
            {
                "name": "Load",
                "bus": 2,
                "p_mw": 0.1,
                "q_mvar": 0.05,
                "const_z_percent": 0.0,
                "const_i_percent": 0.0,
                "sn_mva": pp.nan,
                "scaling": 1.0,
                "in_service": True,
                "type": "wye"
            }
        ],
        "load",
        log=True
    )

    validate_pp_load_flow_results(
        pp_network.ext_grid,
        [
            {
                "name": "Grid Connection",
                "bus": 0,
                "vm_pu": 1.02,
                "va_degree": 0.0,
                "in_service": True
            }
        ],
        "ext_grid",
        log=True
    )
