#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from typing import List, Dict, Any

import pandapower as pp
# noinspection PyPackageRequirements
from pandas.core.frame import DataFrame
from zepben.evolve.model.busbranch.bus_branch import create_bus_branch_model, CreationResult

from pp_creators.creators import create_pp_bus, create_pp_line, create_pp_load, create_pp_line_type, get_line_type_id, \
    create_pp_transformer, create_pp_transformer_type, get_transformer_type_id, create_pp_grid_connection


# Refer to pandapower's simple 3-bus network example: https://www.pandapower.org/start/#a-short-introduction-
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
    _assert_no_errors(result)

    # Run Load Flow
    pp.runpp(result.bus_branch_model)
    pp_network = result.bus_branch_model

    # Validate and Log
    _validate_and_log_load_flow_results(
        pp_network.bus,
        [
            {"name": "bus_None", "vn_kv": 20.0, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None}
        ],
        "bus"
    )

    _validate_and_log_load_flow_results(
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
        "trafo"
    )

    _validate_and_log_load_flow_results(
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
        "line"
    )

    _validate_and_log_load_flow_results(
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
        "load"
    )

    _validate_and_log_load_flow_results(
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
        "ext_grid"
    )


def _validate_and_log_load_flow_results(
        actual_results: DataFrame,
        expected_result_values: List[Dict[str, Any]],
        title: str
):
    _assert_df_are_equal(actual_results, expected_result_values)
    _log_pp_dataframe(actual_results, title)


def _assert_df_are_equal(pp_df: DataFrame, expected_values: List[Dict[str, Any]]):
    columns = pp_df.columns
    value_dict = {}
    for column in columns:
        value_dict[column] = getattr(pp_df, column)

    actual_values = []
    for i in range(0, len(next(iter(value_dict.values())))):
        actual_values.append({column: value_dict[column][i] for column in columns})

    assert len(actual_values) == len(expected_values)
    for i in range(0, len(actual_values)):
        _assert_dicts_are_equal(actual_values[i], expected_values[i])


def _assert_dicts_are_equal(a: Dict, b: Dict):
    a_keys = set(a.keys())
    b_keys = set(b.keys())
    shared_keys = a_keys.intersection(b_keys)
    non_shared_keys = a_keys ^ b_keys
    assert len(non_shared_keys) == 0

    for k in shared_keys:
        a_i = a[k]
        b_i = b[k]

        if isinstance(a_i, float) and pp.isnan(a_i):
            assert isinstance(b_i, float) and pp.isnan(b_i), \
                f"actual {k} value of {a_i} is different to expected {k} value of {b_i}"
        else:
            assert a_i == b_i, f"actual {k} value of {a_i} is different to expected {k} value of {b_i}"


def _log_pp_dataframe(pp_df: DataFrame, title: str):
    print(f"\n|_{title}_|")

    columns = pp_df.columns
    print(f"| {' | '.join(columns)} |")

    values = {}
    for column in columns:
        values[column] = getattr(pp_df, column)

    total_row_number = pp_df.shape[0]
    for i in range(0, total_row_number):
        row = f"| {' | '.join((str(values[column][i]) for column in columns))} |"
        print(row)


def _assert_no_errors(result: CreationResult):
    if not result.succeed:
        error_msg = ""
        for error_type, infos in result.errors.items():
            error_msg += f"{error_type}\n"
            for info in infos:
                error_msg += f"    {error_type.value} for {info.io.mrid}\n"
        assert False, error_msg
