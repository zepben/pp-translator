#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from typing import Dict, List, Any

import pandapower as pp
# noinspection PyPackageRequirements
from pandas import DataFrame

__all__ = ["validate_pp_load_flow_results"]


def validate_pp_load_flow_results(
        actual_results: DataFrame,
        expected_result_values: List[Dict[str, Any]],
        title: str,
        log: bool = False
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
