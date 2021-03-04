#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import pandapower as pp
from zepben.evolve.model.busbranch.bus_branch import create_bus_branch_model

from pp_creators.creators import create_pp_bus, create_pp_line, create_pp_load, create_pp_line_type, get_line_type_id, \
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

    if not result.succeed:
        error_msg = ""
        for error_type, infos in result.errors.items():
            error_msg += f"{error_type}\n"
            for info in infos:
                error_msg += f"    {error_type.value} for {info.io.mrid}\n"
        assert False, error_msg
    # Print Results for Smoke Test
    print(" -------------------------------------------")
    pp.runpp(result.bus_branch_model)
    print("bus\n")
    print(result.bus_branch_model.bus)
    print(" -------------------------------------------")

    print("trafo\n")
    print(result.bus_branch_model.trafo)
    print(" -------------------------------------------")

    print("line\n")
    print(result.bus_branch_model.line)
    print(" -------------------------------------------")

    print("load\n")
    print(result.bus_branch_model.load)
    print(" -------------------------------------------")

    print("ext_grid\n")
    print(result.bus_branch_model.ext_grid)
    print(" -------------------------------------------")
