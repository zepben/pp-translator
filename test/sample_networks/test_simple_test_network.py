import pandapower
import pytest
from zepben.evolve.examples import simple_test_network

from pp_creators.creator import PandaPowerNetworkCreator
from test.pp_test_utils import validate_df


@pytest.fixture
@pytest.mark.asyncio
async def creator_result():
    node_breaker_model = await simple_test_network.simple_test_network()
    creator = PandaPowerNetworkCreator(vm_pu=1.02)
    return await creator.create(node_breaker_model)


@pytest.fixture
def pp_network(creator_result) -> pandapower.pandapowerNet:
    pp_network: pandapower.pandapowerNet = creator_result.network
    return pp_network


def test_creator(creator_result):
    assert creator_result.was_successful


def test_simple_test_network(pp_network: pandapower.pandapowerNet):
    assert len(pp_network.bus) == 3
    assert len(pp_network.line) == 1
    assert len(pp_network.trafo) == 1
    assert len(pp_network.ext_grid) == 1
    assert len(pp_network.load) == 1

    # Validate and Log

    # Bus

    validate_df(
        pp_network.bus,
        [
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 0.4, "type": "b", "in_service": True, "zone": None},
            {"name": "bus_None", "vn_kv": 20.0, "type": "b", "in_service": True, "zone": None},
        ],
        "bus",
        log=True
    )

    # Transformer

    validate_df(
        pp_network.trafo,
        [
            {
                "name": "Transformer",
                "std_type": "0.4 MVA 20/0.4 kV",
                "hv_bus": [0, 1, 2],
                "lv_bus": [0, 1, 2],
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

    # Line

    validate_df(
        pp_network.line,
        [
            {
                "name": "line_None",
                "std_type": "NAYY 4x50 SE",
                "from_bus": [0, 1, 2],
                "to_bus": [0, 1, 2],
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

    # Load

    validate_df(
        pp_network.load,
        [
            {
                "name": "Load",
                "bus": [0, 1, 2],
                "p_mw": 0.1,
                "q_mvar": 0.05,
                "const_z_percent": 0.0,
                "const_i_percent": 0.0,
                "sn_mva": pandapower.nan,
                "scaling": 1.0,
                "in_service": True,
                "type": "wye"
            }
        ],
        "load",
        log=True
    )

    # Slack Bus

    validate_df(
        pp_network.ext_grid,
        [
            {
                "name": "Grid Connection",
                "bus": [0, 1, 2],
                "vm_pu": 1.02,
                "va_degree": 0.0,
                "in_service": True
            }
        ],
        "ext_grid",
        log=True
    )

    # Load flow validation

    pandapower.runpp(pp_network)
    assert len(pp_network.res_bus) == 3
    assert len(pp_network.res_line) == 1
    assert len(pp_network.res_trafo) == 1
    assert len(pp_network.res_ext_grid) == 1
    assert len(pp_network.res_load) == 1

    # Bus results
    if pp_network.res_bus.vm_pu[0] == 1.0088427233128725:
        df_voltage = [{"vm_pu": 1.0088427233128725, "va_degree": -0.760125718616866, "p_mw": 0.000000, "q_mvar": 0.0},
                      {"vm_pu": 0.9644305728933401, "va_degree": 0.11585861401564015, "p_mw": 0.1, "q_mvar": 0.05},
                      {"vm_pu": 1.02, "va_degree": 0.0, "p_mw": -0.10726539055005266, "q_mvar": -0.05267519521321317}]
    else:
        df_voltage = [{"vm_pu": 0.9644305728933401, "va_degree": 0.11585861401564015, "p_mw": 0.100000, "q_mvar": 0.05},
                      {"vm_pu": 1.0088427233128725, "va_degree": -0.760125718616866, "p_mw": 0.000000, "q_mvar": 0.0},
                      {"vm_pu": 1.02, "va_degree": 0.0, "p_mw": -0.10726539055005266, "q_mvar": -0.05267519521321317}]

    validate_df(
        pp_network.res_bus,
        df_voltage,
        "res_bus",
        log=True
    )

    # Line results

    if pp_network.res_line.p_from_mw[0] == 0.10539239122743502:
        df_line = [
            {"p_from_mw": 0.10539239122743502, "q_from_mvar": 0.05069611896419347, "p_to_mw": -0.09999999976783382,
             "q_to_mvar": -0.04999999985382754, "pl_mw": 0.005392391459601201, "ql_mvar": 0.0006961191103659337,
             "i_from_ka": 0.16732533378705575, "i_to_ka": 0.167325995497805, "i_ka": 0.167325995497805,
             "vm_from_pu": 1.0088427233128725, "va_from_degree": -0.760125718616866, "vm_to_pu": 0.9644305728933401,
             "va_to_degree": 0.11585861401564015, "loading_percent": 117.83520809704578}]
    else:
        df_line = [
            {"p_from_mw": -0.09999999976783382, "q_from_mvar": -0.04999999985382754, "p_to_mw": 0.10539239122743502,
             "q_to_mvar": 0.05069611896419347, "pl_mw": 0.005392391459601201, "ql_mvar": 0.0006961191103659337,
             "i_from_ka": 0.167325995497805, "i_to_ka": 0.16732533378705575, "i_ka": 0.167325995497805,
             "vm_from_pu": 0.9644305728933401, "va_from_degree": 0.11585861401564015, "vm_to_pu": 1.0088427233128725,
             "va_to_degree": -0.760125718616866, "loading_percent": 117.83520809704578}]

    validate_df(
        pp_network.res_line,
        df_line,
        "res_line",
        log=True
    )

    # Transformer Results

    if pp_network.res_trafo.p_hv_mw[0] == 0.10726539055005266:
        df_trafo = [
            {"p_hv_mw": 0.10726539055005266, "q_hv_mvar": 0.05267519521321317, "p_lv_mw": -0.1053923911853459,
             "q_lv_mvar": -0.05069611903719688, "pl_mw": 0.001872999364706765, "ql_mvar": 0.001979076176016288,
             "i_hv_ka": 0.00338206167645762, "i_lv_ka": 0.16732533377806555, "vm_hv_pu": 1.02, "va_hv_degree": 0.0,
             "vm_lv_pu": 1.0088427233128725, "va_lv_degree": -0.760125718616866, "loading_percent": 29.289513289780857}]
    else:
        df_trafo = [
            {"p_hv_mw": -0.1053923911853459, "q_hv_mvar": -0.05069611903719688, "p_lv_mw": 0.10726539055005266,
             "q_lv_mvar": 0.05267519521321317, "pl_mw": 0.001872999364706765, "ql_mvar": 0.001979076176016288,
             "i_hv_ka": 0.16732533377806555, "i_lv_ka": 0.00338206167645762, "vm_hv_pu": 1.0088427233128725,
             "va_hv_degree": -0.760125718616866, "vm_lv_pu": 1.02, "va_lv_degree": 0.0,
             "loading_percent": 29.289513289780857}]
    validate_df(
        pp_network.res_trafo,
        df_trafo,
        "res_trafo",
        log=True
    )

    # Slack bus results

    validate_df(
        pp_network.res_ext_grid,
        [
            {"p_mw": 0.10726539055005266, "q_mvar": 0.05267519521321317},
        ],
        "res_ext_grid",
        log=True
    )

    # Load results

    validate_df(
        pp_network.res_load,
        [
            {"p_mw": 0.1, "q_mvar": 0.05},
        ],
        "res_load",
        log=True
    )
