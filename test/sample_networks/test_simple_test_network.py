import pandapower
import pytest

from pp_creators.creator import PandaPowerNetworkCreator
from sample_networks import simple_test_network
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

    # todo: investigate why hv_bus and lv_bus is not repeatable attibute.

    validate_df(
        pp_network.trafo,
        [
            {
                "name": "Transformer",
                "std_type": "0.4 MVA 20/0.4 kV",
                "hv_bus": 2,
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

    # todo: investigate why from_bus to _bus is array x 2

    validate_df(
        pp_network.line,
        [
            {
                "name": "line_None",
                "std_type": "NAYY 4x50 SE",
                "from_bus": [1, 0],
                "to_bus": [1, 0],
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

    validate_df(
        pp_network.load,
        [
            {
                "name": "Load",
                "bus": 0,
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

    validate_df(
        pp_network.ext_grid,
        [
            {
                "name": "Grid Connection",
                "bus": 2,
                "vm_pu": 1.02,
                "va_degree": 0.0,
                "in_service": True
            }
        ],
        "ext_grid",
        log=True
    )


def test_simple_test_network_load_flow(pp_network: pandapower.pandapowerNet):
    pandapower.runpp(pp_network)
    assert len(pp_network.res_bus) == 3
    assert len(pp_network.res_line) == 1
    assert len(pp_network.res_trafo) == 1
    assert len(pp_network.res_ext_grid) == 1
    assert len(pp_network.res_load) == 1
    # todo: Finish load flow results validation
