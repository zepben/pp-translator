
from zepben.evolve.examples import *
from zepben.evolve import BaseVoltage, EnergySource, Junction, Terminal, Feeder, PowerTransformer, \
    ConnectivityNode, DiagramObject, Breaker, Equipment, AcLineSegment, EnergyConsumer
import zepben.evolve
import asyncio
import sys
import pandapower as pp
import pandas as pd


net = SimpleNodeBreakerFeeder(breaker_is_open=False)

voltages = list(net.network_service.objects(BaseVoltage))
sources = list(net.network_service.objects(EnergySource))
junctions = list(net.network_service.objects(Junction))
terminals = list(net.network_service.objects(Terminal))
breakers = list(net.network_service.objects(Breaker))
power_transformers = list(net.network_service.objects(PowerTransformer))
connectivity_nodes = list(net.network_service.objects(ConnectivityNode))
feeders = list(net.network_service.objects(Feeder))
lines = list(net.network_service.objects(AcLineSegment))
loads = list(net.network_service.objects(EnergyConsumer))

print(power_transformers[0]._power_transformer_ends)
print(connectivity_nodes)
print(voltages)
print(sources)
print(loads)
print(lines)

v1_kv = voltages[0].nominal_voltage/1000
# v2_kv = voltages[1].nominal_voltage/1000
v2_kv = 0.4

# Create empty Pandapower network
net_pp = pp.create_empty_network()

# Create buses
b1 = pp.create_bus(net_pp, vn_kv=v1_kv, name="Bus 1")
b2 = pp.create_bus(net_pp, vn_kv=v2_kv, name="Bus 2")
b3 = pp.create_bus(net_pp, vn_kv=v2_kv, name="Bus 3")

print(net_pp.bus)

# TODO: Create elements
# Create bus elements
pp.create_ext_grid(net_pp, bus=b1, vm_pu=sources[0].voltage_magnitude/(v1_kv*1000), name=sources[0].name)
pp.create_load(net_pp, bus=b3, p_mw=0.1, q_mvar=0.05, name="Load")
# pp.create_load(net_pp, bus=b3, p_mw=loads[0].p/1e6, q_mvar=loads[0].q/1e6, name='Load')
# TODO: create name of load in simple_node_breaker_feeder

# Create branch elements
# tid = pp.create_transformer(net_pp, hv_bus=b1, lv_bus=b2, std_type="0.4 MVA 20/0.4 kV", name="Trafo")
vn_hv_kv = 20
vn_lv_kv = 0.4
vk_percent = 6.0
vkr_percent = 1.425
pfe_kw = 1.35
i0_percent = 0.3375
shift_degree = 150
tap_side = "hv"
tap_neutral = 0
tap_min = -2
tap_max = 2
tap_pos = 0
name = "Trafo"
sn_mva = 0.4

# Create transformer from parameters
tid = pp.create_transformer_from_parameters(net_pp, hv_bus=b1, lv_bus=b2, name=name, vn_hv_kv=v1_kv, vn_lv_kv=v2_kv,
                            vk_percent=vk_percent, pfe_kw=pfe_kw, i0_percent=i0_percent, shift_degree=shift_degree,
                            tap_side=tap_side, tap_neutral=tap_neutral, tap_min=tap_min, tap_pos=0,
                            tap_max=tap_max,sn_mva=sn_mva, vkr_percent=vkr_percent)

# pp.create_line(net_pp, from_bus=b2, to_bus=b3, length_km=0.1, name="Line", std_type="NAYY 4x50 SE")
r_ohm_per_km = 0.642
# r_ohm_per_km = lines[0].per_length_sequence_impedance.r*1000
x_ohm_per_km = 0.083
# x_ohm_per_km = lines[0].per_length_sequence_impedance.x*1000
c_nf_per_km = 210
# c_nf_per_km = lines[0].per_length_sequence_impedance.bch/(2*50*3.141592*1e-9)
max_i_ka = 0.142
# max_i_ka = lines[0].asset_info.rated_current/1000
df = 1.0
from_bus = 1
to_bus = 2
length_km = 0.1
# length_km = lines[0].length/1000
parallel = 1
linename = 'Line 1'
# linename = line[0].name

# Create line from parameters
pp.create_line_from_parameters(net_pp, from_bus=from_bus, to_bus=to_bus, length_km=length_km, r_ohm_per_km=r_ohm_per_km,
                               x_ohm_per_km=x_ohm_per_km, max_i_ka=max_i_ka, name=linename, df=df, parallel=parallel,
                               c_nf_per_km=c_nf_per_km)

# Run power flow
pp.runpp(net_pp)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
print(net_pp)
print(net_pp.res_bus)
print(net_pp.res_line)
print(net_pp.res_ext_grid)