# Get started example from PandaPower
import pandapower as pp
import pandas as pd

# Create empty net
net = pp.create_empty_network()

# Create buses
b1 = pp.create_bus(net, vn_kv=20., name="Bus 1")
b2 = pp.create_bus(net, vn_kv=0.4, name="Bus 2")
b3 = pp.create_bus(net, vn_kv=0.4, name="Bus 3")

# Create bus elements
pp.create_ext_grid(net, bus=b1, vm_pu=1.02, name="Grid Connection")
pp.create_load(net, bus=b3, p_mw=0.1, q_mvar=0.05, name="Load")

# Create branch elements
tid = pp.create_transformer(net, hv_bus=b1, lv_bus=b2, std_type="0.4 MVA 20/0.4 kV", name="Trafo")
pp.create_line(net, from_bus=b2, to_bus=b3, length_km=0.1, name="Line", std_type="NAYY 4x50 SE")

# Run power flow
pp.runpp(net)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
print(net)
print(net.res_bus)
print(net.res_line)
print(net.res_ext_grid)
