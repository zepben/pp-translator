# Example lv test network for running three phase power flow using PandaPower
import pandapower as pp

# Create network
net = pp.create_empty_network()

# Buses
pp.create_bus(net, index=1, name="SourceNode", vn_kv=11, type='b')
pp.create_bus(net, index=2, name="Bus0", vn_kv=0.416, type='b')
pp.create_bus(net, index=3, name="Bus1", vn_kv=0.416, type='b')
pp.create_bus(net, index=4, name="Bus25", vn_kv=0.416, type='b')
pp.create_bus(net, index=5, name="Bus27", vn_kv=0.416, type='b')
pp.create_bus(net, index=6, name="Bus34", vn_kv=0.416, type='b')
pp.create_bus(net, index=7, name="Bus70", vn_kv=0.416, type='b')
pp.create_bus(net, index=8, name="Bus32", vn_kv=0.416, type='b')
pp.create_bus(net, index=9, name="Bus36", vn_kv=0.416, type='b')
pp.create_bus(net, index=10, name="Bus47", vn_kv=0.416, type='b')

# External grid
pp.create_ext_grid(net, 1, vm_pu=1.05, va_degree=0, s_sc_max_mva=10, s_sc_min_mva=8, rx_min=0.1, rx_max=0.1,
                   r0x0_max=1, x0x_max=1, r0x0_min=0.1, x0x_min=1, name="SourceNode")

# Lines
pp.create_line_from_parameters(net, name="acls1", from_bus=3, to_bus=4, length_km=0.03171938, r_ohm_per_km=0.446,
                               x_ohm_per_km=0.071, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.505,
                               x0_ohm_per_km=0.083, c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls2", from_bus=4, to_bus=5, length_km=0.00675, r_ohm_per_km=1.15,
                               x_ohm_per_km=0.088, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.2, x0_ohm_per_km=0.088,
                               c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls3", from_bus=5, to_bus=6, length_km=0.0053428, r_ohm_per_km=1.15,
                               x_ohm_per_km=0.088, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.2, x0_ohm_per_km=0.088,
                               c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls4", from_bus=5, to_bus=7, length_km=0.0756, r_ohm_per_km=1.15,
                               x_ohm_per_km=0.088, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.2, x0_ohm_per_km=0.088,
                               c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls5", from_bus=4, to_bus=8, length_km=0.0255, r_ohm_per_km=0.446,
                               x_ohm_per_km=0.071, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=0.505,
                               x0_ohm_per_km=0.083, c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls6", from_bus=8, to_bus=9, length_km=0.0057, r_ohm_per_km=1.15,
                               x_ohm_per_km=0.088, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.2, x0_ohm_per_km=0.088,
                               c0_nf_per_km=0)
pp.create_line_from_parameters(net, name="acls7", from_bus=9, to_bus=10, length_km=0.01265, r_ohm_per_km=1.15,
                               x_ohm_per_km=0.088, c_nf_per_km=0, max_i_ka=0.1, r0_ohm_per_km=1.2, x0_ohm_per_km=0.088,
                               c0_nf_per_km=0)

# Transformer
pp.create_transformer_from_parameters(net, hv_bus=1, lv_bus=2, i0_percent=0.038, pfe_kw=0, vkr_percent=0.4,
                                      sn_mva=0.8, vn_lv_kv=0.416, vn_hv_kv=11.0, vk_percent=4, vector_group="Dyn",
                                      vk0_percent=4.01995, vkr0_percent=0.4, mag0_percent=1, mag0_rx=0,
                                      si0_hv_partial=0.9)

# Loads
pp.create_asymmetric_load(net, 6, p_a_mw=10/1000, q_a_mvar=3.286/1000, name="Load1", type="wye")
pp.create_asymmetric_load(net, 10, p_b_mw=10/1000, q_b_mvar=3.286/1000, name="Load2", type="wye")
pp.create_asymmetric_load(net, 7, p_a_mw=10/1000, q_a_mvar=3.286/1000, name="Load3", type="wye")

# Generator
pp.create_asymmetric_sgen(net, bus=6, p_a_mw=5/1000, q_a_mvar=1.5/1000, name="PV-DG", type="PV")

# Switch
pp.create_switch(net, bus=2, element=3, et='b')

# Power Flow
pp.add_zero_impedance_parameters(net)
pp.runpp_3ph(net, numba=False)
print(net)
print(net.res_bus_3ph)
print(net.res_line_3ph)
print(net.res_ext_grid_3ph)

