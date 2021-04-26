# Convert transformer from CIM-evolve model to pp model
from math import sqrt


class TransformerPp(object):

    def __init__(self, transformer):
        if len(transformer._terminals) == 2:

            # TODO: determine the nodes for connecting the transformer
            # hv_bus = get_index(Transformer._terminals[0].connectivity_node.mrid, a)
            # lv_bus = get_index(Transformer._terminals[1].connectivity_node.mrid, a)

            # Rated power and voltages
            self.sn_mva = transformer._power_transformer_ends[0].rated_s/1000000
            self.vn_hv_kv = transformer._power_transformer_ends[0].ratedU/1000
            self.vn_lv_kv = transformer._power_transformer_ends[1].ratedU/1000

            # Extract parameters
            s = transformer._power_transformer_ends[0].rated_s/1e6
            r = transformer._power_transformer_ends[0].r
            x = transformer._power_transformer_ends[0].x
            g = transformer._power_transformer_ends[0].g
            b = transformer._power_transformer_ends[0].b
            r0 = transformer._power_transformer_ends[0].r0
            x0 = transformer._power_transformer_ends[0].x0
            g0 = transformer._power_transformer_ends[0].g0
            b0 = transformer._power_transformer_ends[0].b0

            # Calculate pp parameters
            self.vk_percent = sqrt(r**2+x**2)*s/(1.732*self.vn_hv_kv**2)*100
            self.vkr_percent = (r*s)/(1.732*self.vn_hv_kv**2)*100
            self.pfe_kw = (g*self.vn_hv_kv**2)*1000
            self.i0_percent = (sqrt(b**2+g**2)*self.vn_hv_kv**2*100)/s
            self.vk0_percent = sqrt(r0**2+x0**2)*s/(1.732*self.vn_hv_kv**2)*100
            self.vkr0_percent = (r0*s)/(1.732*self.vn_hv_kv**2)*100
            self.mag0_rx = b0/g0
            self.mag0_percent = 100/sqrt(b0**2+g0**2)
            self.shift_degree = 30*transformer._power_transformer_ends[1].phaseAngleClock
            con1 = transformer._power_transformer_ends[0].connectionKind
            con2 = transformer._power_transformer_ends[1].connectionKind
            self.vector_group = con1 + con2

            # TODO: define the si0_hv_partial conversion
            self.si0_hv_partial = 0.9
