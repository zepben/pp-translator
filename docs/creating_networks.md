# Documentation

## Installing pp-translator

To install the latest development version of pp-translator from github, simply follow these steps:

1. Download and install [git](https://git-scm.com/)
2. Open a git shell and navigate to the directoy where you want to keep your pp-translator files.
3. Run the following git command:
```
   git clone https://github.com/zepben/pp-translator.git
```
4. Set a virtual environment for your python script
5. Navigate to the folder where the pp-translator files are located: 
```
    cd..
    cd pp-translator
```
6. Run the following command to install pp-translator:
```
    pip install -e .
```
This registers your local pp-translator installation with pip. 

## Test your installation

A first basic way to test your installation is to import the simple_test_network from pp-translator and run load flow in pandapower.

1. Import the following libraries:
```
import asyncio
import logging
import pandapower as pp
from pp_creators.creator import PandaPowerNetworkCreator
from sample_networks import simple_test_network
```
2. Use the following code to create a method for loading the simple_test_network in python:
```
async def main():
    node_breaker_model = await simple_test_network.simple_test_network()
    creator = PandaPowerNetworkCreator(vm_pu=1.02, logger=logging.getLogger())
    result = await creator.create(node_breaker_model)

    pp_network = result.network
    print(pp_network)
```
3. If everything is installed correctly, simple_test_network should be loaded. You should be able to see the following: 
```
This pandapower network includes the following parameter tables:
    - bus (3 elements)
    - load (1 element)
    - ext_grid (1 element)
    - line (1 element)
    - trafo (1 element)
```
4. Run the load flow and print the network using the following commands: 
```
pp.runpp(pp_network)
print(pp_network)
```
5. You should see the following if the load flow has converged:
```
This pandapower network includes the following parameter tables:
    - bus (3 elements)
    - load (1 element)
    - ext_grid (1 element)
    - line (1 element)
    - trafo (1 element)
 and the following results tables:
    - res_bus (3 elements)
    - res_line (1 element)
    - res_trafo (1 element)
    - res_ext_grid (1 element)
    - res_load (1 element)
```
This confirms that testing is complete. 
    