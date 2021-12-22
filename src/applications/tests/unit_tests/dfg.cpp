#include <cascade/data_flow_graph.hpp>
#include <iostream>

using namespace derecho::cascade;

// dump dfgs configuration
int main(int argc, char** argv) {
    for(auto dfg:DataFlowGraph::get_data_flow_graphs()) {
        dfg.dump();
    }
    return 0;
}
