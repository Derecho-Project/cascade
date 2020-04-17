#!/bin/bash
source util.sh

# bw_test_num_nodes <msg_size> <window_size> <is_persistent> <device>
bw_test_num_nodes() {
    if [ $# != 3 ]; then
        echo "Format error. Usage: ${FUNCNAME[0]} <msg_size> <window_size> <is_persistent> <mlx5_0|mlx5_1>"
        return -1
    fi
    
    for i in {3..16}
    do
        # run clients
        for j in {0..i}
        do
            remote_run ${nodes_name[${j}]} "taskset 0xaaaa ./perf --SUBGROUP/VCS/max_payload_size=$1 --SUBGROUP/VCS/window_size=$2 --RDMA/domain=$4 -- 0 ${num_objs[i]} $3" | grep -e "^\s*[[:digit:]]\+" | awk '{print $6}'
        done
        # run server
        taskset 0xaaaa ./perf --SUBGROUP/VCS/max_payload_size=$1 --SUBGROUP/VCS/window_size=$2 --RDMA/domain=$4 -- 1 ${num_objs[i]} $3 > output
        cat output | grep -m1 throughput | sed 's/:/ /' | awk '{print $2}' | grep -o '[[:digit:]]\+\.[[:digit:]]\+' >> data
    done
}

# bw_test_msg_size <num_nodes> <window_size> <is_persistent> <device>
bw_test_msg_sizes() {
    sizes=(10240 1048576 104857600)
    if [ $# != 3 ]; then
        echo "Format error. Usage: ${FUNCNAME[0]} <num_nodes> <window_size> <is_persistent> <mlx5_0|mlx5_1>"
        return -1
    fi
    
    for i in {0..${#sizes[@]}}
    do
        # run clients
        for j in {0..$1}
        do
            remote_run ${nodes_name[${j}]} "taskset 0xaaaa ./perf --SUBGROUP/VCS/max_payload_size=${sizes[i]} --SUBGROUP/VCS/window_size=$2 --RDMA/domain=$4 -- 0 ${num_objs[i]} $3" | grep -e "^\s*[[:digit:]]\+" | awk '{print $6}'
        done
        # run server
        taskset 0xaaaa ./perf --SUBGROUP/VCS/max_payload_size=${sizes[i]} --SUBGROUP/VCS/window_size=$2 --RDMA/domain=$4 -- 1 ${num_objs[i]} $3 > output
        cat output | grep -m1 throughput | sed 's/:/ /' | awk '{print $2}' | grep -o '[[:digit:]]\+\.[[:digit:]]\+' >> exptwo
    done
}