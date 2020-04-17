#!/bin/bash
source nodes.sh

# list all nodes
list_nodes() {
    for ((i=0;i<${num_nodes};i++))
    do
        if [[ " ${blacklist[@]} " =~ ${nodes_name[$i]} ]] || [[ " ${temporary_blacklist[@]} " =~ ${nodes_name[$i]} ]]; then
            echo -e "${nodes_name[$i]}(${nodes_ips[$i]})\t[SKIPPED]"
            continue
        fi
        echo -en "${nodes_name[$i]}(${nodes_ips[$i]})\t"
        ping ${nodes_ips[$i]} -c 1 -W 2 > /dev/null
        if [ $? -eq 0 ]; then
            echo -e "[  UP  ]"
        else
            echo -e "[ DOWN ]"
        fi
    done
    return 0
}

# run on an remote node:
# remote_run "command"
remote_run() {
    if [ $# -lt 2 ]; then
        echo "Failed: remote_run $@"
        echo "Usage: ${FUNCNAME[0]} <host> command args..."
        return -1
    fi
    ssh -o ConnectTimeout=2 -o ConnectionAttempts=1 $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10}
    return $?
}

remote_run_bg() {
    if [ $# -lt 2 ]; then
        echo "Failed: remote_run $@"
        echo "Usage: ${FUNCNAME[0]} <host> command args..."
        return -1
    fi
    nohup ssh -o ConnectTimeout=2 -o ConnectionAttempts=1 $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} 1>/dev/null 2>/dev/null &
    return $?
}

# upload to remote
upload_to() {
    if [ $# -lt 2 ]; then
        echo "Failed: upload_to $@"
        echo "Usage: ${FUNCNAME[0]} <local_file> <host:remote_file>"
    else
        scp $1 $2
    fi
}

# print title line.
double_line_title() {
    title=$1
    tlen=${#title}
    # =====
    for ((ii=0;ii<${tlen};ii++))
    do
        echo -en "="
    done
    echo ""
    # title
    echo -e "$title"
    # =====
    for ((ii=0;ii<${tlen};ii++))
    do
        echo -en "="
    done
    echo ""
}

# run on all remote nodes:
nodes_run() {
    for ((i=0;i<${num_nodes};i++))
    do
        # print the title
        double_line_title "${nodes_name[$i]}(${nodes_ips[$i]})"
        if [[ " ${blacklist[@]} " =~ ${nodes_name[$i]} ]] || [[ " ${temporary_blacklist[@]} " =~ ${nodes_name[$i]} ]]; then
            echo -e "...SKIPPED..."
            continue
        fi

        # run
        remote_run ${nodes_ips[$i]} $1 $2 $3 $4 $5 $6 $7 $8 $9
    done
}

# nodes_upload <local_file> <remote_file_path>
nodes_upload() {
    for ((i=0;i<${num_nodes};i++))
    do
        # print the title
        double_line_title "${nodes_name[$i]}(${nodes_ips[$i]})"
        if [[ " ${blacklist[@]} " =~ ${nodes_name[$i]} ]] || [[ " ${temporary_blacklist[@]} " =~ ${nodes_name[$i]} ]]; then
            echo -e "...SKIPPED..."
            continue
        fi

        # run
        upload_to $1 ${nodes_ips[$i]}:$2
    done
}

# nodes_info
nodes_info() {
    for ((i=0;i<${num_nodes};i++))
    do
        if [[ " ${blacklist[@]} " =~ ${nodes_name[$i]} ]] || [[ " ${temporary_blacklist[@]} " =~ ${nodes_name[$i]} ]]; then
            echo -e "${nodes_name[$i]}(${nodes_ips[$i]})\t[SKIPPED]"
            continue
        fi
        echo -en "${nodes_name[$i]}(${nodes_ips[$i]})\t"
        ping ${nodes_ips[$i]} -c 1 -W 2 > /dev/null
        if [ $? -eq 0 ]; then
            echo -e "[  UP  ]"
        else
            echo -e "[ DOWN ]"
        fi
        remote_run ${nodes_ips[i]} "uname -sr;cat /etc/lsb-release|grep RELEASE"
    done
    return 0
}