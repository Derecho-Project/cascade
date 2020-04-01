import matplotlib.pyplot as plt 
import numpy as np

def plot_num_nodes():
    num_nodes = list(range(3,17))
    throughput = []
    with open("data",'r') as f:
        for line in f:
            throughput.append(float(line.strip()))
    
    plt.rcdefaults()
    fig, axes = plt.subplots()
    axes.plot(msg_size, throughput, color = 'C0')
    axes.plot(msg_size, throughput_1, '--', color ='C0')
    axes.plot(msg_size, throughput_2, color ='C1')
    axes.plot(msg_size, throughput_3, '--', color ='C1')

    plt.xscale('log', basex=2)
    plt.ylabel("throughput(MB/s)")
    plt.xlabel("num nodes(KB)")
    plt.xticks(num_nodes)
    colors = ["C0", "C1"]
    linestyles = ['-', "--"]

    dummy_lines = []
    for b_idx, b in enumerate(colors):
        dummy_lines.append(axes.plot([],[], c="black", ls = linestyles[b_idx])[0])
    lines = axes.get_lines()

    legend1 = plt.legend([lines[i] for i in [0,2]], ["VOLATILE", "PERSISTENT"], loc="upper left")
    legend2 = plt.legend([dummy_lines[i] for i in [0,1]], ["mlx5_0", "mlx5_1"], loc=4)
    axes.add_artist(legend1)
    axes.add_artist(legend2)
    plt.title("Derecho ObjectStore Performance")


    plt.savefig("benchmark.png",dpi = 800)