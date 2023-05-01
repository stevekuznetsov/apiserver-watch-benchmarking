#!/usr/bin/env python

import json
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns
from flexitext import flexitext
from matplotlib import lines
from matplotlib.patheffects import withStroke

if len(sys.argv) != 4:
    print("Invalid arguments, usage:")
    print("{} [data-dir] [output-figure-dir]".format(sys.argv[0]))
    sys.exit(1)

data_dir = sys.argv[1]
output_figure_dir = sys.argv[2]

events_data = {}
events_file_path = os.path.join(data_dir, "latent-watch.json")
with open(events_file_path) as events_file:
    events_data = json.load(events_file)

cadvisor_data = {}
cadvisor_file_path = os.path.join(data_dir, "data.json")
with open(cadvisor_file_path) as cadvisor_file:
    cadvisor_data = json.load(cadvisor_file)

def annotate_axis(ax, xticks, xticklabels, max_y, max_x, ytickformat, title):
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.xaxis.set_ticks(xticks)
    ax.xaxis.set_ticklabels(xticklabels, fontsize=16, fontweight=100)
    ax.xaxis.set_tick_params(length=6, width=1.2)
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#A8BAC4", lw=1.2)
    ax.yaxis.set_major_locator(plt.MaxNLocator(6, steps=[1, 2, 4, 5, 10]))
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    ax.yaxis.set_tick_params(labelleft=False, length=0)
    formatter = ticker.FuncFormatter(lambda y, pos: '')
    ax.yaxis.set_major_formatter(formatter)
    ax.set_xlim(left=-1, right=max_x)
    PAD = max_y * 0.01
    for label in ax.get_yticks()[1:-1]:
        ax.text(
            max_x, label + PAD, ytickformat(label),
            ha="right", va="baseline", fontsize=18, fontweight=100
        )
    plt.legend(loc="lower right", fontsize=12, frameon=False)

    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.get_legend().remove()

    ax.spines["bottom"].set_lw(1.2)
    ax.spines["bottom"].set_capstyle("butt")

    flexitext(0, 1, title, va="bottom", ax=ax)
    ax.add_artist(
        lines.Line2D(
            [0, 0.05], [1, 1], lw=2, color="black",
            solid_capstyle="butt", transform=ax.transAxes
        )
    )

nanosecond = 1
microsecond = 1000 * nanosecond
millisecond = 1000 * microsecond
second = 1000 * millisecond


def format(nanoseconds):
    nanoseconds = np.power(10, nanoseconds)
    if second <= nanoseconds:
        return '{:.0f}'.format(nanoseconds / second) + 's'
    elif millisecond <= nanoseconds < second:
        return '{:.0f}'.format(nanoseconds / millisecond) + 'ms'
    elif microsecond <= nanoseconds < millisecond:
        return '{:.0f}'.format(nanoseconds / microsecond) + 'us'
    else:
        return '{:.0f}'.format(nanoseconds / nanosecond) + 'ns'


kilobyte = 1000
megabyte = 1000 * kilobyte
gigabyte = 1000 * megabyte


def formatBytes(bytes):
    if 0 < bytes < megabyte:
        return '{:,.0f}'.format(bytes / kilobyte) + 'KB'
    elif megabyte <= bytes < gigabyte:
        return '{:,.0f}'.format(bytes / megabyte) + 'MB'
    else:
        return '{:,.0f}'.format(bytes / gigabyte) + 'GB'


def parse_cadvisor_data(data):
    for id in data:
        if len(data[id]) == 1:
            # we have a non-ha setup, can simply parse
            return parse_singleton_data(data)
        else:
            # we have HA data
            return parse_ha_data(data)


def parse_singleton_data(data):
    components = []
    cpus = []
    mems = []
    timestamps = []
    for id in data:
        for record in data[id]:
            base = np.datetime64(data[id][record][0]["timestamp"].removesuffix("Z"))
            sub_cpus = []
            sub_ts = []
            for item in data[id][record]:
                components.append(id)
                mems.append(item["memory"]["working_set"])
                sub_cpus.append(item["cpu"]["usage"]["total"] / 1e9)
                sub_ts.append((np.datetime64(item["timestamp"].removesuffix("Z")) - base) / np.timedelta64(1, 's'))

            grad = np.gradient(sub_cpus, sub_ts)
            cpus.extend(grad)
            timestamps.extend(sub_ts)

    df = pd.DataFrame({"component": components, "cpu": cpus, "mem": mems, "timestamp": timestamps})
    df.info()
    return df


def parse_ha_data(data):
    components = []
    cpus = []
    mems = []
    timestamps = []
    for id in data:
        mem = None
        cpu = None
        for record in data[id]:
            base = np.datetime64(data[id][record][0]["timestamp"].removesuffix("Z"))
            sub_cpus = [item["cpu"]["usage"]["total"] / 1e9 for item in data[id][record]]
            sub_mems = [item["memory"]["working_set"] for item in data[id][record]]
            sub_ts = [np.datetime64(item["timestamp"].removesuffix("Z")) - base for item in data[id][record]]

            mem_series = pd.Series(sub_mems, index=sub_ts)
            resampled_mem = mem_series.resample("3S").mean().interpolate(method="time")
            if mem is None:
                mem = resampled_mem
            else:
                mem = mem.add(resampled_mem)

            cpu_series = pd.Series(sub_cpus, index=sub_ts)
            resampled_cpu = cpu_series.resample("3S").mean().interpolate(method="time")
            if cpu is None:
                cpu = resampled_cpu
            else:
                cpu = cpu.add(resampled_cpu)

        timestamp_seconds = [t / np.timedelta64(1, 's') for t in cpu.index]
        cpu_rate = np.gradient(cpu.values, timestamp_seconds)

        components.extend([id for item in timestamp_seconds])
        timestamps.extend(timestamp_seconds)
        weight = 10
        avg = np.convolve(cpu_rate, np.ones(weight), 'same') / weight
        cpus.extend(avg)
        mems.extend(mem.values)

    df = pd.DataFrame({"component": components, "cpu": cpus, "mem": mems, "timestamp": timestamps})
    df.info()
    return df


def plot_resource_usage(ax, df):
    sns.lineplot(x="timestamp", y="cpu", hue="component", data=df, ax=ax[0])
    annotate_axis(
        ax=ax[0],
        xticks=[],
        xticklabels=[],
        max_y=df.cpu.max(),
        max_x=df.timestamp.max() * 1.05,
        ytickformat=lambda y: "{:0.1f}".format(y),
        title="<size:18><weight:bold>CPU Usage,</> vCPUs</>"
    )

    sns.lineplot(x="timestamp", y="mem", hue="component", data=df, ax=ax[1])
    annotate_axis(
        ax=ax[1],
        xticks=[],
        xticklabels=[],
        max_y=df.mem.max(),
        max_x=df.timestamp.max() * 1.05,
        ytickformat=lambda y: formatBytes(y),
        title="<size:18><weight:bold>Working Set,</> bytes</>"
    )

    path_effects = [withStroke(linewidth=10, foreground="white")]
    timestamps = df.loc[df['component'] == "etcd"].timestamp
    timestamp = timestamps[timestamps.size - 190]
    ax[0].text(
        timestamp, 4.2, "kube-api", fontsize=18,
        va="top", ha="left", path_effects=path_effects, color=sns.color_palette()[1]
    )
    ax[1].text(
        timestamp, 7 * gigabyte, "kube-api", fontsize=18,
        va="top", ha="left", path_effects=path_effects, color=sns.color_palette()[1]
    )
    ax[0].text(
        timestamp, 1.8, "etcd", fontsize=18,
        va="bottom", ha="left", path_effects=path_effects, color=sns.color_palette()[0]
    )
    ax[1].text(
        timestamp, 17 * gigabyte, "etcd", fontsize=18,
        va="bottom", ha="left", path_effects=path_effects, color=sns.color_palette()[0]
    )


fig, axes = plt.subplot_mosaic([['left', 'upper right'],
                                ['left', 'lower right']],
                               figsize=(12, 7.2), constrained_layout=True)
plot_request_times(axes["left"], parse_request_times(events_data))
plot_resource_usage([axes["upper right"], axes["lower right"]], parse_cadvisor_data(cadvisor_data))
fig.subplots_adjust(
    left=0, right=1,
    top=0.825, bottom=0.15,
    hspace=0.25, wspace=0.05
)
fig.set_facecolor("w")
title = "<size:22><weight:bold>Server Performance as a Function of Open Watch Count\n</></>" + \
        "<size:20>Comparing etcd and kube-apiserver response to open watches</>"
flexitext(0, .98, title, va="top", xycoords='figure fraction', ax=fig.axes[0])

# source = '{} initial database size comprised of {} objects.\n' \
#          '{} requests across {} parallel workers using field selectors at varying levels of selectivity.'.format(
#     formatBytes(
#         config_data["crcomponent"]["interact"]["selectivity"]["count"] *
#         config_data["crcomponent"]["interact"]["selectivity"]["fill_size"]),
#     formatCount(config_data["crcomponent"]["interact"]["selectivity"]["count"]),
#     config_data["crcomponent"]["interact"]["operations"],
#     config_data["crcomponent"]["interact"]["parallelism"]
# )
fig.text(0.01, 0.02, "source", color="#a2a2a2", fontsize=12)
fig.savefig(
    os.path.join(output_figure_dir, "indexed_requests.png"),
    dpi=300)
plt.show()
plt.show()
