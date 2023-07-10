#!/usr/bin/env python3
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

# Given a number between 1 and 16 try and return a distinct looking color.
def get_cpu_color(cpuid):
    if (cpuid == 0):
        return '#ffdede'
    if (cpuid == 1):
        return '#ff7171'
    if (cpuid == 2):
        return '#ff3131'
    if (cpuid == 3):
        return '#bcfeb1'
    if (cpuid == 4):
        return '#6eff54'
    if (cpuid == 5):
        return '#259211'
    if (cpuid == 6):
        return '#c2d4ff'
    if (cpuid == 7):
        return '#4e81ff'
    if (cpuid == 8):
        return '#0002ff'
    if (cpuid == 9):
        return '#faa4fe'
    if (cpuid == 10):
        return '#f400ff'
    if (cpuid == 11):
        return '#88008e'
    if (cpuid == 12):
        return '#00ffe3'
    if (cpuid == 13):
        return '#fdfc00'
    if (cpuid == 14):
        return '#898903'
    if (cpuid == 15):
        return '#ff9d00'

# Get the grey color used to represent no value.
def get_grey():
    return '#909090'

readings = {}
cpus=set()
# This needs to match the grid size declared at the top of multi_value.c
GRID_SIZE=4000
cpu_count=16

# Ingest the data from the multi_value executable.
f = open("data", "r")
thread_id = 0
for line in f:
    readings[thread_id] = []
    data = line.strip().split(',')
    i = 0
    while i < len(data):
        cpu = data[i]
        i += 1
        time = data[i]
        cpus.add(int(cpu))
        readings[thread_id].append([int(cpu), int(time)])
        i += 1
    thread_id += 1
thread_count = thread_id


fig, ax = plt.subplots()

y_start = 0
y_increment = 10
x_start = 0
x_stop = 0
y_ticks = []
y_labels = []
for i in range (0, thread_count):
    thread_data = readings[i]
    # Walk each of the CPU time pairs
    x_axis = []
    colors = None
    x_start = 0
    first = True
    last_cpu = 0
    for j in thread_data:
        x_stop = j[1]
        if (colors is not None):
            colors.append(get_cpu_color(last_cpu))
        else:
            colors = [get_grey()]
        last_cpu = j[0]
        # Append the start point and the stop point to the list.
        x_axis.append((x_start, j[1]))
        x_start = x_stop
    x_axis.append((x_start, GRID_SIZE))
    colors.append(get_cpu_color(last_cpu))
    ax.broken_barh(x_axis,  (y_start, y_start + y_increment), facecolors=colors)
    y_ticks.append(y_start + 5)
    y_labels.append(str(i))
    y_start += y_increment

ax.set_ylim(0, thread_count * 10)
ax.set_xlim(0, GRID_SIZE)
ax.set_xlabel('Ticks*')
ax.set_ylabel('CPU ID')
ax.set_yticks(y_ticks, y_labels)
plt.savefig('plot.png', dpi=300)
