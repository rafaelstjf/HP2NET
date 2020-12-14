# Copyright (c) 2020, Diego Carvalho - d.carvalho@ieee.org
# All rights reserved.
#
# Adapted from psrecord.py - https://github.com/astrofrog/psrecord/blob/master/psrecord/main.py
# Copyright (c) 2013, Thomas P. Robitaille
# All rights reserved.

import time
import subprocess

children = []

def get_percent(process):
    return process.cpu_percent()

def get_memory(process):
    return process.memory_info()

def all_children(pr):
    global children

    try:
        children_of_pr = pr.children(recursive=True)
    except Exception:  # pragma: no cover
        return children

    for child in children_of_pr:
        if child not in children:
            children.append(child)

    return children


def monitor(pid, logfile=None, plot=None, duration=None, interval=None,
            include_children=False, title=None):

    # We import psutil here so that the module can be imported even if psutil
    # is not present (for example if accessing the version)
    import psutil

    pr = psutil.Process(pid)

    # Record start time
    start_time = time.time()
    
    # Declare f as None here just in case to make Pylance to stop complaining.
    f = None
    if logfile:
        f = open(logfile, 'w')
        f.write("# {0:12s} {1:12s} {2:12s} {3:12s}\n".format(
            'Elapsed time'.center(12),
            'CPU (%)'.center(12),
            'Real (MB)'.center(12),
            'Virtual (MB)'.center(12))
        )

    log = {}
    log['times'] = []
    log['cpu'] = []
    log['mem_real'] = []
    log['mem_virtual'] = []

    try:

        # Start main event loop
        while True:

            # Find current time
            current_time = time.time()

            try:
                pr_status = pr.status()
            except TypeError:  # psutil < 2.0
                pr_status = pr.status
            except psutil.NoSuchProcess:  # pragma: no cover
                break

            # Check if process status indicates we should exit
            if pr_status in [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD]:
                print("Process finished ({0:.2f} seconds)"
                      .format(current_time - start_time))
                break

            # Check if we have reached the maximum time
            if duration is not None and current_time - start_time > duration:
                break

            # Get current CPU and memory
            try:
                current_cpu = get_percent(pr)
                current_mem = get_memory(pr)
            except Exception:
                break
            current_mem_real = current_mem.rss / 1024. ** 2
            current_mem_virtual = current_mem.vms / 1024. ** 2

            # Get information for children
            if include_children:
                for child in all_children(pr):
                    try:
                        current_cpu += get_percent(child)
                        current_mem = get_memory(child)
                    except Exception:
                        continue
                    current_mem_real += current_mem.rss / 1024. ** 2
                    current_mem_virtual += current_mem.vms / 1024. ** 2

            if logfile:
                f.write("{0:12.3f} {1:12.3f} {2:12.3f} {3:12.3f}\n".format(
                    current_time - start_time,
                    current_cpu,
                    current_mem_real,
                    current_mem_virtual))
                f.flush()

            if interval is not None:
                time.sleep(interval)

            # If plotting, record the values
            if plot:
                log['times'].append(current_time - start_time)
                log['cpu'].append(current_cpu)
                log['mem_real'].append(current_mem_real)
                log['mem_virtual'].append(current_mem_virtual)

    except KeyboardInterrupt:  # pragma: no cover
        pass

    if logfile:
        f.close()

    if plot:

        # Use non-interactive backend, to enable operation on headless machines
        import matplotlib.pyplot as plt
        with plt.rc_context({'backend': 'Agg'}):

            fig = plt.figure()

            if title:
                fig.suptitle(title, fontsize=16)

            ax = fig.add_subplot(1, 1, 1)

            ax.plot(log['times'], log['cpu'], '-', lw=1, color='r')

            ax.set_ylabel('CPU (%)', color='r')
            ax.set_xlabel('time (s)')
            ax.set_ylim(0., max(log['cpu']) * 1.2)

            ax2 = ax.twinx()

            ax2.plot(log['times'], log['mem_real'], '-', lw=1, color='b')
            ax2.set_ylim(0., max(log['mem_real']) * 1.2)

            ax2.set_ylabel('Real Memory (MB)', color='b')

            ax.grid()

            fig.savefig(plot)

def spy_process(cmd, log, fig=False, interval=1, title=None):
    my_process = subprocess.Popen(cmd, shell=True)
    monitor(my_process.pid, 
            logfile=log, 
            plot=fig, 
            include_children=True, 
            interval=interval, 
            title= title)
    return
