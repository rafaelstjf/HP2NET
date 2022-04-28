import numpy as np
import os
import glob
import re
import argparse
import pygame
import math
import tkinter
import tkinter.messagebox
import tkinter.filedialog
from datetime import datetime
TIME_PATTERN = '%Y-%m-%d %H:%M:%S.%f'
WIDTH = 800
HEIGHT = 600
# initiate pygame font
pygame.font.init()


def create_numbers(screen, t0, tf, beg, end, interval):
    t0_c = datetime.strptime(t0, TIME_PATTERN)
    tf_c = datetime.strptime(tf, TIME_PATTERN)
    diftot_seconds = (tf_c - t0_c).total_seconds()
    font = pygame.font.SysFont(pygame.font.get_default_font(), 16)
    numbers = np.arange(beg, end+1, interval)
    i = 1
    for number in numbers:
        #pygame.draw.rect(screen, color=(0,0,0), rect=(i, HEIGHT-26, 1, 2))
        img = font.render(
            ('{0:.1f}'.format((number*diftot_seconds)/WIDTH)), True, (0, 0, 0))
        screen.blit(img, (i, HEIGHT - 24))

        i += (end)/len(numbers)


def convert_timestamp(t0, tf, tx):
    beg = datetime.strptime(t0, TIME_PATTERN)
    end = datetime.strptime(tf, TIME_PATTERN)
    x = datetime.strptime(tx, TIME_PATTERN)
    diftot_seconds = (end - beg).total_seconds()
    dif_seconds = (x - beg).total_seconds()
    return ((WIDTH * dif_seconds) / diftot_seconds)


def get_timestamp_dif(t0, tx):
    beg = datetime.strptime(t0, TIME_PATTERN)
    x = datetime.strptime(tx, TIME_PATTERN)
    dif_seconds = (x - beg).total_seconds()
    return dif_seconds


def get_color(name):
    if name == 'raxml':
        return pygame.Color("#E04D01")
    elif name == 'snaq':
        return pygame.Color("#3A3845")
    elif name == 'iqtree':
        return pygame.Color("#61A4BC")
    elif name == 'mrbayes':
        return pygame.Color("#F10086")
    elif name == 'bucky':
        return pygame.Color("#E0DDAA")
    elif name == 'mbsum':
        return pygame.Color("#603601")
    elif name == 'maxcut':
        return pygame.Color("#270082")
    elif name == 'phylonet':
        return pygame.Color("#8B9A46")
    elif name == 'plot':
        return pygame.Color("#F05454")
    elif name == 'astral':
        return pygame.Color("#9A0680")
    else:
        return pygame.Color("#383838")


def draw_rectangles(screen, tasks, task_times, t0, tf):
    tasks_sorted = dict(sorted(tasks.items()))
    run = -1
    y_axis = -1
    if len(tasks.keys()) > (HEIGHT/2):  # that means at least 2 pixels per rec
        rect_size = (HEIGHT/(0.6*len(tasks.keys())))
    else:
        rect_size = (HEIGHT/len(tasks.keys()))
    rect_list = list()
    name_list = list()
    for task in tasks_sorted:
        beg = convert_timestamp(t0, tf, task_times[task][0])
        end = convert_timestamp(t0, tf, task_times[task][1])
        dim = end - beg
        if run > beg:
            y_axis += 1
            run = max(end, run)
        else:
            y_axis = 0
            run = end
        rect_list.append(pygame.draw.rect(screen, color=get_color(
            tasks[task]), rect=(beg, (rect_size+0.2)*y_axis, dim, (rect_size/(1)))))
        name_list.append(
            f'Task {task}: {tasks[task]}\nTime spent: {get_timestamp_dif(task_times[task][0], task_times[task][1]):.2f} seconds')
    return (rect_list, name_list)


def parse_tasks(runinfo_dir):
    tasks = dict()
    tasks_file_path = os.path.join(
        runinfo_dir, os.path.join('task_logs', '0000'))
    tasks_files = glob.glob(os.path.join(tasks_file_path, "*"))
    for task in tasks_files:
        task_array = os.path.basename(task).split('_')
        task_name = (task_array[len(task_array) - 1]).split('.')[0]
        if task_name == 'raxml':
            tasks[task_array[1]] = 'raxml'
        elif task_name == 'snaq':
            tasks[task_array[1]] = 'snaq'
        elif task_name == 'iqtree':
            tasks[task_array[1]] = 'iqtree'
        elif task_name == 'mrbayes':
            tasks[task_array[1]] = 'mrbayes'
        elif task_name == 'bucky':
            tasks[task_array[1]] = 'bucky'
        elif task_name == 'mbsum':
            tasks[task_array[1]] = 'mbsum'
        elif task_name == 'maxcut':
            tasks[task_array[1]] = 'maxcut'
        elif task_name == 'phylonet':
            tasks[task_array[1]] = 'phylonet'
        elif task_name == 'plot':
            tasks[task_array[1]] = 'plot'
        elif task_name == 'astral':
            tasks[task_array[1]] = 'astral'
        else:
            tasks[task_array[1]] = 'other'
    return tasks


def parse_times(runinfo_dir, tasks):
    log_path = os.path.join(runinfo_dir, 'parsl.log')
    time_launch = list()
    time_complete = list()
    with open(log_path, 'r') as log:
        for line in log.readlines():
            if re.search('Task \d+ launched', line):
                time_launch.append(line)
            elif re.search('Task \d+ completed', line):
                time_complete.append(line)
        log.close()
    task_times = dict()
    for task in tasks.keys():
        launch = ""
        complete = ""
        for t in time_launch:
            if re.search(f'Task {int(task)}', t):
                launch = re.match('\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', t).group(0)
                break
        for t in time_complete:
            if re.search(f'Task {int(task)}', t):
                complete = re.match(
                    '\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', t).group(0)
                break
        if not (launch == "") and not (complete == ""):
            task_times[task] = (launch, complete)
    return task_times


def get_times(dir_):
    log = os.path.join(dir_, 'parsl.log')
    first_line = ""
    last_line = ""
    with open(log, "rb") as file:
        try:
            file.seek(0)
            first_line = file.readline().decode()
            file.seek(-2, os.SEEK_END)
            while file.read(1) != b'\n':
                file.seek(-2, os.SEEK_CUR)
        except OSError:
            file.seek(0)
        last_line = file.readline().decode()
    time_begin = re.match('\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', first_line).group(0)
    time_end = re.match('\d+-\d+-\d+\s\d+:\d+:\d+\.\d+', last_line).group(0)
    return (time_begin, time_end)


def open_folder():
    top = tkinter.Tk()
    top.withdraw()
    folder = tkinter.filedialog.askdirectory(parent=top)
    top.destroy()
    return folder


def create_dialog(message):
    diag = tkinter.Tk()
    diag.withdraw()
    message_box = tkinter.messagebox.showinfo(
        title='Task', message=message, parent=diag)
    diag.destroy()


def main(dir_):
    background_colour = (255, 255, 255)
    pygame.display.set_caption(os.path.basename(dir_))
    tasks = parse_tasks(dir_)
    tasks_time = parse_times(dir_, tasks)
    screen = pygame.display.set_mode((WIDTH, HEIGHT))
    screen.fill(background_colour)
    beg = 0
    time_begin, time_end = get_times(dir_)
    end = convert_timestamp(time_begin, time_end, time_end)
    create_numbers(screen, time_begin, time_end, beg, end, math.ceil(WIDTH/10))
    rect_list, name_list = draw_rectangles(
        screen, tasks, tasks_time, time_begin, time_end)
    pygame.display.flip()
    running = True
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            if event.type == pygame.MOUSEBUTTONUP:
                for i in range(0, len(rect_list)):
                    if rect_list[i].collidepoint(pygame.mouse.get_pos()):
                        create_dialog(name_list[i])
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Extract informations from parsl runinfo log files')
    parser.add_argument('-d', '--dir', type=str,
                        help='Runinfo folder', required=False)
    args = parser.parse_args()
    if args.dir is not None:
        runinfo_dir = args.dir
    else:
        runinfo_dir = open_folder()

    main(runinfo_dir)
