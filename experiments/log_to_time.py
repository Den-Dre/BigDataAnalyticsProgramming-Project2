#!/usr/bin/python

import sys
import os
from datetime import datetime

directory = '.'
result = ''
date_format = '%Y-%m-%d %H:%M:%S'
seconds = 0


def get_seconds(_lines):
    date_lines = list(filter(lambda l: '%' in l, _lines))
    first = datetime.strptime(date_lines[0].split(',')[0], date_format)
    last = datetime.strptime(date_lines[-1].split(',')[0], date_format)
    return (last - first).total_seconds()


for file in sorted(os.listdir(directory)):
    if os.path.isfile(os.path.join(directory, file)) and '.' not in file:
        with open(file, 'r') as f:
            print(f'Reading file {file}')
            lines = f.readlines()
            end_index = lines.index(list(filter(lambda l: 'successfully' in l, lines))[0])
            lines1, lines2 = lines[:end_index], lines[end_index:]
            seconds = get_seconds(lines1) + get_seconds(lines2)
            result += file + ":\t" + str(int(seconds // 60)) + 'm' + str(int(seconds % 60)) + 's\n'
print(result)


