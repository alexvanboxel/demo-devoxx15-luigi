__author__ = 'alexvanboxel'

from datetime import date
from datetime import timedelta


def month(current):
    return date(current.year, current.month, 15)


def month_first_day(current):
    return date(current.year, current.month, 1)

def month_last_day(current):
    d = next_month(current)
    return date(d.year, d.month, 1) - timedelta(days=1)


def rec_day_range(collect, current, stop):
    value = current
    collect.append(value)
    if current == stop:
        return collect
    elif current > stop:
        return collect
    else:
        return rec_day_range(collect, next_day(current), stop)


def rec_month_range(collect, current, stop):
    value = month(current)
    collect.append(value)
    if current == stop:
        return collect
    elif current > stop:
        return collect
    else:
        return rec_month_range(collect, next_month(current), stop)


def rec_year_range(collect, current, stop):
    value = month(current)
    collect.append(value)
    if current == stop:
        return collect
    elif current > stop:
        return collect
    else:
        return rec_year_range(collect, next_year(current), stop)


def day_range(range_from, range_till):
    part_from = str(range_from).split('-')
    part_till = str(range_till).split('-')
    start = date(int(part_from[0]), int(part_from[1]), int(part_from[2]))
    stop = date(int(part_till[0]), int(part_till[1]), int(part_till[2]))
    return rec_day_range([], start, stop)


def month_range(range_from, range_till):
    part_from = str(range_from).split('-')
    part_till = str(range_till).split('-')
    start = date(int(part_from[0]), int(part_from[1]), 15)
    stop = date(int(part_till[0]), int(part_till[1]), 15)
    return rec_month_range([], start, stop)


def year_range(range_from, range_till):
    part_from = str(range_from).split('-')
    part_till = str(range_till).split('-')
    start = date(int(part_from[0]), 1, 15)
    stop = date(int(part_till[0]), 1, 15)
    return rec_year_range([], start, stop)


def this_month():
    return month(date.today())


def last_month():
    return prev_month(this_month())


def next_month(current):
    return month(month(current) + timedelta(days=30))


def next_year(current):
    return month(month(current) + timedelta(days=365))


def prev_month(current):
    return month(month(current) - timedelta(days=30))

def substract_month(current, m):
    if m == 0:
        return current
    else:
        return substract_month(prev_month(current), m-1)


def prev_year(current):
    return month(month(current) - timedelta(days=365))

def last_year():
    return prev_year(this_month())


def year_first_month(current):
    m = month(current)
    return date(m.year, 1, 15)


def yester_day():
    return prev_day(date.today());

def to_day():
    return date.today();

def prev_day(current):
    return current - timedelta(days=1)


def next_day(current):
    return current + timedelta(days=1)


def end_of_month(current):
    """Return the current day when it's the last day of the month, otherwise return
    a day from previous month. Has only month precision."""
    if next_day(current).month != current.month:
        return current
    else:
        return prev_month(current)


def generate_range_from_argv(argv, last):
    if argv[0] == 'from':
        part = argv[1].partition('-');
        return month_range(part[0], part[2], last.year, last.month)
    elif argv[0] == 'range':
        part1 = argv[1].partition('-');
        part2 = argv[2].partition('-');
        return month_range(part1[0], part1[2], part2[0], part2[2])
    elif argv[0] == 'month':
        part = argv[1].partition('-');
        return month_range(part[0], part[2], part[0], part[2])
    elif argv[0] == 'last':
        month = last_month()
        return month_range(month.year, month.month, month.year, month.month)
    elif argv[0] == 'this':
        month = this_month()
        return month_range(month.year, month.month, month.year, month.month)
    else:
        print "Known modes are: from, range"

