def delta_to_str(start, end):
    msg = ''
    if end < start:
        end, start = start, end
        msg += '-'
    diff = end - start
    d, s = divmod(int(diff.total_seconds()), 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    msg = ''
    if d != 0:
        msg += f'{d}d {h}h {m}m'
    elif h != 0:
        msg += f'{h}h {m}m'
    else:
        msg += f'{m}m'
    return msg

async def tabulate(data, max=35):
    d = []
    if not isinstance(data[0], list):
        for row in data:
            d.append([row])
    else:
        d = data
    rows = len(d)
    cols = len(d[0])
    max_wid = [0] * cols
    for row in d:
        for i, col in enumerate(row):
            max_wid[i] = len(str(col)[:max]) + 2 if len(str(col)) + 2 > max_wid[i] else max_wid[i]
    sep = '├' + '┼'.join('─' * wid for wid in max_wid) + '┤'
    start = '╒' + '╤'.join('═' * wid for wid in max_wid) + '╕'
    end = '╘' + '╧'.join('═' * wid for wid in max_wid) + '╛'
    table = [start]

    for i, row in enumerate(d):
        table.append('│' + '│'.join(f'{str(r)[:max]:^{max_wid[j]}}' for j, r in enumerate(row)) + '│')
        if not i:
            table.append(sep)
    table.append(end)
    return '\n'.join(table)