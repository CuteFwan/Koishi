def delta_to_str(start, end):
    msg = ''
    if end < start:
        end, start = start, end
        msg += '-'
    diff = end - start
    d, s = divmod(diff.total_seconds(), 86400)
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