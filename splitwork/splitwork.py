import os
import sys
import _split_merge


def get_rw_pair():
    fin, fout = os.pipe()
    os.set_inheritable(fin, True)
    os.set_inheritable(fout, True)
    return fin, fout


def fork_with_piped_io(f, child_close):
    fin_r, fin_w = get_rw_pair()
    fout_r, fout_w = get_rw_pair()
    pid = os.fork()
    if pid == 0:
        for x in child_close:
            os.close(x)
        os.close(fin_w)
        os.close(fout_r)
        try:
            f(fin_r, fout_w)
        finally:
            os.close(fin_r)
            os.close(fout_w)
            sys.exit()
    os.close(fin_r)
    os.close(fout_w)
    return pid, fin_w, fout_r


def _round_robin_input(file_in, files_out, child_close=None):
    if child_close is None:
        child_close = []
    pid = os.fork()
    if pid == 0:  # child
        for x in child_close:
            os.close(x)
        try:
            _split_merge.split_lines(file_in, [x for x in files_out])
        except Exception:
            exit_code = 1
        else:
            exit_code = 0
        os.close(file_in)
        for out in files_out:
            os.close(out)
        sys.exit(exit_code)
    return pid  # parent


def _round_robin_output(files_in, file_out=None, child_close=None):
    if child_close is None:
        child_close = []
    if file_out is not None:
        fout = file_out
        fin = None
    else:
        fin, fout = get_rw_pair()
    pid = os.fork()
    if pid == 0:  # child
        for x in child_close:
            os.close(x)
        if file_out is None:
            os.close(fin)
        try:
            _split_merge.merge_lines(fout, [x for x in files_in])
        except Exception:
            exit_code = 1
        else:
            exit_code = 0
        os.close(fout)
        for x in files_in:
            os.close(x)
        sys.exit(exit_code)
    # parent
    if file_out is not None:
        return pid, None
    os.close(fout)
    return pid, fin


def round_robin_split(func, file_in, file_out=None, N=1):
    ins = []
    outs = []
    pids = []
    for _ in range(N):
        pid, fin, fout = fork_with_piped_io(func, child_close=ins + outs)
        pids.append(pid)
        ins.append(fin)
        outs.append(fout)
    pid = _round_robin_input(file_in, ins, child_close=outs)
    pids.append(pid)
    pid, file_out = _round_robin_output(outs, file_out=file_out, child_close=ins)
    pids.append(pid)
    for x in ins + outs:
        os.close(x)
    return pids, file_out
