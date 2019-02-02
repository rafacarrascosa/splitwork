import os
import io
import sys
import itertools
import _split_merge


def get_rw_pair():
    fin, fout = os.pipe()
    os.set_inheritable(fin, True)
    os.set_inheritable(fout, True)
    fin = io.FileIO(fin, 'r')
    fout = io.FileIO(fout, 'w')
    return fin, fout


def fork_with_piped_io(f, child_close):
    fin_r, fin_w = get_rw_pair()
    fout_r, fout_w = get_rw_pair()
    pid = os.fork()
    if pid == 0:
        for x in child_close:
            x.close()
        fin_w.close()
        fout_r.close()
        try:
            f(fin_r, fout_w)
        finally:
            fin_r.close()
            fout_w.close()
            sys.exit()
    fin_r.close()
    fout_w.close()
    return pid, fin_w, fout_r


def _round_robin_input(file_in, files_out, child_close=None):
    if child_close is None:
        child_close = []
    pid = os.fork()
    if pid == 0:  # child
        for x in child_close:
            x.close()
        try:
            _split_merge.split_lines(file_in.fileno(), [x.fileno() for x in files_out])
        except:
            exit_code = 1
        else:
            exit_code = 0
        file_in.close()
        for out in files_out:
            out.close()
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
            x.close()
        if file_out is None:
            fin.close()
        try:
            _split_merge.merge_lines(fout.fileno(), [x.fileno() for x in files_in])
        except:
            exit_code = 1
        else:
            exit_code = 0
        fout.close()
        for x in files_in:
            x.close()
        sys.exit(exit_code)
    # parent
    if file_out is not None:
        return pid, None
    fout.close()
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
        x.close()
    return pids, file_out
