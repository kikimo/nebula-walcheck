# coding: utf-8
import os
import bisect
from functools import reduce
from operator import add

# wal_dir = '/Users/wenlinwu/src/nebula-walcheck/data/storaged.0/nebula/1/wal/1'
# wal_dir = '/Users/wenlinwu/src/nebula-walcheck/data/storaged.1/nebula/1/wal/1'
wal_dir = '/Users/wenlinwu/src/nebula-walcheck/data/storaged.2/nebula/1/wal/1'

class LogEnt(object):
    def __init__(self, log_id, term, msg_sz, cluster_id, msg, wfile):
        self.log_id = log_id
        self.term = term
        self.msg_sz = msg_sz
        self.cluster_id = cluster_id
        self.msg = msg
        self.wfile = wfile

    def __str__(self):
        return 'log index: {}, term: {}, cluster_id: {}, walfile: {}'.format(self.log_id, self.term, self.cluster_id, self.wfile)

    def __lt__(self, other):
        return self.log_id < other.log_id

    def __eq__(self, other):
        return self.log_id == other.log_id

    def __gt__(self, other):
        return self.log_id > other.log_id

class Wal(object):
    def __init__(self, wfile, logs):
        self.wfile = wfile
        self.logs = logs

def load_entry(fp, wfile):
    # log_id = int.from_bytes(fp.read(8), byteorder = "little", signed = True)
    log_id = int.from_bytes(fp.read(8), byteorder = "little", signed = True)
    if log_id == 0:
        return None

    term = int.from_bytes(fp.read(8), byteorder = "little", signed = True)
    msg_sz = int.from_bytes(fp.read(4), byteorder = "little", signed = True)
    cluster_id = int.from_bytes(fp.read(8), byteorder = "little", signed = True)
    msg = fp.read(msg_sz)
    foot = int.from_bytes(fp.read(4), byteorder = "little", signed = True)
    assert(foot==msg_sz)

    ent = LogEnt(log_id=log_id, term=term, msg_sz=msg_sz, cluster_id=cluster_id, msg=msg, wfile=wfile)
    # print(log_id)
    # import pdb; pdb.set_trace()

    return ent

def do_load_wal(wal_file):
    fname = os.path.basename(wal_file)
    # fname = '0000000000000279409.wal'
    parts = fname.split('.')
    if len(parts) != 2:
        raise Exception('illegal wal file name: {}'.format(fname))

    start_log_id = int(parts[0])
    start = True
    logs = []
    with open(wal_file, 'rb+') as fp:
        while True:
            ent = load_entry(fp, wal_file)
            if ent is None:
                break

            # if ent.log_id == 17567:
            #     print('file 17567: {}'.format(wal_file))

            # if ent.log_id == 143903:
            #     print('file 143903: {}'.format(wal_file))

            # if ent.log_id == 17567:
            #     print('file 17567: {}'.format(wal_file))

            if ent.log_id == 2369:
                print('file 2369: {}'.format(wal_file))


            if start:
                if ent.log_id != start_log_id:
                    raise Exception('illegal start log id: {}, wal file: {}'.format(ent.log_id, fname))
                start = False

            logs.append(ent)

    check_single_wal(logs)
    return Wal(wfile=wal_file, logs=logs)

def check_single_wal(logs):
    sz = len(logs)
    if sz < 2:
        return True

    for i in range(1, sz):
        curr = logs[i]
        prev = logs[i-1]
        if curr.log_id <= prev.log_id or curr.term < prev.term:
            print('fuck idx {} and {}'.format(i-1, i))
            print('prev {}'.format(prev))
            print('curr {}'.format(curr))

def load_wal(wal_dir):
    wfiles = os.listdir(wal_dir)
    wal_list = []
    for f in wfiles:
        # print('loading wal file: {}'.format(f))
        wf = os.path.join(wal_dir, f)
        wal = do_load_wal(wf)
        if len(wal.logs) > 0:
            wal_list.append(wal)

    wal_list.sort(key=lambda wal: wal.logs[0].log_id)
    return wal_list

    # all_logs = [LogEnt(0, 0, 0, 0, 0)]
    # for logs in logs_list:
    #     first = logs[0]
    #     idx = first.log_id
    #     if len(all_logs) < idx:
    #         print("all logs: {}, idx: {}".format(len(all_logs), idx))
    #         raise Exception("fuck idx")

    #     all_logs = all_logs[:idx] + logs

    # # sorted(logs_list, key=lambda logs: logs[0].log_id)
    # logs = reduce(add, logs_list)
    # # for log in logs_list[0]:
    # for log in logs:
    #     print('log index: {}, term: {}, cluster: {}'.format(log.log_id, log.term, log.cluster_id))

    # return logs
wal_list = load_wal(wal_dir=wal_dir)
logs = [LogEnt(0, 0, 0, 0, 0, '')]
for wal in wal_list:
    first = wal.logs[0]
    if len(logs) < first.log_id:
        print('all logs: {}, idx: {}'.format(len(logs), first.log_id))
        print('prev log: {}'.format(logs[-1]))
        print('curr log: {}'.format(first))
        raise Exception("fuck idx")

    logs = logs[:first.log_id] + wal.logs
    # print('file: {}, sz: {}, log index: {}, term: {}'.format(wal.wfile, len(wal.logs), wal.logs[0].log_id, wal.logs[0].term))

# print('begin log:')
for i, log in enumerate(logs[1:]):
    print('log index: {}, term: {}, cluster: {}, wfile: {}'.format(log.log_id, log.term, log.cluster_id, log.wfile))
    if i != log.log_id-1:
        raise Exception('shit')

# check_single_wal(logs)
# print(wal_dir)
