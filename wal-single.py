import os
import sys

def goon(walFile, fileSize):
    global curLogId
    global pos

    import pdb; pdb.set_trace()
    logId = int.from_bytes(walFile.read(8), byteorder = "little", signed = True)
    term = int.from_bytes(walFile.read(8), byteorder = "little", signed = True)
    head = int.from_bytes(walFile.read(4), byteorder = "little", signed = True)
    clusterId = int.from_bytes(walFile.read(8), byteorder = "little", signed = True)
    log = walFile.read(head)
    foot = int.from_bytes(walFile.read(4), byteorder = "little", signed = True)
    print(logId)

    if logId != curLogId or head != foot:
        print("Actual log id ", logId, "Expect log id ", curLogId, "Term ", term, "Head ", head, "Cluster ", clusterId, "Log size ", len(log), "Foot ", foot, "pos ", hex(pos))
        print("Bad wal, truncate after ", pos)
        #walFile.truncate(pos)
        return False

    pos += 8 + 8 + 4 + 8 + head + 4
    curLogId += 1

    if pos == fileSize:
        print("Good wal")
        return False
    return True

def readFile(path):
    fileSize = os.path.getsize(path)
    with open(path, "rb+") as walFile:
        while True:
            if goon(walFile, fileSize) == False:
                break

if __name__ == "__main__":
    # path = "0000000000000007341.wal"
    path = "0000000000000279409.wal"
    curLogId = int(path[0 : path.find(".")])
    pos = 0
    readFile(path)
