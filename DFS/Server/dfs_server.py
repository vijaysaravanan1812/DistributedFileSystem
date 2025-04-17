# -*- coding: utf-8 -*-
"""
=================== SERVER ===================
"""
import random, rpyc
import pandas as pd
import threading
from rpyc.utils.server import ThreadedServer
from rpyc.lib import setup_logger
import sys
class MasterServer(rpyc.Service):
    # Shared lock table for all connections
    lock_table = {}
    lock_table_lock = threading.Lock()

    class exposed_Master():
 
        DN_LIST = [["127.0.0.1",8888,r'/tmp/DFS/Dnode2'],       # Define the expected number of Datanodes ...
                   ["127.0.0.1",8000,r'/tmp/DFS/Dnode2']]       # ... that will be used for file storage
        # Client functions
        def exposed_Matchfile(self, key):
            mf = self.exposed_filemap()
            result = mf[mf['Name'].str.contains(key, case=False, na=False)]
            print(result)
            rdict = result.to_dict('records')
            print(rdict)
            return rdict

        def exposed_select_dn(self):
            return random.choice(self.DN_LIST)

        def exposed_filemap(self):
            filetable = []
            for DN in self.DN_LIST:
                try:
                    dcon = rpyc.connect(DN[0], DN[1])
                    dn = dcon.root.DNode()
                    filemap = list(dn.filequery())
                    filetable.extend(filemap)
                except Exception as e:
                    print(f"Error contacting DN {DN[0]}:{DN[1]}: {e}")
            df = pd.DataFrame(filetable)
            print(df.to_string(index=False))
            return df

        def exposed_acquire_lock(self, filename, mode):  # mode: 'read' or 'write'
            with MasterServer.lock_table_lock:
                if filename not in MasterServer.lock_table:
                    MasterServer.lock_table[filename] = {'readers': 0, 'writer': False}

                lock = MasterServer.lock_table[filename]

                if mode == 'read':
                    if lock['writer']:
                        print(f"[LOCK] Read blocked on '{filename}' due to active writer.")
                        return False
                    lock['readers'] += 1
                    print(f"[LOCK] Read lock acquired on '{filename}'. Readers: {lock['readers']}")
                    return True

                elif mode == 'write':
                    if lock['writer'] or lock['readers'] > 0:
                        print(f"[LOCK] Write blocked on '{filename}' due to active readers/writer.")
                        return False
                    lock['writer'] = True
                    print(f"[LOCK] Write lock acquired on '{filename}'.")
                    return True

        def exposed_release_lock(self, filename, mode):
            with MasterServer.lock_table_lock:
                if filename in MasterServer.lock_table:
                    lock = MasterServer.lock_table[filename]
                    if mode == 'read':
                        lock['readers'] -= 1
                        print(f"[LOCK] Read lock released on '{filename}'. Remaining readers: {lock['readers']}")
                    elif mode == 'write':
                        lock['writer'] = False
                        print(f"[LOCK] Write lock released on '{filename}'.")

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Error: Not enough arguments.")
        print("Usage: python script.py <arg1> <arg2> ")
        sys.exit(1) 

    # host = "127.0.0.1"
    host =  sys.argv[1] # Ip Address
    port =  sys.argv[2] # port
   
    if port and host:
        port = int(port)

    t = ThreadedServer(MasterServer, hostname=host, port=port, protocol_config={'allow_public_attrs': True})
    setup_logger(quiet=False, logfile=None)
    print(f"Master Server with locking started on port {port}...")
    t.start()
