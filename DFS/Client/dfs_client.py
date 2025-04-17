# -*- coding: utf-8 -*-
"""
=================== CLIENT WITH LOCKING ===================
"""
import os, rpyc
import pandas as pd
import ftplib
import io
from tabulate import tabulate

# cpath = r"C:\Users\Arup Sau\Desktop\DFS\DistributedFileSystem-main\DFS\Client"
cpath = r"/tmp/DFS/Client"
def connect(host, port, flag, file):
    print("Host: {}, Port:{}, Opt Code: {}, filename: {}".format(host, port, flag, file))
    try:
        ftp = ftplib.FTP('')
        print("connecting...\n")
        ftp.connect(host, int(port))
        print("connected\n")
        while True:
            username = input("Please enter FTP username: ")
            passwd = input("Please enter password for {} : ".format(username))
            try:
                ftp.login(str(username), str(passwd))
                print("Connected to FTP Server {}:{}".format(host, port))
                print("Files in cwd:\n")
                ftp.dir()
                if flag == 2:
                    retcode = upload(ftp, file)
                elif flag == 3:
                    retcode = download(ftp, file)
                elif flag == 4:
                    retcode = deletefile(ftp, file)
                elif flag == 5:
                    retcode = append_to_file(ftp, file)
                elif flag == 6:
                    retcode = readfile(ftp, file)
                return retcode
            except:
                print("Incorrect Username or Password. Please try again...")
    except ftplib.all_errors as error:
        print(error)
        return error

def upload(ftp, ufile):
    try:
        print("Starting upload....")
        full_path = os.path.join(cpath, ufile)
        print(ufile)
        ftp.storbinary("STOR " + ufile, open( full_path , 'rb'))
        print("File", ufile, "uploaded successfully.")
        ftp.dir()
        ftp.quit()
        return '250-Requested file action completed'
    except ftplib.all_errors as error:
        print(error)
        return "426-Connection closed; File action aborted"

def append_to_file(ftp, filename):
    try:
        print("Enter text to append (Press Ctrl+Z then Enter to finish):")
        lines = []
        while True:
            try:
                line = input()
                lines.append(line)
            except EOFError:
                break
        data = '\n'.join(lines) + '\n'
        ftp.storbinary(f"APPE {filename}", io.BytesIO(data.encode()))
        print(f"Appended {len(data)} bytes to {filename}")
        return '250-Append operation completed'
    except Exception as e:
        return f'450-Append failed: {str(e)}'

def readfile(ftp, rfile):
    try: 
        content = []
        ftp.retrlines("RETR " + rfile, content.append)
        print("\n--- FILE CONTENT START ---")
        print('\n'.join(content))
        print("--- FILE CONTENT END ---\n")
        ftp.quit()
        return '250-Requested file read completed'
    except ftplib.all_errors as error:
        print(error)
        return "426-Connection closed; File action aborted"

def download(ftp, dfile):
    try:
        ftp.retrbinary("RETR " + dfile, open(dfile, 'wb').write, 1024)
        print("Downloaded", dfile, "successfully.")
        ftp.quit()
        return '250-Requested file action completed'
    except ftplib.all_errors as error:
        print(error)
        return "426-Connection closed; File action aborted"

def deletefile(ftp, xfile):
    try:
        ftp.delete(xfile)
        print("Deleted", xfile, "successfully.")
        ftp.dir()
        ftp.quit()
        return '250-Requested file action completed'
    except ftplib.all_errors as error:
        print(error)
        return "426-Connection closed; File action aborted"

def getlocalfiles():
    flist = []
    for (dirpath, dirnames, filename) in os.walk(cpath):
        for file in filename:
            flist.append(file)
   
    return flist

def get_DNode_info(master):
    dnode = master.select_dn()
    print("""Selected Data Node:
        Node_IP:{}
        Node_Port:{}
        Node_Directory: {}\n""".format(dnode[0], dnode[1], dnode[2]))
    return (dnode[0], dnode[1], dnode[2])

def perform_with_lock(master, filename, mode, fn):
    if master.acquire_lock(filename, mode):
        try:
            return fn()
        finally:
            master.release_lock(filename, mode)
    else:
        print(f" File is locked for {mode}. Try again later.")
        return None

def main():
    host = input("Enter the server IP [Default = 127.0.0.1]:") # Requests the user to input the IP of the Master server to be connected
    if host:
        pass                                                   # In case of user input, do nothing 
    else:
        host = "127.0.0.1"                                     # IN case of NO User input, set default Master server IP         
    port = input("Enter the server port [Default = 18812]:")   # Requests the user to input the Port number of the Master server to be connected
    if port:
         pass                                                  # In case of user input, do nothing
    else:
        port = 18812                                           # IN case of NO User input, set default Master server port
    con=rpyc.connect(host,port)                                # Connect to the Master server IP: Port
    print ("Connected to MasterServer [{}]:{}".format(host,port))
    master=con.root.Master()                                   # Allows calling of exposed remote Master server functions
                     
    instructions = """Please select an option [0-6]:
    1. Get file list
    2. Upload a file
    3. Download a file
    4. Delete a file
    5. Append and Write
    6. Read a file
    0. Quit\n
Input Option >>> """

    while master:
        try:
            opt = int(input(instructions))
            if opt == 0:
                break
            elif opt == 1:
                flist = master.filemap()
                print(tabulate(flist, headers='keys', tablefmt='psql'))
                # with pd.option_context('max_colwidth', 1000, 'display.max_columns', 500):
                #     print(flist)
            elif opt == 2:
                print ("Fetching upload server details...")
                DN_HOST,DN_PORT,DN_PATH = get_DNode_info(master) # Get a random DNode server to upload the file
                ftpport = DN_PORT + 1                            # FTP port for each DNode is the following port of the DNode rpyc server
                localfiles = getlocalfiles()                     # Prints the contents & total files in the client directory for user to select
                print (localfiles)
                ufile = input("Please enter the filename to upload to DNode: ") # Request user to enter the filename to be uploaded
                print(ufile)
                if ufile in localfiles:                                         # Check if the user input filename exists in the local directory
                    try:
                        print ("Connecting to ftpserver...")
                        msgcode = connect(DN_HOST, ftpport, opt, str(ufile))    # If file exists, share the required args to Connect() function
                        print (msgcode, "\n\n")                                 # Print the returned code from the Connect() function    
                    except ftplib.error_perm as error:
                        print (error)                                           # Print any file permission errors encountered during upload
                else:
                    print ("No such file found at ", cpath, "\n\n\n")           # Print error message if no file found
            elif opt == 3:
                key = input("Search keyword [Enter to list all]: ")
                match = master.Matchfile(key) if key else master.filemap()
                print(pd.DataFrame(match))
                dfile = input("Enter filename to download: ")
                md = pd.DataFrame(master.Matchfile(dfile))
                host, port = md.at[0, 'DN_IP'], int(md.at[0, 'DN_Port']) + 1
                perform_with_lock(master, dfile, 'read', lambda: connect(host, port, opt, dfile))
            elif opt == 4:
                flist = master.filemap()
                print(flist)
                xfile = input("Enter file to delete: ")
                xf = pd.DataFrame(master.Matchfile(xfile))
                host, port = xf.at[0, 'DN_IP'], int(xf.at[0, 'DN_Port']) + 1
                perform_with_lock(master, xfile, 'write', lambda: connect(host, port, opt, xfile))
            elif opt == 5:
                flist = master.filemap()
                print(flist)
                target_file = input("Enter filename to append: ")
                xf = pd.DataFrame(master.Matchfile(target_file))
                host, port = xf.at[0, 'DN_IP'], int(xf.at[0, 'DN_Port']) + 1
                perform_with_lock(master, target_file, 'write', lambda: connect(host, port, opt, target_file))
            elif opt == 6:
                key = input("Search keyword to read [Enter to list all]: ")
                match = master.Matchfile(key) if key else master.filemap()
                print(pd.DataFrame(match))
                rfile = input("Enter filename to read: ")
                rf = pd.DataFrame(master.Matchfile(rfile))
                print("connecting...\n")
                host, port = rf.at[0, 'DN_IP'], int( rf.at[0, 'DN_Port']) + 1
                print("connected...\n")
                perform_with_lock(master, rfile, 'read', lambda: connect(host, port, opt, rfile))
                print("connecting...\n")
        except Exception as e:
            print("Error:", e)

    print("Quitting Program. Thank you!")

if __name__ == "__main__":
    main()
