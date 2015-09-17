import subprocess
from globals import getLogger
import os, glob



def makesystemcall (cmd):
    process = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)

    # wait for the process to terminate
    out, err = process.communicate()
    errcode = process.returncode

    return (out, err, errcode)



def get_directory_list(p_dir, p_list_entries='df'):
    entries = []
    v_get_dir =  p_list_entries.find('d') >= 0
    print 'v_get_dir', v_get_dir
    v_get_file = p_list_entries.find('f') >= 0
    print 'v_get_file', v_get_file

    for fn in glob.glob(p_dir + os.sep + '*'):
        if (v_get_file and os.path.isfile(fn)):
            entries.append( ('f', os.path.basename(fn)) )
        elif (v_get_dir and os.path.isdir(fn)):
            entries.append( ('d', os.path.basename(fn)) )

    entries = sorted(entries)

    return entries
