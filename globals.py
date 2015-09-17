import logging
import logging.config
import os, sys
import inspect


__all__ = ["config",  "cmd_args"]

# add 'lib' directory to PYTHON paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + os.sep + 'lib')

#global logger
#logger = logger.main().getLogger('svn-tools')


logging.config.fileConfig(os.path.realpath(os.path.dirname(sys.argv[0])) + os.sep + 'logger.conf')

def getLogger(p_file_name = ''):
    if p_file_name == '':
        v_file_name = inspect.stack()[1][1]
    else:
        v_file_name = p_file_name
    return logging.getLogger(v_file_name)


import config
from utils import CmdParser

#global config
config = config.main()

#global cmd_args
cmd_args = CmdParser.main()





