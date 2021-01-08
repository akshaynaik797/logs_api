import sys
import os
import inspect

directory = os.path.dirname(__file__)+'/logs/'

def log_exceptions(**kwargs):
    from datetime import datetime as akdatetime
    import traceback
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + os.path.split(os.path.relpath(inspect.stack()[1][1]))[1] + '_error.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        tb = traceback.format_exc()
        entry = ('===================================================================================================\n'
                 '%s'
                 '---------------------------------------------------------------------------------------------------\n'
                 'sys.args->%s'
                 '---------------------------------------------------------------------------------------------------\n'
                 'variables->%s\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 '%s\n') % (nowtime, sys.argv, str(kwargs), tb)
        fp.write(entry)

def log_data(**kwargs):
    from datetime import datetime as akdatetime

    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + os.path.split(os.path.relpath(inspect.stack()[1][1]))[1] + '_data.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        entry = ('===================================================================================================\n'
                 '%s\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 'sys.args->%s\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 'variables->%s\n') % (nowtime, sys.argv, str(kwargs))
        fp.write(entry)


def log_custom_data(**kwargs):
    from datetime import datetime as akdatetime
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + kwargs['filename'] + '_.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        entry = ('===================================================================================================\n'
                 '%s\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 'sys.args->%s\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 'variables->%s\n') % (nowtime, sys.argv, str(kwargs))
        fp.write(entry)
