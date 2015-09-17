import argparse


def main():
    '''
        sctip_name  <action> <command> <parameters...>
        action : casino, games
        command : branch, merge, status
        
    '''
    parser = argparse.ArgumentParser(add_help=False)
    cmd_args = argparse.Namespace()


    #subparser_action
    subparser_action = parser.add_subparsers(dest='action')
    
    #subparser_action - CASINO
    subparser_casino = subparser_action.add_parser('database'
                                                   , help='for example: pilot1'
                                                   , add_help=True)
#    subparsers_casino_command = subparser_casino.add_subparsers(dest='command')
    
    subparser_casino.add_argument('dbname'
                               #, nargs='*'
                               , help='database name such as PILOT1')

    subparser_casino.add_argument('--type', '-t'
                                             , default='all'
                                             , choices=['all', 'fast']
                                             , required=True)



    '''
    print "========================================="
    parser.print_help()
    print "========================================="
    # retrieve subparser_action from parser
    subparsers_actions = [
        action for action in parser._actions 
        if isinstance(action, argparse._SubParsersAction)]
    # there will probably only be one subparser_action,
    # but better save than sorry
    for subparser_action in subparsers_actions:
        # get all subparser_action and print help
        for choice, subparser in subparser_action.choices.items():
            print("Subparser '{}'".format(choice))
            print(subparser.format_help())
            print "-----------------------------------------"
    
    
    
    #subparser_svn_branch.print_help()
    
    
    '''

#    subparser_casino_cmd_status.print_help()
    
    parser.parse_args(namespace=cmd_args)


    return cmd_args

#def main();


