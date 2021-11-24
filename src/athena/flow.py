import boto3
import pandas as pd
import os
import operator
import ipcalc
import argparse
import sys
import re
from functools import reduce

"""
Script to download flow logs from an input bucketname, based on a provided period.
This script can then be used to query the parsed flow logs based on:
- ingress cidr
- egress cidr
- ports
- cidr
- ip address

This script assumes the default synthax as seen here
${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}
@see https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html#flow-logs-fields

Steps v1
1. With the flow logs in the `flow-logs` dir, execute the script to scan the data
"""


def read_logs(log_path: str) -> pd.DataFrame:
    """ Go through the log path, read all log files, returns a single DF of the entries """

    log_dir = os.path.abspath(log_path)

    df = pd.DataFrame()
    for path, dirs, files in os.walk(log_dir):
        for file_name in files:
            df = df.append(read_log_file(f'{log_dir}/{file_name}'))

    return df


def convert_ip_str_to_tuple(ip_str: str) -> tuple[int, int, int, int]:
    """ 
    Convert an Ip into a tuple of int to allow for efficient and correct sorting 
    e.g. if left as a str, then the ip x.x.x.99, will come after the ip x.x.x.123 
        since 1 is less than 9 in a ascending sort
    """
    # ip_str not available or invalide (don't want to do regex here since records will be many)
    if not ip_str or ip_str == '-':
        return ip_str

    # tuples are harshable and can be indexed by pandas
    return tuple([int(ip) for ip in ip_str.split('.')])


def convert_ip_tuple_to_str(ip):
    if isinstance(ip, tuple):
        return '.'.join(map(str, list(ip)))

    return ip


def serialize_records(record: dict) -> dict:
    """ Serialize tuple is to standard format """

    record['srcaddr'] = convert_ip_tuple_to_str(record['srcaddr'])
    record['dstaddr'] = convert_ip_tuple_to_str(record['dstaddr'])

    return record


def read_log_file(file_path: str) -> pd.DataFrame:
    """ Read a single log file """

    return pd.read_csv(
        file_path,
        # dtype=str,  # convert every field to str
        converters={'srcaddr': convert_ip_str_to_tuple,
                    'dstaddr': convert_ip_str_to_tuple},
        delim_whitespace=True,  # delimiter is a whitespace or tab
    )


def get_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Script to query through VPC flow logs.",
        epilog='This script runs query on vpc flow logs based on the following headers, (with space separator) \n' +
        '${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status} \n' +
        '@see https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html#flow-logs-fields')
    parser.add_argument('--account-id')
    parser.add_argument('--interface-id')
    parser.add_argument('--srcaddr', nargs="*")
    parser.add_argument('--dstaddr', nargs="*")
    parser.add_argument(
        '--addr', nargs="*", help="If passed, will match 'srcaddr or dstaddr'")
    parser.add_argument('--srcport')
    parser.add_argument('--dstport')
    parser.add_argument(
        '--port', help="If passed, will match 'srcport or dstport'")
    parser.add_argument('--protocol')
    parser.add_argument('--action')
    parser.add_argument('--log-status')

    parser.add_argument('--log-path', default='./flow-logs')
    parser.add_argument('--results-dest', default='./results.log',
                        help='Where to send the results')

    parser.add_argument('--isolate-by', nargs='*', default=[],
                        help='Remove duplicates based on field, e.g. srcaddr dstaddr')
    parser.add_argument('--sort-by', nargs='*', default=[],
                        help='Sort by fields, e.g. srcaddr dstaddr')

    return parser.parse_args(sys.argv[1:])


def get_cidr_cond(df: pd.DataFrame, arg_name: str, arg_values: list[str]):
    """
    Get a list of IPs if a range is provided
    consider Ip 10.8.4.168, we search cidr 10.8.4.0/16, it should be included
    """
    ips = []
    for arg_value in arg_values:
        # check if its a cidr
        if re.search('(\/([0-9]|[1-2][0-9]|3[0-2]))$', arg_value):
            # second list is ips from that cidr
            ips = [
                *ips, *list(map(lambda x: convert_ip_str_to_tuple(str(x)), ipcalc.Network(arg_value)))]
        else:
            ips.append(convert_ip_str_to_tuple(arg_value))

    return df[arg_name].isin(ips)


if __name__ == "__main__":
    args = vars(get_arguments())  # convert the namespace to dict

    print(args)

    df = read_logs(args['log_path'])

    norm = ['account_id',
            'interface_id',
            'protocol',
            'action',
            'log_status', ]
    conds = [df[key.replace('_', '-')] == args[key]
             for key in filter(lambda x: args[x], norm)]

    if args['port']:
        conds.append(reduce(operator.or_,
                            [df['srcport'] == args['port'], df['dstport'] == args['port']]))
    else:
        if args['srcport']:
            conds.append(df['srcport'] == args['srcport'])
        if args['dstport']:
            conds.append(df['dstport'] == args['dstport'])

    if args['addr']:
        conds.append([get_cidr_cond(df, 'srcaddr', args['addr'])
                     or get_cidr_cond(df, 'dstaddr', args['addr'])])
    else:
        if args['srcaddr']:
            conds.append(get_cidr_cond(df, 'srcaddr', args['srcaddr']))
        if args['dstaddr']:
            conds.append(get_cidr_cond(df, 'dstaddr', args['dstaddr']))

    result_df = df.loc[reduce(operator.and_, conds)]

    # remove duplicates, isolate
    if len(args['isolate_by']) > 0:
        result_df = result_df.drop_duplicates(subset=args['isolate_by'])

    # sort the dataframe
    if len(args['sort_by']) > 0:
        result_df = result_df.sort_values(by=args['sort_by'])

    # serialize the ip tuples to standard ip strings
    result_df = pd.DataFrame(
        map(serialize_records, result_df.to_dict('records')))

    # output the results to results-dest arg
    result_df.to_csv(args['results_dest'], sep=' ', index=False)
