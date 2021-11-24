import sys
import typing
import time
import random
from multiprocessing.pool import Pool
import subprocess

""" First argument is the stage (dev, qa...)"""


def get_bucket_pairs():
    """ Will get map of bucket names from the command line args """
    """ In the form source_full,destination """
    env_stage = sys.argv[1]

    pairs: typing.List[typing.List[str]] = []
    for _pair in sys.argv[2:]:
        source_full, destination = _pair.split(',')
        pairs.append(
            [f's3://{source_full}', f's3://scan-{env_stage}-{destination}'])

    return pairs


def sync_bucket(name_pair: typing.List[str]):
    # sync_command: typing.List[str] = ['aws', 's3', 'sync']
    sync_command: typing.List[str] = ['ls', '-la']
    # sync_command.extend(name_pair)

    print(f'Syncing from {name_pair[0]} to {name_pair[1]} started.')
    # time.sleep(random.randint(1, 20))
    subprocess.run(' '.join(sync_command), shell=True,  check=True)
    print(f'Syncing from {name_pair[0]} to {name_pair[1]} completed.')


def main():
    bucket_pairs = get_bucket_pairs()

    with Pool(len(bucket_pairs)) as p:
        p.map(sync_bucket, bucket_pairs)


if __name__ == '__main__':
    main()
