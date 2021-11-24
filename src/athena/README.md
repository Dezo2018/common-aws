# Commong Scripts

## Running Querys

Run the script with -h argument to get a list of arguments and description.

## Basic Commands

1. The following command will scan the logs based on
   1. The destination cidrs 10.10.10.0/26 and 10.10.8.0/26 (So all ips in those ranges)
   2.

```
python flow-log.py --dstaddr 10.10.10.0/26 10.10.8.0/26 --isolate-by srcaddr --sort-by srcaddr dstaddr
```

## Setup Repository

Clone the repository

Create a virtual environment

```bash
python3 -m venv venv
```

Activate the virtual environment

```bash
source venv/bin/activate
```

Install dependencies

```bash
pip install -r requirements.txt
```
