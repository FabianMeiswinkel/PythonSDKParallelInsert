import argparse
import pandas as pd
import multiprocessing as mp
import threading as mt
import os
import time

def prcoess_file(filename):
    print('Start {fn}'.format(fn=filename))
    time.sleep(10)
    print('Finished {fn}'.format(fn=filename))

parser = argparse.ArgumentParser(description='Generating test data')
parser.add_argument('--filenames', type=str, required=True, help='The "|" delimited list of filenames of the csv files to be imported')
parser.add_argument('--connectionstring', type=str, required=True, help='The CosmosDB connection string')
parser.add_argument('--chunksize', type=int, required=True, help='the chunk size used when processing the csv file')
parser.add_argument('--maxConcurrencyPerFile', type=int, required=True, help='the number of threads that should be used to process a single file in parallel')
parser.add_argument('--linecount', type=int, required=False, help='the number of lines the geenrated CSV file will have')
parser.add_argument('--filename', type=str, required=False, help='The filename of the generated csv file')
args = parser.parse_args()

print('Importing files {fn} into CosmosDB with chunk size {cs}'.format(fn=args.filenames, cs=args.chunksize))
filenames = str.split("|")
cpuCount = mp.cpu_count()
print('CPU count: {cpc}'.format(cpc=cpuCount))
fileSemaphore = mp.BoundedSemaphore(cpuCount)
https://www.ellicium.com/python-multiprocessing-pool-process/
for filename in filenames:
    fileSemaphore.acquire()
    prcoess_file(filename)
    fileSemaphore.release()

