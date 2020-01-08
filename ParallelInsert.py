import argparse
import pandas as pd
import multiprocessing as mp
import threading as mt
import os
import time
import random
import shutil
import concurrent.futures
from azure.cosmos import CosmosClient

def prcoess_file(filename, maxConcurrencyPerFile, chunksize, connectionString, database, container):
    print('Process file {fn} - PID: {PID} [{ParentPID}] (Max. Concurrency: {mc} Chunksize: {cs})'.format(
        fn=filename,
        PID = mp.current_process().pid,
        ParentPID = mp.current_process().name + "-->" + mp.Process().name,
        mc = maxConcurrencyPerFile,
        cs = chunksize))
    import_file(filename, maxConcurrencyPerFile, chunksize, connectionString, database, container)

def import_file(filename, maxConcurrencyPerFile, chunksize, connectionString, database, container):
    print('Start {fn}'.format(fn=filename))
    iteration = 0
    client = CosmosClient.from_connection_string(connectionString)
    database = client.get_database_client(database)
    container = database.get_container_client(container)
    with concurrent.futures.ThreadPoolExecutor(max_workers=maxConcurrencyPerFile) as executor:
        for df in pd.read_csv(filename, chunksize=chunksize, iterator=True, header=None, names=['id', 'payload']):
            iteration+=1
            executor.submit(import_chunk, df, filename, iteration, container)
        executor.shutdown(wait=True)
    print('Finished {fn}'.format(fn=filename))

def import_chunk(df, filename, iteration, container):
    print('[{fn} - {i}] Import Chunk of size: {l}'.format(
        fn = filename,
        i = iteration,        
        l = df.shape))
    records = df.to_dict('records')
    for record in records:
        container.upsert_item(record)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generating test data')
    parser.add_argument('--filenames', type=str, required=True, help='The "|" delimited list of filenames of the csv files to be imported')
    parser.add_argument('--connectionstring', type=str, required=True, help='The CosmosDB connection string')
    parser.add_argument('--chunksize', type=int, required=True, help='the chunk size used when processing the csv file')
    parser.add_argument('--maxConcurrencyPerFile', type=int, required=True, help='the number of threads that should be used to process a single file in parallel')
    parser.add_argument('--database', type=str, required=True, help='The CosmosDB database the data should be ingested to')
    parser.add_argument('--container', type=str, required=True, help='The CosmosDB container/collection the data should be ingested to')
    parser.add_argument('--linecount', type=int, required=False, help='the number of lines the geenrated CSV file will have')
    parser.add_argument('--filename', type=str, required=False, help='The filename of the generated csv file')
    args = parser.parse_args()

    print('Importing files {fn} into CosmosDB with chunk size {cs}'.format(fn=args.filenames, cs=args.chunksize))
    filenames = args.filenames.split("|")
    cpuCount = mp.cpu_count()
    poolSize = min(cpuCount, len(filenames))
    pool = mp.Pool(poolSize)
    with pool:
        print('CPU count: {cpc}'.format(cpc=cpuCount))

        arguments = []
        for filename in filenames:
            arguments.append((filename, args.maxConcurrencyPerFile, args.chunksize, args.connectionstring, args.database, args.container))

        pool.starmap(prcoess_file, arguments)

