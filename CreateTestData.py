import argparse
import sys
import uuid
import csv

parser = argparse.ArgumentParser(description='Generating test data')
parser.add_argument('--filename', type=str, required=True, help='The filename of the generated csv file')
parser.add_argument('--linecount', type=int, required=True, help='the number of lines the geenrated CSV file will have')
parser.add_argument('--connectionstring', type=str, required=False, help='The CosmosDB connection string')
parser.add_argument('--chunksize', type=int, required=False, help='the chunk size used when processing the csv file')
parser.add_argument('--database', type=str, required=False, help='The CosmosDB database the data should be ingested to')
parser.add_argument('--container', type=str, required=False, help='The CosmosDB container/collection the data should be ingested to')
parser.add_argument('--filenames', type=str, required=False, help='The "|" delimited list of filenames of the csv files to be imported')
parser.add_argument('--maxConcurrencyPerFile', type=int, required=False, help='the number of threads that should be used to process a single file in parallel')

args = parser.parse_args()

print('Generating test data file {fn} with {l} lines'.format(fn=args.filename, l=args.linecount))
with open(args.filename, 'w') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    
    for i in range(0, args.linecount):
        if (i % 1000 == 0):
            print(str(i))

        payload = ''
        for i in range(0, 20):
            payload += str(uuid.uuid1())

        filewriter.writerow((str(uuid.uuid1()), payload))
