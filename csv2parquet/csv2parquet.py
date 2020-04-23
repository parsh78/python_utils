import os
import pandas as pd
import concurrent.futures


def convert_csv2parquet(thisdir):
    file_list = list()
    # r=root, d=directories, f = files
    for r, d, f in os.walk(thisdir):
        for file in f:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(r, file)
                file_list.append(csv_file_path)
    #Process with Max 6 Threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(process_file, iter(file_list))


def process_file(csv_file_path):
    print('Processing: {0}'.format(csv_file_path))
    parquet_file_path = csv_file_path + '.parquet'
    df = pd.read_csv(csv_file_path)
    df.to_parquet(parquet_file_path)
    print('Completed: {0}'.format(parquet_file_path))


if __name__ == "__main__":
    # Getting the current work directory (cwd)
    thisdir = os.getcwd()
    convert_csv2parquet(thisdir)
