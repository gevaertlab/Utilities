"""
Author: Pritam Mukherjee
Date: 04/14/2020
Python version: 3.6

Purpose: To download images from TCIA. It takes as input the .tcia manifest files (and some optional arguments), downloads the images in zip format and unzips them. Note that this will only work with public repositories on TCIA where API keys are not required

"""

import os
from multiprocessing import Manager, Process, Pool
import requests
import time
from zipfile import ZipFile
import argparse


BASE_URL = 'https://services.cancerimagingarchive.net/services/v3/TCIA/query/getImage'


def extract_from_queue(out_dir, q, errors):
    print('started unzip process')
    while True:
        item = q.get()
        if item is None:
            break
        content = os.path.join(out_dir, item + '.zip')
        print("Unzipping series {}".format(item))
        unzip_path = os.path.join(out_dir, item)
        if not os.path.exists(unzip_path):
            os.makedirs(unzip_path)
        with ZipFile(content) as myzip:
            extracted = False
            try:
                myzip.extractall(path=unzip_path)
                extracted = True
            except Exception as e:
                errors.put(
                    'Exception encountered while unzipping! {} in processing {}'.format(
                        e, content))
        if extracted:
            try:
                os.unlink(content)
            except Exception as e:
                errors.put(
                    'Exception encountered while cleaning up zip file! {} in processing {}'.format(
                        e, content))
    errors.put(None)


def download_series(series_name, q, errors, out_dir):
    print('Downloading {}'.format(series_name))
    payload = {'SeriesInstanceUID': series_name}
    r = requests.get(BASE_URL, params=payload)
    if r.status_code != requests.codes.ok:
        errors.put(
            'Bad data: Status code {} in processing {}'.format(
                r.status_code, series_name))
        return
    file = os.path.join(out_dir, series_name + '.zip')
    with open(file, 'wb') as fp:
        fp.write(r.content)
    q.put(series_name)


def download_series_to_queue(series_list, q, errors, out_dir, n_jobs):
    print('Started download process')
    with Pool(n_jobs) as pool:
        pool.starmap(
            download_series, [
                (n, q, errors, out_dir) for n in series_list])
    q.put(None)


def writer(errors, errorlog):
    while True:
        if errors.empty:
            time.sleep(30)
        line = errors.get()
        if line is not None:
            with open(errorlog, 'a') as fp:
                fp.write(line + '\n')
        else:
            with open(errorlog, 'a') as fp:
                fp.write('No more errors to report')
            break


def extract_filenames_from_manifest(manifest_file):
    with open(manifest_file, 'r') as fp:
        all_lines = [l.strip() for l in fp.readlines()]
    return all_lines[6:]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('manifest_filepath')
    parser.add_argument(
        '--logfile',
        '-l',
        required=False,
        default='download.err')
    parser.add_argument('--outputdir', '-o', required=False, default='./')
    parser.add_argument('--n_jobs', required=False, type=int, default=10)
    args = parser.parse_args()
    manifest_file = args.manifest_filepath
    errorlog = args.logfile
    out_dir = args.outputdir
    n_jobs = args.n_jobs
    all_series = extract_filenames_from_manifest(manifest_file)
    all_series = all_series[0:2]
    with Manager() as manager:
        q = manager.Queue()
        errors = manager.Queue()
        downloader = Process(
            target=download_series_to_queue, args=(
                all_series, q, errors, out_dir, n_jobs))
        unzipper = Process(
            target=extract_from_queue, args=(
                out_dir, q, errors))
        error_writer = Process(target=writer, args=(errors, errorlog))
        downloader.start()
        unzipper.start()
        error_writer.start()
        downloader.join()
        unzipper.join()
        error_writer.join()
