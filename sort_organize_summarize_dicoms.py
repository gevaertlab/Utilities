import os
import sys
import time
from multiprocessing import Manager, Process, Pool
import pydicom
import argparse
import shutil
import numpy as np
import pandas as pd


TIMEOUT = 10
TAGS_OF_INTEREST = {
    'SeriesInstanceUID': str,
    'StudyInstanceUID': str,
    'PatientName': str,
    'PatientID': str,
    'Modality': str,
    'PatientSex': str,
    'SliceThickness': float,
    'PixelSpacing': lambda x: np.array(x).astype(np.float32),
    'ConvolutionKernel': str,
    'Rows': int,
    'Columns': int,
    'Manufacturer': str,
    'InstitutionName': str,
    'StudyDescription': str,
    'SeriesDescription': str,
    'KVP': float,
    'Exposure': int,
    'AccessionNumber': str,
    'ImageType': str,
    'MagneticFieldStrength': float,
    'EchoTime': float,
    'InversionTime': float,
    'ImagedNucleus': str,
    'ImagingFrequency': float,
    'NumberOfAverages': int,
    'SpacingBetweenSlices': float,
    'EchoTrainLength': int,
    'PercentPhaseFieldOfView': float,
    'PixelBandwidth': float,
    'ContrastBolusAgent': str,
    'ReconstructionDiameter': float
}


def progressbar(counts):
    #    print("Progress:{}".format(os.getpid()))
    while True:
        sys.stdout.write(
            '\r Number of files analyzed: {}, moved: {}'.format(
                counts[0], counts[1]))
        sys.stdout.flush()
        if 'MOVE_COMPLETE' in counts and 'ANALYSIS_COMPLETE' in counts:
            break
        time.sleep(TIMEOUT)


def errorlogger(errors_q, errorlog):
    #    print("Errors:{}".format(os.getpid()))
    while True:
        if errors_q.empty():
            time.sleep(TIMEOUT)
        item = errors_q.get()
        if item is None:
            line = 'No more errors to report'
            break
        else:
            line = item
        with open(errorlog, 'a') as fp:
            fp.write(line + '\n')


def mover(input_q, errors_q, counts, status):
    #    print("Mover:{}".format(os.getpid()))
    def rec_clean(path):
        parent = os.path.dirname(path)
        if len(list(os.listdir(path))) == 0:
            try:
                os.rmdir(path)
                rec_clean(parent)
            except Exception as e:
                errors_q.put('Error {} in cleaning up {}'.format(e, parent))

    while True:
        item = input_q.get()
        if item is None:
            break
        in_path, out_path = item
        try:
            os.makedirs(out_path)
        except FileExistsError:
            pass
        try:
            shutil.move(in_path, out_path)
            counts[1] += 1
        except Exception as e:
            errors_q.put('Error {} in moving {}'.format(e, in_path))
        rec_clean(os.path.dirname(in_path))
    status.append(1)


def summarizer(summary_q, summary_output):
    summary_dic = {k: [] for k in TAGS_OF_INTEREST}
    while True:
        item = summary_q.get()
        if item is None:
            break
        for k in TAGS_OF_INTEREST:
            summary_dic[k].append(item[k])
    df = pd.DataFrame(summary_dic)
    df.to_csv(summary_output)


def analyzer(
        input_q,
        output_q,
        errors_q,
        summary_q,
        dest,
        path_format,
        summarize,
        instances,
        all_series,
        counts,
        status):
    #    print("Analyzer:{}".format(os.getpid()))
    keys = path_format.split('/')
    while True:
        item = input_q.get()
        if item is None:
            break
        valid = False
        try:
            ds = pydicom.dcmread(item)
            out_path = os.path.join(
                *([dest] + [str(ds.data_element(k).value) for k in keys]))
            valid = True
        except pydicom.errors.InvalidDicomError:
            errors_q.put('Invalid dicom: {}'.format(item))
            out_path = os.path.join(dest, 'non-dicoms')
        except KeyError:
            errors_q.put('Did not find all required tags in {}'.format(item))
            out_path = os.path.join(dest, 'uncategorized_dicoms')
        if valid:
            first_encounter = False
            try:
                series = ds.SeriesInstanceUID
            except KeyError:
                errors_q.put(
                    "No series information...Skipping {}".format(item))
                continue
            if ds.SOPInstanceUID in instances:
                errors_q.put("Duplicate detected...Skipping {}".format(item))
                continue
            instances[ds.SOPInstanceUID] = 1
            if summarize and series not in all_series:
                summary_dic = {}
                all_series[series] = 1
                for k in TAGS_OF_INTEREST:
                    try:
                        val = TAGS_OF_INTEREST[k](
                            ds.data_element(k).value)
                    except (ValueError, KeyError):
                        val = np.nan
                    summary_dic[k] = val
                summary_q.put(summary_dic)

        output_q.put((item, out_path))
        counts[0] += 1
    status.append(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source_dir',
        help='The source directory containing all the dicoms')
    parser.add_argument(
        '--path_format',
        default='PatientID/StudyInstanceUID/SeriesInstanceUID')
    parser.add_argument('--n_jobs', type=int, default=10)
    parser.add_argument('--output_dir', help='Output directory', default=None)
    parser.add_argument(
        '--summarize',
        help='True/False',
        type=bool,
        default=True)
    parser.add_argument('--summary_output', default='summary.csv')
    parser.add_argument('--error_log', default='errors.log')
    args = parser.parse_args()
    root = args.source_dir
    out_dir = args.output_dir
    if out_dir is None:
        out_dir = root
    n_jobs = args.n_jobs
    path_format = args.path_format
    summarize = args.summarize
    summary_output = args.summary_output
    errorfile = args.error_log
    with Manager() as manager:
        analyze_queue = manager.Queue()
        mover_queue = manager.Queue()
        errors_queue = manager.Queue()
        summary_queue = manager.Queue()
        instances = manager.dict()
        series = manager.dict()
        counts = manager.list([0, 0])
        analyzers = []
        movers = []
        move_tasks = (n_jobs) // 2
        analyze_tasks = n_jobs - move_tasks
        analyze_status = manager.list()
        move_status = manager.list()
        for i in range(analyze_tasks):
            analyzers.append(
                Process(
                    target=analyzer,
                    args=(
                        analyze_queue,
                        mover_queue,
                        errors_queue,
                        summary_queue,
                        out_dir,
                        path_format,
                        summarize,
                        instances,
                        series,
                        counts,
                        analyze_status)))
        for i in range(move_tasks):
            movers.append(
                Process(
                    target=mover,
                    args=(
                        mover_queue,
                        errors_queue,
                        counts,
                        move_status)))
        errorlog = Process(target=errorlogger, args=(errors_queue, errorfile))
        progress = Process(target=progressbar, args=(counts,))
        if summarize:
            summary = Process(
                target=summarizer, args=(
                    summary_queue, summary_output))
            summary.start()
        for p in analyzers:
            p.start()
        for p in movers:
            p.start()
        errorlog.start()
        progress.start()
        for r, c, f in os.walk(root):
            for file in f:
                analyze_queue.put(os.path.join(r, file))
        for i in range(analyze_tasks):
            analyze_queue.put(None)
        while sum(analyze_status) < analyze_tasks:
            time.sleep(TIMEOUT)
        for i in range(move_tasks):
            mover_queue.put(None)
        summary_queue.put(None)
        counts.append('ANALYSIS_COMPLETE')
        while sum(move_status) < move_tasks:
            time.sleep(TIMEOUT)
        counts.append('MOVE_COMPLETE')
        errors_queue.put(None)
        for p in analyzers + movers + [errorlog, progress]:
            p.join()
        if summarize:
            summary.join()
