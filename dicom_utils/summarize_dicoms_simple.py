"""
Author: Pritam Mukherjee
Date: 04/15/2020
Python version: 3.6
Purpose: To sort through dicoms and summarize them by series, all in a single pass! 
"""

import os
import sys
import time
import pydicom
import argparse
import numpy as np
import pandas as pd


TAGS_OF_INTEREST = {
    'SeriesInstanceUID': str,
    'StudyInstanceUID': str,
    'PatientName': str,
    'PatientID': str,
    'Modality': str,
    'PatientSex': str,
    'SliceThickness': float,
    'PixelSpacing': str,
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



def summarizer(ds):
    for k in TAGS_OF_INTEREST:
        try:
            val = TAGS_OF_INTEREST[k](
                ds.data_element(k).value)
        except (ValueError, KeyError):
            val = np.nan
        summary_dic[k].append(val)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source_dir',
        help='The source directory containing all the dicoms')
    parser.add_argument(
        '--full',
        help='True/False',
        action='store_true',
        default=False)
    parser.add_argument('--summary_output', default='summary.csv')
    parser.add_argument('--error_log', default='errors.log')
    args = parser.parse_args()
    root = args.source_dir
    full = args.full
    summary_output = args.summary_output
    errorfile = args.error_log
    counter = 1
    summary_dic = {k:[] for k in TAGS_OF_INTEREST}
    summary_dic['Location'] = []
    errors = []
    for r, c, f in os.walk(root):
        for file in f:
            try:
                ds = pydicom.dcmread(os.path.join(r, file))
                summarizer(ds)
                summary_dic['Location'].append(r)
                counter +=1
                if counter % 100 == 0:
                    print("Analyzed {} dicoms".format(counter))
                if not full:
                    break
            except pydicom.errors.InvalidDicomError:
                errors.append('Invalid dicom {}'.format(os.path.join(r, file)))
    print("Analyzed {} dicoms".format(counter))
    df = pd.DataFrame(summary_dic)
    df = df.drop_duplicates()
    df.to_csv(summary_output, index=False)
    with open(errorfile, 'w') as fp:
        fp.writelines('\n'.join(errors))
