# This code converts the xml format segmentations output by the "ExportROIs" plugin of Horos. The function returns an ITKImage mask, 
# given the reference dicom volume and the xml segmentation. If a save path is provided, it can also save the output. 
#
# Usage:
# mask = get_mask_from_xml(xml_file, ref_vol_path, save_path=None)
#
# Author: Pritam Mukherjee
#
# The created roi is not exact and may differ slightly from the one Horos produces. This is due to the spline interpolation that Horos 
# uses. The details of that is not replicated here. Rather, I use scipy's spline interpolation here.

import numpy as np
import SimpleITK as sitk
from plistlib import load
from scipy.interpolate import splev, splrep
from skimage.draw import polygon, polygon2mask, polygon_perimeter

tol = 1e-3
SCALE_FOR_INTERP = 10

def get_length(rr, cc):
    if len(rr) != len(cc):
        raise ValueError("Must be of the same size")
    d = 0
    rr += rr[-1:]
    cc += cc[-1:]
    for i in range(len(rr)):
        if i == 0:
            continue
        d += ((rr[i] - rr[i-1])*(rr[i] - rr[i-1])  + (cc[i] - cc[i-1])*(cc[i] - cc[i-1]))**(0.5)
    return d

def get_spline(x, length):
    scale = SCALE_FOR_INTERP
    n = length*scale
    i = range(len(x)+2)
    y = x + x[0:2]
    spl, err, succ, m = splrep(i, y, per='periodic', full_output=1)
    if succ >= 0:
        print(m)
    new_i = np.linspace(0, len(x) + 2, int(n))
    new_y = splev(new_i, spl)
    return new_y[0:-2]   
    


def get_mask_from_xml(xml_file, ref_vol_path, save_path=None):
    ref_vol_path = str(ref_vol_path)
    series_reader = sitk.ImageSeriesReader()
    dicom_names = series_reader.GetGDCMSeriesFileNames(str(ref_vol_path))
    if len(dicom_names) == 0:
        raise FileNotFoundError("No dicoms found")
    series_reader.SetFileNames(dicom_names)
    vol = series_reader.Execute()
    np_vol = sitk.GetArrayFromImage(vol)
    np_mask = np.zeros_like(np_vol).astype(np.bool)
    with open(xml_file, 'rb') as fp:
        pl = load(fp)
    for image in pl['Images']:
        z_index = image['ImageIndex']
        num_rois = image['NumberOfROIs']
        counter = 0
        for roi in image['ROIs']:
            num_points = roi['NumberOfPoints']
            roi_length = roi['Length']
            points_mm = [eval(x) for x in roi['Point_mm']]
            points_px = [eval(x) for x in roi['Point_px']]
            assert len(points_mm) == len(points_px) == num_points, "Number of points in mm, px, and in xml do not match"
            assert all([abs((vol.TransformPhysicalPointToContinuousIndex(p)[2] - z_index)) < tol for p in points_mm]), \
            "Geometry mismatch: All the points of the ROI do not seem to lie on the specified slice"
#            points_px = [vol.TransformPhysicalPointToContinuousIndex(p)[0:2] for p in points_mm]
            px_x = [p[1] for p in points_px]
            px_y = [p[0] for p in points_px]
            px_length = get_length(px_x, px_y)
            spl_x, spl_y = get_spline(px_x, px_length), get_spline(px_y, px_length)
            rr, cc = polygon(spl_x, spl_y)
            np_mask[z_index][rr, cc] = True
            counter +=1
        assert counter == num_rois, "Number of ROIs do not match"
    mask = sitk.GetImageFromArray(np_mask)
    mask.SetOrigin(vol.GetDepthOrigin())
    mask.SetDirection(vol.GetDirection())
    mask.SetSpacing(vol.GetSpacing())
    if save_path is not None:
        save_path = str(save_path)
        writer = sitk.ImageFileWriter()
        writer.SetFileName(save_path)
        writer.Execute(mask)        
    return mask
