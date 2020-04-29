
import SimpleITK as sitk

class Converter:
    """
    This is a generic converter built upon SimpleITK and supports many commonly 
    used formats such as dicom and niftis. 
    Usage:
    Inititalize as:
    converter = Converter(input_type)
    The input type can be the filename extension of the image
    Then convert as:
    converter.convert(input_path, output_path)   
    The code does no error handling by design, and you may have to handle errors
    in the user code.
    """
    def __init__(self, input_type='dicom'):
        self.input_type = input_type
        if input_type in ['dicom', 'dcm']:
            self.input_type = 'dicom'
            self.reader = sitk.ImageSeriesReader()
        else:
            self.reader = sitk.ImageFileReader()
        self.writer = sitk.ImageFileWriter()
    
    def convert(self, input_path, output_filename):
        self.writer.SetFileName(output_filename)
        if self.input_type == 'dicom':
            dicom_files = self.reader.GetGDCMSeriesFileNames(input_path)
            self.reader.SetFileNames(dicom_files)
        else:
            self.reader.SetFileName(input_path)
        img = self.reader.Execute()
        self.writer.Execute(img)      
            
        
