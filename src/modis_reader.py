import os, subprocess

class ModisReader():

    # Look for an HDF file in the gsdir
    # If it does not exist then download it from the MODIS site and copy it to the gsdir, also to the localdir
    # If it does then copy it to the localdir
    # Return a string representing the string to get the required layer out of the HDF as a GDAL dataset

   def __init__(self, gsdir, localdir, destdir, product, layertemplate):
      basename = os.path.basename(gsdir)
      self.gsfile = '{0}/{1}_{2}.TIF'.format(gsdir, basename, band)
      self.dest = os.path.join(destdir, os.path.basename(self.gsfile))



   def __enter__(self):
      print ('Getting {0} to {1} '.format(self.gsfile, self.dest))
      ret = subprocess.check_call(['gsutil', 'cp', self.gsfile, self.dest])
      if ret == 0:
         dataset = gdal.Open( self.dest, gdal.GA_ReadOnly )
         return dataset
      else:
         return None

   def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
      os.remove( self.dest ) # cleanup

def ProduceModisVrtMosaic(gs_baseurl, product, datestr, tiles, layertemplate):
    # get url from product
    # create a modisreader for each tile for this date, which will get the tiles to local tempdir
    # build gdal vrt on all the layers returned from the modisreaders
    pass

def ProduceLstDayTiff(gs_baseurl, datestr, region):
    pass

def ProduceLstNightTiff(gs_baseurl, datestr, region):
    pass

def ProduceEviTiff(gs_baseurl, datestr, region):
    pass
