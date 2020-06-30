import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
import os
import requests
from retrying import retry

# for a given day and set of tiles, copy all matching HDFs from a bucket path
# (which comprises a product i.e. the folder that contains the date folder)
# to a local dir, and make a vrt from them which selects
# the right layer from each tile into a singleband mosaic
class CreateVrtsForDays(beam.PTransform):
    def __init__(self, bucketpath, layertemplate, varname, tiles="*"):
        #import os
        #from apache_beam.io.gcp.gcsio import GcsIO
        self._bucketproductpath = bucketpath
        gcs = GcsIO()
        self._existing = [l for l in list(gcs.list_prefix(hdf_bucket_path).keys())]
        self._layertemplate = layertemplate
        self._tiles = tiles
        self._varname = varname

    def get_tilenames_for_day(self, day):
        return [f for f in self._existing if f.split('/')[-2] == day]

    def filter_to_required_tiles(self, daytiles):
        if self._tiles == '*':
            return daytiles
        return [f for f in daytiles if split(os.path.basename(f), '.') in self._tiles]

    def get_tmp_folder_for_day(self, day):
        tmpfolder = tempfile.gettempdir()
        workfolder = os.path.join(tmpfolder, day)
        return workfolder

    def localise_day_files(self, day):
        files = self.filter_to_required_files(self.get_tilenames_for_day(day))
        tempfolder = self.get_tmp_folder_for_day(day)
        localpaths = []
        gcs = GcsIO()
        if not os.path.isdir(tempfolder):
            os.makedirs(tempfolder)
        for f in files:
            localname = os.path.join(tempfolder, os.path.basename(f))
            if not os.path.exists(localname):
                # it might have already been copied if say day and night files, which come
                # from the same HDFs, are being made on the same worker
                with gcs.open(f) as gcsfile, open(localname, 'wb') as localfile:
                    localfile.write(gcsfile.read())
            localpaths.append(localname)
        return (day, localpaths)

    def build_vrt_file(self, day, paths):
        #daytemplate = 'HDF4_EOS:EOS_GRID:"{}":MODIS_Grid_8Day_1km_LST:LST_Day_1km'
        #nighttemplate = 'HDF4_EOS:EOS_GRID:"{}":MODIS_Grid_8Day_1km_LST:LST_Night_1km'
        qualpaths = [self_layertemplate.format(f) for f in paths]
        thisfolder = os.path.dirname(paths[0])
        # day = os.path.basename(thisfolder)
        vrtfile = os.path.join(thisfolder, "{}.{}.vrt".format(self._varname, day))
        vrt = gdal.BuildVRT(vrtfile, qualpaths)
        vrt.FlushCache()
        vrt = None
        return vrtfile

    def expand(self, pcoll):
        return (pcoll | "copy_files_to_local_tmp" >> beam.Map(self.localise_day_files)
                | "build_vrts_on_tmp" >> beam.Map(lambda d, p: self.build_lst_vrt_files(d, p))
                )
        # groups by day


class TranslateVrtToLstTiff(beam.PTransform):

    def __init(self, calculation):
        self._calculation = calculation
        #lst_calc = "band_data * 0.02 + (-273.15)"

    def get_out_name(self, vrtname):
        return vrtname.replace('.vrt', '.sinusoidal.tif')

    def run_singleband_calculation(input_singleband_file, out_file, calc, out_type='Float32'):
        '''Hacked together from gdal_calc.py. Uses numexpr for slightly more efficient calculation (multithreaded).

        Calc must be the calculation to apply to the data from input_singleband_file, specified as a string which will be
        eval'd against the data which will exist in a variable called band_data. i.e. to specify doubling the data then subtracting
        three, provide calc="(band_data * 2.0) - 3.0"'''
        # input_datasets = []
        # myBands = []
        # myDataType = []
        # myDataTypeNum = []
        # myNDV = []
        import numpy as np, numexpr as ne
        DimensionsCheck = None

        ds = gdal.Open(input_singleband_file, gdal.GA_ReadOnly)
        if not ds:
            raise IOError("Error opening input file {}".format(input_file))
        input_dataset = ds
        inputDataType = (gdal.GetDataTypeName(ds.GetRasterBand(1).DataType))
        inputDataTypeNum = (ds.GetRasterBand(1).DataType)
        inputNDV = (ds.GetRasterBand(1).GetNoDataValue())

        DimensionsCheck = [ds.RasterXSize, ds.RasterYSize]

        if os.path.isfile(out_file):
            os.remove(out_file)
        # gdal_calc does this but it isn't valid as two int datasets can result in a float!
        # outType = gdal.GetDataTypeName(max(myDataTypeNum))

        # create the output file
        outDriver = gdal.GetDriverByName("GTiff")
        cOpts = ["TILED=YES", "SPARSE_OK=TRUE", "BLOCKXSIZE=1024", "BLOCKYSIZE=1024", "BIGTIFF=YES", "COMPRESS=LZW",
                 "NUM_THREADS=ALL_CPUS"]
        outDS = outDriver.Create(out_file, DimensionsCheck[0], DimensionsCheck[1], 1, gdal.GetDataTypeByName(out_type),
                                 cOpts)
        outDS.SetGeoTransform(input_dataset.GetGeoTransform())
        outDS.SetProjection(input_dataset.GetProjection())
        DefaultNDVLookup = {'Byte': 255, 'UInt16': 65535, 'Int16': -32767, 'UInt32': 4294967293, 'Int32': -2147483647,
                            'Float32': 3.402823466E+38, 'Float64': 1.7976931348623158E+308}
        outBand = outDS.GetRasterBand(1)
        outNDV = DefaultNDVLookup[out_type]
        outBand.SetNoDataValue(outNDV)
        outBand = None

        # vrt file reports a block size of 128*128 but the underlying hdf block size is 1200*100
        # so hard code this or some clean multiple : this minimises disk access
        myBlockSize = [4800, 4800]
        nXValid = myBlockSize[0]
        nYValid = myBlockSize[1]
        nXBlocks = (int)((DimensionsCheck[0] + myBlockSize[0] - 1) / myBlockSize[0]);
        nYBlocks = (int)((DimensionsCheck[1] + myBlockSize[1] - 1) / myBlockSize[1]);

        for x in range(0, nXBlocks):
            if x == nXBlocks - 1:
                nXValid = DimensionsCheck[0] - x * myBlockSize[0]

            myX = x * myBlockSize[0]

            nYValid = myBlockSize[1]
            myBufSize = nXValid * nYValid

            for y in range(0, nYBlocks):
                if y == nYBlocks - 1:
                    nYValid = DimensionsCheck[1] - y * myBlockSize[1]
                    myBufSize = nXValid * nYValid

                myY = y * myBlockSize[1]
                band_data = input_dataset.GetRasterBand(1).ReadAsArray(xoff=myX, yoff=myY,
                                                                       win_xsize=nXValid, win_ysize=nYValid)
                nodata_locs = band_data == inputNDV

                try:
                    result = ne.evaluate(calc)
                except:
                    raise

                # apply ndv (set nodata cells to zero then add nodata value to these cells)
                result = ((1 * (nodata_locs == 0)) * result + (outNDV * nodata_locs))

                outBand = outDS.GetRasterBand(1)
                outBand.WriteArray(result, xoff=myX, yoff=myY)
        return out_file

    def expand(self, pColl):
        return pColl | beam.Map(lambda v: run_singleband_calculation(v, self.get_out_name(v), self._calculation))


class CreateProjectedOutput(beam.PTransform):

    def __init__(self, ForceGlobalExtent=False):
        self._forceglobal = ForceGlobalExtent

    def warpfile(self, sinusFile):
        cOpts = ["TILED=YES", "BIGTIFF=YES", "COMPRESS=LZW", "NUM_THREADS=ALL_CPUS"]
        if self._forceglobal:
            wo = gdal.WarpOptions(format='GTiff',
                                  outputBounds=[-180, -90, 180, 90],
                                  xRes=1 / 120.0, yRes=-1 / 120.0, dstSRS='EPSG:4326',
                                  creationOptions=cOpts, multithread=True, dstNodata=-9999, warpMemoryLimit=4096)
        else:
            wo = gdal.WarpOptions(format='GTiff',
                                  xRes=1 / 120.0, yRes=-1 / 120.0, dstSRS='EPSG:4326',
                                  targetAlignedPixels="YES",
                                  creationOptions=cOpts, multithread=True, dstNodata=-9999, warpMemoryLimit=4096)

        outname = sinusFile.replace('.sinusoidal', '')
        gdal.Warp(outname, sinusFile, options=wo)
        return outname

    def expand(self, pColl):
        return pColl | beam.Map(self.warpfile)