Repository contains a so-far-incomplete development of code to acquire global geotiffs of MODIS variables using Google Cloud Dataflow.

There are several necessary steps:
- Identify the days and tiles for which data are needed. There are up to 317 tiles per global mosaic and these are available (for the products we use) at 8-daily intervals.
- Identify the URLs to download the necessary tiles which are HDF files
- Create a virtual mosaic of the relevant layers from each HDF file (each may contain several data layers)
- Read one or more of those mosaics as raster bands and calculate the output data (e.g. to convert scaled integer values into float temperatures in celsius, or calculate EVI values from reflectance band data
- Reproject those output data into WGS84 geotiffs in the standard format we use

Historically I have done this in several stages: 
- use a tool such as pyModis or getModis to download all the tiles, optionally running this in parallel (using xargs -P, or the windows equivalent ppx2) to download in several streams
- for each output day, use a batch script to:
  - call gdalbuildvrt to generate the "input" global data bands
  - call a python script to calculate the necessary output values (like gdal_calc.py), saving to 
  a tiff file in the original (sinusoidal) projection
  - call gdalwarp to reproject this tiff into the final WGS84 30-arcsecond compressed tiffs
- run several instances of the batch script in parallel again using xargs / ppx2
Input, temporary, and output locations in the batch script were chosen to maximise throughput on a well-specced desktop workstation, in particular using a RAM disk for the temporary unprojected tiffs. This process all worked ok but was still fairly manual. The code is in a separate very out-of-date repo at https://github.com/harry-gibson/modis-acquisition. Other colleagues have developed similar processes using slightly different tools.

The present repo contains an attempt to reproduce this pipeline using Google Dataflow (Apache Beam) with the hope that it can be run with less oversight and hassle, and not relying on local disks, network, and computation, all of which are used heavily. 

First we collate the various steps into a single place to work out what we need to do (see initial-coalesce-workflow). Next we translate all these steps into Apache Beam code (PTransforms or DoFns and a pipeline). This is done using google dataflow notebooks with the Beam Interactive Runner (see dev-pipeline-on-dataflow-notebooks). Finally we will transfer this into a standard dataflow submission script (TBD).