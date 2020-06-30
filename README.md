# df_test_cookiecutter

Download MODIS tiles, mosaic, calculate output data, reproject to WGS84 GeoTiffs

A so-far-incomplete development of code to acquire global geotiffs of MODIS variables using Google Cloud Dataflow.

## About the process

In MAP we need global 8-daily Geotiffs with 30 arcsecond resolution and WGS84 projection for 
each of 5 MODIS-derived variables: Day and Night Land Surface Temperature, EVI, TCB, and TCW.
These are derived from two MODIS products: MOD11A2 and MCD43D*
 
To create these there are several necessary steps:
- Identify the days and tiles for which data are needed. There are up to 317 tiles per global mosaic and these are available (for the products we use) at 8-daily intervals.
- Identify the URLs to download the necessary tiles which are HDF files
- Download the files from the NASA data pool (authentication is needed)
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

## Developing the process

First the various steps have been collated into a single place to work out what we need to do (see notebooks/initial-coalesce-workflow). 

Next these steps have been translated into Apache Beam code (PTransforms or DoFns and a pipeline). 
This was done using google dataflow notebooks with the Beam Interactive Runner (see notebooks/dev-pipeline-on-dataflow-notebooks). 

Finally we will transfer this into a standard dataflow submission script (TBD).

## Installation

It is highly-recommended to run the Dataflow pipeline within a virtual
environment. Create a virtual env and install the dependencies inside it:

```sh
python3 -m virtualenv venv
venv/bin/pip install -r requirements.txt
```
or
```sh
python3 -m venv venv
source venv/bin/activate
pip3 install wheel
pip3 install apache-beam[gcp]
```

## Usage

### Setting-up the workers

You can customize how workers were set-up by updating the `setup.py` file. You
can add more commands by adding new lines in the `CUSTOM_COMMANDS` list. For
example:

```python
CUSTOM_COMMANDS = [
    ["apt-get", "update"],
    ["apt-get", "--assume-yes", "install", "libjpeg62"],
]
```

You can also add more dependencies for each of your workers by filling-in the
`requirements-worker.txt` file. **Lastly, you can edit the pipeline by updating
`main.py`.** 

### Running pipelines

In order to run the Dataflow pipeline, execute the following command: 

```sh
venv/bin/python3 main.py
```

You can pass multiple parameters such as the number of workers, machine type,
and more. For example, let's run 10  `n1-standard-1` workers:

```sh
venv/bin/python3 main.py --num-workers 10 --machine-type "n1-standard-1"
```

As best practice, we recommend running your pipeline locally. You can do this
by passing the `--local` flag.

```sh
venv/bin/python3 main.py --local
```

| Parameter              	| Type 	| Description                                        	|
|------------------------	|------	|----------------------------------------------------	|
| `-n`, `--num-workers`  	| int  	| Number of workers to run the Dataflow job.         	|
| `-m`, `--machine-type` 	| str  	| Machine type to run the jobs on.                   	|
| `-s`, `--disk-size`    	| int  	| Disk size (in GB) for each worker when job is run. 	|
| `--project`            	| str  	| Google Cloud Platform (GCP) project to run the Dataflow job                	|
| `--region`             	| str  	| Google Cloud Platform (GCP) region to run the Dataflow job                 	|
| `--artifact-bucket`    	| str  	| Google Cloud Storage (GCS) bucket to store temp and staging files         	|


---

This project was generated using the `standard` template from
[ljvmiranda921/dataflow-cookiecutter](https://github.com/ljvmiranda921/dataflow-cookiecutter).
Have other templates in mind? Feel free to make a Pull Request!
