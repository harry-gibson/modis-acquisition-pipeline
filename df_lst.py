import argparse
from src.modis_date_utils import get_downloadable_dates
from src.utils import configure_pipeline
from src.modis_download_transforms import *
from src.modis_tiles import loadTiles, filterByIsRequiredTile

def run():
    parser = argparse.ArgumentParser(description='Produce 8-daily LST files')
    parser.add_argument('--start_year', type=int, default=2000,
                        help='default=2000 First year to download data for')
    parser.add_argument('--end_year', type=int, default=2020,
                        help='default=2020 Last year to download data for')
    parser.add_argument('--start_doy', type=int, default=1,
                        help='default=1 First day-of-year to download data for')
    parser.add_argument('--end_doy', type=int, default=366,
                        help='default=366 First year to download data for')
    parser.add_argument('--tiles', nargs='*', default='*',
                        help='default=''*'' Supply a comma-separated list of modis tiles to download or * for all tiles')
    parser.add_argument('--output_dir', required=True,
                        help='Where should the HDF tiles and output images be stored? Supply a GCS location when running on cloud')
    parser.add_argument('--dataset', choices=['LST_Day', 'LST_Night', 'EVI', 'TCB', 'TCW'], help="MAP product to create")
    parser.add_argument('--nasa_username', required=True,
                        help='NASA Earthdata login username')
    parser.add_argument('--nasa_password', required=True,
                        help='NASA Earthdata login password')
    parser.add_argument('--local', type=bool, default=True)
    known_args, pipeline_args = parser.parse_known_args()

    map_product = known_args.dataset
    BASE_URL = "http://e4ftl01.cr.usgs.gov"
    if map_product == "LST_Day":
        platform = "MOLT"
        product = "MOD11A2.006"
        layer_template = 'HDF4_EOS:EOS_GRID:"{}":MODIS_Grid_8Day_1km_LST:LST_Day_1km'
    elif map_product == "LST_Night":
        platform = "MOLT"
        product = "MOD11A2.006"
        layer_template = 'HDF4_EOS:EOS_GRID:"{}":MODIS_Grid_8Day_1km_LST:LST_Night_1km'
    else:
        raise NotImplementedError()
    product_url = f"{BASE_URL}/{platform}/{product}"

    dates_to_get = get_downloadable_dates(product_url,
                                known_args.start_year, known_args.end_year,
                                known_args.start_doy, known_args.end_doy)
    print(dates_to_get)
    print(known_args.tiles)

    hdf_bucket_path = "gs://hsg-dataflow-test/lst_download_dev/hdflocal"
    #HDF_BUCKET_PATH = BUCKET_PATH + '/hdf
    pipeline_options = configure_pipeline(
        project="map-oxford-hsg",
        artifact_bucket="hsg-dataflow-test",
        num_workers=10,
        region="europe-west4",
        machine_type="n1-standard-2",
        disk_size=100,
        local=known_args.local
    )
    p = beam.Pipeline(options=pipeline_options)
    hdfpaths = (p
                | "relevant_dates" >> beam.Create(dates_to_get)
                | "date_page_urls" >> beam.ParDo(GetDatePageUrl(product_url)) # (date,url) tuples
                | "hdf_urls" >> GetHdfUrlsFromDateUrl() # (date, hdfurl) tuples
                | "not_yet_downloaded" >> FilterExistingFiles(hdf_bucket_path, req_tile_list=known_args.tiles) # (date, hdfurl) tuples
                | "download_files_to_bucket" >> DownloadHdfToBucket(known_args.nasa_username, known_args.nasa_password, hdf_bucket_path)
                )
    p.run()
    print(hdfpaths)

    vrts = p | beam.Create(dates_to_get) | CreateVrtsForDays(hdf_bucket_path)
    uploaded_tiffs = vrts | TranslateVrtToLstTiff() | CreateProjectedOutput() | beam.ParDo(UploadAndClean())
if __name__ == "__main__":
    run()
#python3 df_lst.py --output_dir /tmp --nasa_username harrygibson --nasa_password 0Striches --dataset LST_Day --start_year 2019 --end_year 2020 --start_doy 20 --end_doy 40 --tiles h17v03 h17v04 h18v03 h18v04