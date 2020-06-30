import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
import os
import requests
from retrying import retry

class GetDatePageUrl(beam.DoFn):
    def __init__(self, producturl):
        self.producturl = producturl
    def process(self, date):
        return [(date,self.producturl + "/" + date)]


class GetHdfUrlsFromDateUrl(beam.PTransform):

    def load_page_text_to_lines(self, date, url):
        import requests
        resp = requests.get(url)
        lines = resp.text.split('\n')
        return [(date, l, url) for l in lines]
        # return beam.Create(lines)

    def parse_hdf_from_line(self, date, textline, baseurl):
        if textline.find('.hdf"') != -1:
            return (date, baseurl + '/' + textline.split('<a href="')[1].split('">')[0])

    def expand(self, pcoll): #
        return (pcoll
                | "Load_page_lines" >> beam.MapTuple(lambda date, url: self.load_page_text_to_lines(date, url))
                | "Flatten" >> beam.Flatten()
                | "discover_hdf_urls" >> beam.MapTuple(lambda date, line, url: self.parse_hdf_from_line(date, line, url))
                | "remove_non_matching" >> beam.Filter(lambda l: l is not None)
                )


class FilterExistingFiles(beam.PTransform):
    def __init__(self, hdf_bucket_path, req_tile_list="*"):
        gcs = GcsIO()
        self._existing = [os.path.basename(l) for l in list(gcs.list_prefix(hdf_bucket_path).keys())]
        self._required_tiles = req_tile_list

    def checktile(self, url):
        import os
        thistile = os.path.basename(url).split('.')[2]
        return self._required_tiles == "*" or thistile in self._required_tiles

    def expand(self, pcoll):
        import os
        stripped_existing = (pcoll | "remove_existing" >> beam.Filter(lambda l: os.path.basename(l[1])
                                                                                not in self._existing))
        if self._required_tiles == "*":
            return stripped_existing
        else:
            return stripped_existing | "remove_unrequired" >> beam.Filter(lambda l: self.checktile(l[1]))


class DownloadHdfToBucket(beam.PTransform):
    #https://pypi.org/project/retrying/
    #https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
    def __init__(self, user, pw, hdf_bucket_path):
        #import requests
        self._session = requests.Session()
        self._session.auth = (user, pw)
        self._hdf_bucket_path = hdf_bucket_path
        self._chunk_size = 8 * 1024 * 1024

    @retry(stop_max_attempt_number=7, wait_fixed=3000)
    def download_file(self, url):
        #import requests, tempfile, os
        #from apache_beam.io.gcp.gcsio import GcsIO
        req = self._session.request('get', url)
        resp = self._session.get(req.url, stream=True)
        product, datestr, fname = url.split('/')[-3:]
        bucketfilename = '/'.join([self._hdf_bucket_path, product, datestr, fname])
        gcs = GcsIO()
        with gcs.open(bucketfilename, 'w') as fp:
            # with open(tempfilename, 'wb') as fp:
            for chunk in resp.iter_content(chunk_size=self._chunk_size):
                if chunk:
                    fp.write(chunk)
            fp.flush()
            # os.fsync(fp)
        return bucketfilename

    def expand(self, pcoll):
        return (pcoll | beam.Map(self.download_file))


