import os
import requests
import time


def generate_selected_dates(year_from=2000, year_to=2020, doy_start=1, doy_end=-1):
    """Returns dates within the given years for the given range of julian days
     (https://landweb.modaps.eosdis.nasa.gov/browse/calendar.html) formatted as
     strings like YYYY.MM.DD which form the directories on the MODIS datapool site"""
    import calendar, time
    dates = []
    for year in range(year_from, year_to+1):
        if doy_end == -1:
            if calendar.isleap(year):
                end_day = 367
            else:
                end_day = 366
        else:
            end_day = doy_end
        dates_this_yr = [time.strftime("%Y.%m.%d", time.strptime("%d/%d" % (i, year),
                                                         "%j/%Y")) for i in
                 range(doy_start, end_day)]
        dates.extend(dates_this_yr)
    return dates


def get_existing_files(out_dir):
    # in case we need to do something different to list files on bucket
    return os.listdir(out_dir)


def load_page_text(url):
    """Loads a page and returns its raw text, unless it is wednesday afternoon
     (nasa data pools are unavailable for maintenance on wednesday afternoons)"""
    the_day_today = time.asctime().split()[0]
    the_hour_now = int(time.asctime().split()[3].split(":")[0])
    if the_day_today == "Wed" and 14 <= the_hour_now <= 17:
        #LOG.info("Sleeping for %d hours... Yawn!" % (18 - the_hour_now))
        time.sleep(60 * 60 * (18 - the_hour_now))
    resp = requests.get(url)
    return resp.text


def parse_modis_dates(product_url, requested_dates):
    """Parse returned MODIS dates.

    This function gets the available dates for a given MODIS product, and
    crosses these with the required dates that the user has selected and returns the
    intersection to give e.g. the dates of the 8-daily products within the selected timespan

    Parameters
    ----------
    product_url: str
        A top level product URL such as "http://e4ftl01.cr.usgs.gov/MOTA/MCD45A1.005/"
    requested_dates: list
        A list of required dates in the format "YYYY.MM.DD"
    Returns
    -------
    A (sorted) list with the dates that can be downloaded.
    """
    import time
    html = load_page_text(product_url).split('\n')

    available_dates = []
    for line in html:
        if line.find("href") >= 0 and \
                line.find("[DIR]") >= 0:
            # Points to a directory
            the_date = line.split('href="')[1].split('"')[0].strip("/")
            available_dates.append(the_date)

    dates = set(requested_dates)
    available_dates = set(available_dates)
    suitable_dates = list(dates.intersection(available_dates))
    suitable_dates.sort()
    return suitable_dates


def get_downloadable_dates(product_url, year_from=2000, year_to=2020, doy_start=1, doy_end=-1):
    return parse_modis_dates(product_url, generate_selected_dates(year_from,year_to,doy_start,doy_end))
