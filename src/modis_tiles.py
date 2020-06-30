import numpy as np

#!wget --no-check-certificate https://modis-land.gsfc.nasa.gov/pdf/sn_bound_10deg.txt
#https://www.earthdatascience.org/tutorials/convert-modis-tile-to-lat-lon/
data = np.genfromtxt('sn_bound_10deg_cleaned.txt',
                     skip_header = 7,
                     skip_footer = , encoding=None)

class TileInfo:
    def __init__(self, line):
        try:
            self.V, self.H, self.WEST_LON, self.EAST_LON, self.SOUTH_LAT, self.NORTH_LAT=line.split()
            self.WEST_LON = float(self.WEST_LON)
            self.EAST_LON = float(self.EAST_LON)
            self.SOUTH_LAT = float(self.SOUTH_LAT)
            self.NORTH_LAT = float(self.NORTH_LAT)

        except:
            print("WARNING! Error parsing tiles index file on line "+line)

    def contains(self, lat, lon):
        return (lat > self.SOUTH_LAT) and (lat < self.NORTH_LAT) and (lon > self.WEST_LON) and (lon < self.EAST_LON)

    def intersects(self, slat, wlon, nlat, elon):
        return (nlat > self.SOUTH_LAT) and (slat < self.NORTH_LAT) and (elon > self.WEST_LON) and (wlon < self.EAST_LON)

    def tile_id(self):
        return "h{}v{}".format(self.H.zfill(2), self.V.zfill(2))

def filterByLocation(tile, lat, lon):
    if tile.contains(lat, lon):
        yield tile

def filterByArea(tile, slat, wlon, nlat, elon):
    if tile.intersects(slat, wlon, nlat, elon):
        yield tile

def filterByIsRequiredTile(tile, requiredTiles):
    if requiredTiles == '*' or tile.tile_id in requiredTiles:
        yield tile

def loadTiles():
    with open ('../resources/sn_bound_10deg_cleaned.txt') as master_tiles_listing:
        next(master_tiles_listing)
        return [TileInfo(l) for l in master_tiles_listing.readlines()]