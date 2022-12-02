# import geocoder

# g = geocoder.geonames('46410', key='yuegong', maxRows=5)
# print(g.json)

import censusgeocode as cg

location = cg.coordinates(x=-87.74, y=41.78)
print(location)
# g = geocoder.geonames('New York', key='yuegong')
# c = geocoder.geonames(g.raw['geonameId'], key='yuegong', method='children')
# print(c.geojson)