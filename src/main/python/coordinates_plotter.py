import pandas as pd
import geopandas as gpd
import numpy as np
import contextily as cx
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt

# (https://towardsdatascience.com/geopandas-101-plot-any-data-with-a-latitude-and-longitude-on-a-map-98e01944b972)
# https://towardsdatascience.com/plotting-maps-with-geopandas-428c97295a73
# https://geodata.lib.utexas.edu/?f%5Bdc_format_s%5D%5B%5D=Shapefile&f%5Bdct_spatial_sm%5D%5B%5D=San+Francisco+Bay+Area+%28Calif.%29&f%5Blayer_geom_type_s%5D%5B%5D=Line&per_page=20&sort=dc_title_sort+asc
street_map = gpd.read_file('./data/ba_streets.shp', bbox=(37.805814, -122.534699, 37.580173, -122.336083))  # Takes a while!


# ,

# Data form a trip starting in the North, travelling to the airport
# and then travelling back up to the North in one 'E-M-...-M-E' trip
df = pd.read_csv('./down-up_airport_trip.csv', names=["Latitude", "Longitude"], comment='#')
df['Descending'] = [True] + [True if df['Latitude'][i] < df['Latitude'][i - 1] else False
                             for i in np.arange(1, len(df['Latitude']))]
df.loc[1, 'Descending'] = True

geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
crs = {'init': 'EPSG:4326'}
geo_df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

# Plotting
fig, ax = plt.subplots(figsize=(10, 10))
# Plot the map and the data onto the same axes
street_map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey', zorder=0)  # alpha=0.4  # Note the .plot!
# Plot trip from North to South in Blue
geo_df[geo_df['Descending'] == True].plot(ax=ax, markersize=20, color="blue", marker='o', label="Downwards trip", zorder=10)
# Plot trip from South to North in Red
geo_df[geo_df['Descending'] == False].plot(ax=ax, markersize=20, color="red", marker='o', label="Upwards trip", zorder=10)
cx.add_basemap(ax, crs=geo_df.crs)
leg = ax.legend()
plt.show()
