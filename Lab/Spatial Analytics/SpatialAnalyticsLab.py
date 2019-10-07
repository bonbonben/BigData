#!/usr/bin/env python
# coding: utf-8

# In[1]:


# download Shapely-1.6.4.post2-cp37-cp37m-win_amd64.whl
get_ipython().system('pip install pandas')
get_ipython().system('pip install Shapely-1.6.4.post2-cp37-cp37m-win_amd64.whl')

import pandas as pd
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
import math
import time

def loadPolygons(polyFile):
    print("reading file: " + polyFile)
    fp = open(polyFile)
    nregions = int(fp.readline())

    polySet = {}
    for i in range(0,nregions):
        name = fp.readline().strip('\n\r')
        # A single region can be made up of multiple polygons
        npolys = int(fp.readline())
        polys = []
        for j in range(0,npolys):
            npts = int(fp.readline())
            pts = []
            for k in range(0,npts):
                coords = fp.readline().split(",")
                # coords are lat, lon. switch to lon lan to be consistent with points data.
                lonlat = (float(coords[1]), float(coords[0]))
                pts.append(lonlat)
            poly = Polygon(pts)
            polys.append(poly)
        polySet[name] = MultiPolygon(polys)
    fp.close()
    return polySet

def bruteForceSearch(pts, queryPoly):
    result = []

    for i in range(len(pts)):
        pt = Point(pts['lon'][i], pts['lat'][i])
        if queryPoly.contains(pt) == True:
            result.append(i)
    return result

def getIndex(minval, maxval, res, val):
    ind = int(math.floor((val - minval) * res / (maxval - minval)))
    if ind < 0:
        ind = 0
    if ind >= res:
        ind = res - 1
    return ind

def createPointsGrid(pts, xsize, ysize, bounds):
    print("creating grid")
    latmin = bounds[0]
    lonmin = bounds[1]
    latmax = bounds[2]
    lonmax = bounds[3]

    grid = [[] for _ in range(xsize * ysize)]

    for i in range(len(pts)):
        lonin = getIndex(lonmin,lonmax,xsize,pts['lon'][i])
        latin = getIndex(latmin,latmax,ysize,pts['lat'][i])
        
        # convert to index in the grid array
        index = latin * xsize + lonin
        grid[index].append(i)

    return grid

def gridSearch(pts, queryPoly, grid, xsize, ysize, bounds):
    print("querying using grid")
    latmin = bounds[0]
    lonmin = bounds[1]
    latmax = bounds[2]
    lonmax = bounds[3]
    xmin = getIndex(lonmin, lonmax, xsize,queryPoly.bounds[0])
    xmax = getIndex(lonmin, lonmax, xsize,queryPoly.bounds[2])
    ymin = getIndex(latmin, latmax, ysize,queryPoly.bounds[1])
    ymax = getIndex(latmin, latmax, ysize,queryPoly.bounds[3])

    result = []
    for x in range(xmin,xmax):
        for y in range(ymin,ymax):
            index = y * xsize + x
            for i in grid[index]:
                pt = Point(pts['lon'][i], pts['lat'][i])
                if queryPoly.contains(pt) == True:
                    result.append(i)
    return result


# In[2]:


if __name__ == "__main__":
    # testing with 100k points
    pts = pd.read_csv("taxi_lab_100k.csv")
    nbdata = loadPolygons("neighborhoods.poly")
    
    # Query polygon is lower manhattan -- Battery Park City-Lower Manhattan
    queryPoly = nbdata['Battery Park City-Lower Manhattan']

    # brute force
    start_time = time.time()
    result = bruteForceSearch(pts,queryPoly)
    print("Duration (bruteForceSearch): %.4f seconds" %(time.time() - start_time))
    print("Result size (bruteForceSearch): %d" %len(result))

    # bounds for NYC
    bounds = (40.494835,-74.277302,40.952497,-73.674025)

    # create grid index 
    start_time = time.time()
    grid = createPointsGrid(pts,1024,1024,bounds)
    print("Duration (createPointsGrid): %.4f seconds" %(time.time() - start_time))

    # query grid index
    start_time = time.time()
    result = gridSearch(pts,queryPoly,grid,1024,1024,bounds)
    print("Duration (gridSearch): %.4f seconds" %(time.time() - start_time))
    print("Result size (gridSearch): %d" %len(result))

