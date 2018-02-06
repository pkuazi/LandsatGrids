#
# -*-coding:utf-8 -*-
#
# @Author: zhaojianghua
# @Date  : 2018-02-06 14:02
#

"""
true_color_composition for Landsat45 images for presentation
for Landsat45, band1-blue, band2-green, band3-red, band4-nir
"""
import rasterio
import os
import numpy as np

data_dir = '/tmp/LT51040772009244ASA00'

# step 1: calculate NDVI
# ndvi = (nir - red)/(nir + red)
nir_file = os.path.join(data_dir, 'B40.TIF')
nir_raster = rasterio.open(nir_file, 'r')
band_nir = nir_raster.read(1)

red_file = os.path.join(data_dir, 'B30.TIF')
red_raster = rasterio.open(red_file, 'r')
band_red = red_raster.read(1)

a = (band_nir - band_red)
b = (band_nir + band_red)
# ndvi = np.divide(a,b, out=np.zeros_like(a), where=b!=0)
ndvi = np.divide(a,b, where=b!=0)
print(ndvi)

# step 2: calculate a new band to replace band green
# new_band = (b1 GT 0 AND b1 LE 0.1)*(0.15*b2+0.85*b3)+(b1 GT 0.1 AND b1 LE  0.2)*(0.2*b2+0.8*b3) + (b1 GT 0.2 AND b1 LE 0.3)*(0.25*b2+0.75*b3)+(b1 GT 0.3 AND b1 LE 0.4)*(0.3*b2+0.7*b3)+(b1 GT 0.4)*(0.35*b2+0.65*b3)+ (b1 LE 0)*b3”
# b1 is ndvi, b2 is nir, b3 is green
green_file = os.path.join(data_dir, 'B20.TIF')
green_raster = rasterio.open(green_file, 'r')
band_green = green_raster.read(1)

# #the first kind of new band formula
# mask1 = (ndvi > 0) & (ndvi <= 0.1).astype(int)
# mask2 = (ndvi > 0.1) & (ndvi <= 0.2).astype(int)
# mask3 = (ndvi > 0.2) & (ndvi <= 0.3).astype(int)
# mask4 = (ndvi > 0.3) & (ndvi <= 0.4).astype(int)
# mask5 = (ndvi > 0.4).astype(int)
# mask6 = (ndvi < 0).astype(int)
#
# new_band = ndvi * mask1 * (0.15 * band_nir + 0.85 * band_green) + ndvi * mask2 * (
# 0.2 * band_nir + 0.8 * band_green) + ndvi * mask3 * (0.25 * band_nir + 0.75 * band_green) + ndvi * mask4 * (
# 0.3 * band_nir + 0.7 * band_green) + ndvi * mask5 * (0.35 * band_nir + 0.65 * band_green) + ndvi * mask6 * band_green

#the second kind of new band formula
new_band = 0.8*band_green+0.2*band_nir
print(new_band)
new_band_file = os.path.join(data_dir,'B00_1.TIF')
with rasterio.open(new_band_file, 'w', driver='GTiff', width=new_band.shape[1], height=new_band.shape[0],
                   crs=nir_raster.crs, transform=nir_raster.transform, dtype=np.uint16, nodata=256, count=1) as dst:
    # Write the src array into indexed bands of the dataset. If `indexes` is a list, the src must be a 3D aray of matching shape. If an int, the src must be a 2D array.
    dst.write(new_band.astype(rasterio.uint16),indexes=1)

# step 3: gdal_merge band red, new band, and band blue
blue_file = os.path.join(data_dir, 'B10.TIF')
cmd = 'gdal_merge.py -separate -tap -o %s %s %s %s'%(os.path.join(data_dir,'B_true1.tif'),red_file, new_band_file, blue_file)
os.system(cmd)