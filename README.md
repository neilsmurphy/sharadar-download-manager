# Sharadar Download Manager: a Python tool for downloading sharadar data from Quandl



[![GitHub issues:](https://img.shields.io/github/issues/neilsmurphy/sharadar-download-manager)](https://github.com/neilsmurphy/sharadar-download-manager/issues)
[![GitHub stars:](https://img.shields.io/github/stars/neilsmurphy/sharadar-download-manager)](https://github.com/neilsmurphy/sharadar-download-manager/stargazers)
[![GitHub license:](https://img.shields.io/github/license/neilsmurphy/sharadar-download-manager)](https://github.com/neilsmurphy/sharadar-download-manager/blob/master/LICENSE)
![Style:](https://img.shields.io/badge/code%20style-black-black)
# Important: Still under development. Not deployed for use.



## What is it?  
Sharadar Download Manager is a simple script for assisting in downloading the Sharadar stock tables from Quandl. It aims
to simplify downloading and updating these files to local csv files and databases. It seeks to make downloading 
Sharadar data files easier for new or non-programmers. It is not a bulk downloader as the 
Quandl python API does not allow for concurrency.

## Main Features
Here are a few of the things the Sharadar Download Manager does well. 
- Easy input of dates, file options, and directory and filenames.
- Can select one, many or all tables to download.
- Can display table results after download to confirm data is saved.
- Automatic multi-threading for speed.
- Size of file management to ensure limits are not exceeded.
- Reads current datafiles to determine latest date available.
- Api-key stored for ease of use on a regular basis.

## Where to get it
At the moment, just download or clone the repository from github. To run this file you just need the main "update.py"
file.  

## Dependencies
Packages required: 

[Quandl](https://github.com/quandl/quandl-python)  
[Pandas](https://github.com/pandas-dev/pandas)

## License
[MIT](https://github.com/neilsmurphy/sharadar-download-manager/blob/master/LICENSE)

## Documentation
Coming.

## Getting Help
Coming.









