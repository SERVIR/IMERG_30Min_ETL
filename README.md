<a href="https://www.servirglobal.net//">
    <img src="https://www.servirglobal.net/Portals/0/Images/Servir-logo.png" alt="SERVIR Global"
         title="SERVIR Global" align="right" />
</a>


IMERG 30Minute ETL
=======================
> Python script for automating the Extract, Transform, and Load of raster data from a source ftp location into a file geodatabase mosaic datasets (shared as image services)

## Introduction:
This ETL (Extract, Transform, and Load) retrieves all of the most recent 30 Minute IMERG (Integrated Multi-satellitE Retrievals for GPM) precipitation data from the [NASA/Goddard Space Flight Center's GPM Team and Precipitation Processing System (PPS)](http://pmm.nasa.gov/GPM) ftp server and processes/loads the data into a file geodatabase mosaic dataset supporting a [30 Minute](https://gis1.servirglobal.net/arcgis/rest/services/Test/IMERG_30Min_ImgSvc/ImageServer) Image Service.  The source tif files are generated every 30 minutes in "Early" and "Late" products and placed on the ftp site (currently jsimpson.pps.eosdis.nasa.gov - please see [https://pmm.nasa.gov/data-access](https://pmm.nasa.gov/data-access) for more information about accessing the data products).  This process maintains an up-to-date, rolling set of 30 Minute raster files in a file geodatabase mosaic dataset for the last 90 days.

## Details: 
The high-level processing details are:
1. Retrieve the latest "Late" and latest "Early" date timestamps from the files already in the mosaic dataset.
2. --------------- LATE Processing ---------------
3. Connect to the source ftp site and based on the latest dates found in the mosaic dataset, set the proper ftp folder and check for "Late" files that may need to be downloaded. Continue looping through the ftp folder hierarchy (from the latest date found in the mosaic up through today's date) looking for "Late" files to download.
3. Download (to a temp extract folder) any "Late" files from the ftp site that were found to fall between the latest "Late" date already in the mosaic dataset up through today's date.
4. Process through the "Late" files in the temp extract folder and a.) check if there is a corresponding "Early" file already present in the mosaic dataset. If so, delete the "Early" raster before adding the "Late" raster. b.) rewrite/save each "Late" file to it's proper final folder location, and c.) load each file to the file geodatabase 30 minute mosaic dataset.
5. As each file is processed successfully, delete the temp extract copy of the file.
6. --------------- Early Processing ---------------
7. Connect to the source ftp site and based on the latest "Late" date found processing the files above, set the proper ftp folder and check for "Early" files that may need to be downloaded. Continue looping through the ftp folder hierarchy (from the latest "Late" date found in the mosaic up through today's date) looking for "Early" files to download.
8. Download (to a temp extract folder) any "Early" files from the ftp site that were found to be later than the latest "Late" date already in the mosaic dataset AND later than the latest "Early" date already in the mosaic dataset (to keep from reprocessing any "Early" files more than once.).
9. Process through the "Early" files in the temp extract folder and a.) rewrite/save each "Early" file to it's proper final folder location, and b.) load each file to the file geodatabase 30 minute mosaic dataset.
10. As each file is processed successfully, delete the temp extract copy of the file.
11. Calculate statistics on the mosaic dataset.
12. Compact the file geodatabase.
13. Refresh (Stop and Restart) the service.

The "Early" files show up on the ftp site first as raw or forecast data.  Then, as the "Late" files for the same date/time periods are processed and become available, they are placed on the ftp site, with a slightly different filename, and in a different folder hierarchy.  Each time this script runs and finds new "Late" files to add to the mosaic dataset, it first checks to see if there are any corresponding "Early" files representing the same date/time period as the late files being processed.  If corresponding "Early" files are found, those are deleted prior to adding the new replacement "Late" files.

As both the "Early" and "Late" ftp files are generated in a folder hierarchies broken down by ../(basefolder)/(year)/(month), this script queries the mosaic dataset for the latest dates already processed to determine the source ftp folder locations and then downloads the latest 30 Minute files based on the date/time stamp in the file names.  (The files are named similar to '3B-HHR-L.MS.MRG.3IMERG.20180809-S233000-E235959.1410.V05B.30Min.tif' and the code logic parses out the date/start time from the filename string to determine the latest files.)  Processing the "Late" files first, then the "Early" files, once the most recent files are downloaded to a temp extract folder, the script then processes each file in that folder and extracts only pixel values > 0 and saves the resulting files into the source folder supporting the mosaic dataset. As each downloaded file is loaded into it's mosaic dataset and copied into the folder supporting the mosaic dataset, the downloaded file is deleted from the temp extract folder.  Finally, some file geodatabase and mosaic dataset maintenance is performed before the ArcGIS Image service is stopped and restarted to reflect the added data.

## Environment:
IMERG_30Min_ETL.py is the main script file and was created and tested with python 2.7. The script relies on Esri's Arcpy module, as well as their Spatial Analyst extension for the arcpy.sa.ExtractByAttributes() method.  The tif files are loaded into a raster mosaic dataset within an Esri file geodatabase.  The file geodatabase and the mosaic dataset can be located and named whatever you want - these settings are ultimately stored in the config.pkl file.

The IMERG_30Min_Pickle.py file contains a dictionary object with the needed configuration parameters and is used to generate a configuration file (config.pkl) that is read by the main script at run time.  Please carefully modify the paths and username/password variables in IMERG_30Min_Pickle.py to meet your needs!  IMERG_30Min_Pickle.bat is simply a batch file to run the IMERG_30Min_Pickle.py file to generate config.pkl.

Below are the configuration settings that are stored in the pickle file and their description:
```
      'extract_EarlyFolder':            Local folder where the "Early" ftp files will be downloaded.
      'extract_LateFolder':             Local folder where the "Late" ftp files will be downloaded.
      'final_Folder':                   Local source folder supporting the mosaic dataset. This is where the downloaded files will ultimately reside once loaded into the mosaic.
      'logFileDir':                     Local folder where the log file will be written.
      'logFilePrefix':                  Prefix/Name for the log file.  i.e. 'IMERG_30Min'
      'GDBPath':                        Path and filename for the file geodatabase.  i.e. 'C:/somefolder/myFileGeodatabase.gdb'
      '1DayDSName':                     Name of the mosaic dataset for the 1 Day IMERG data.  i.e. 'IMERG1Day'
      '3DayDSName':                     Name of the mosaic dataset for the 3 Day IMERG data.  i.e. 'IMERG3Day'
      '7DayDSName':                     Name of the mosaic dataset for the 7 Day IMERG data.  i.e. 'IMERG7Day'
      'rasterTimeProperty':             Name of the field in the mosaic dataset that will receive the main date/time value.  i.e. 'timestamp'
      'rasterStartTimeProperty':        Name of the field in the mosaic dataset that will receive the starting offset date/time value.  i.e. 'start_datetime'  (= 15 min prior to rasterTimeProperty)
      'rasterEndTimeProperty':          Name of the field in the mosaic dataset that will receive the ending offset date/time value.  i.e. 'end_datetime'  (= 15 min after rasterTimeProperty)
      'RegEx_StartDateFilterString':    A regular expression format string that helps identify the date and start timestamp portion within the IMERG filenames.  i.e. '\d{4}[01]\d[0-3]\d-S[0-2]\d{5}'
      'GDB_DateFormat':                 A format string for dates.  i.e. '%Y%m%d%H%M'
      'Filename_StartDateFormat':       A format string that helps identify the date and start timestamp portion within the IMERG filenames.  i.e. '%Y%m%d-S%H%M%S'
      'ftp_host':                       The name of the ftp site for downloading IMERG data.  i.e. 'jsimpson.pps.eosdis.nasa.gov'
      'ftp_user':                       ftp site USERNAME
      'ftp_pswrd':                      ftp site PASSWORD
      'ftp_baseLateFolder':             ftp site base folder for where we will retrieve the 1, 3, and 7 day files.  i.e. '/data/imerg/gis'
      'svc_adminURL':                   Base ArcGIS Admin URL for your Image Services. i.e. 'https://gis1.servirglobal.net/arcgis/admin'
      'svc_username':                   ArcGIS Admin USERNAME
      'svc_password':                   ArcGIS Admin PASSWORD
      'svc_folder':                     Name of folder where the Image Services reside. i.e. 'Test' or 'Global' or '#' (if in root).
      'svc_Name':                       Name of the 30 Minute Image Service
      'svc_Type':                       Type of service.  i.e. 'ImageServer' or 'MapServer'
```

## Instructions to prep the script for running:
1.	Go to IMERG_30Min_Pickle.py and CAREFULLY enter your specific paths and credentials.
2.  Go to IMERG_30Min_Pickle.bat and a.) check the path to your version of python.exe, and b.) update the path to your copy of IMERG_30Min_Pickle.py.
3.  Run IMERG_30Min_Pickle.bat to generate the 'config.pkl' settings file in the same folder.  (config.pkl file is required for the main script.)
4.	Go to IMERG_30Min_ETL.bat and a.) check the path to your version of python.exe, and b.) update the path to your copy of IMERG_30Min_ETL.py.
5.  Run IMERG_30Min_ETL.bat to execute the main script.

