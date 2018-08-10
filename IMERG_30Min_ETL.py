# -------------------------------------------------------------------------------
# Name:        IMERG_30Min_ETL.py
# Purpose:     Retrieve IMERG 30 Minute "late" and "early" files from their source FTP location and load them into a
#               file geodatabase raster mosaic dataset.
#               The process uses dates from entries already in the GDB to know what new files to look for/retrieve/add.
#               It processes the "late" ftp files first, in which some corresponding "early" files already in the
#               GDB will need to be replaced by some of the "late" files.  Then it processes any newer "early" files.
#
# Author:               Lance Gilliland / SERVIR GIT Team       2018
# Last Modified By:
# Copyright:   (c) SERVIR 2018
#
# Note: This is a rewrite of the initial IMERG ETL - some portions of the initial code were reused. This version
#       takes a "get whatever rasters are available between certain dates" approach to downloading the source rasters
#       from the FTP site vs. "building a predictive list of filenames to try to download" as was the initial approach.
# -------------------------------------------------------------------------------

import arcpy
import argparse  # required for processing command line arguments
import datetime
import time
import os

import pickle
import logging
import glob  # required for usage within deleteOutOfDateRasters()

import linecache  # required for capture_exception()
import sys  # required for capture_exception()

import re  # required for Regular Expressions

import json  # required for RefreshService() (stopping and starting services)
import urllib  # required for RefreshService() (stopping and starting services) and retrieving remote files.
import urllib2  # required for retrieving remote files.

import ftplib  # require for ftp downloads

import shutil  # required for DeleteFolderContents()


# ------------------------------------------------------------
# Read configuration settings
# Global Variables - contents will not change during execution
# ------------------------------------------------------------
pkl_file = open('config.pkl', 'rb')
myConfig = pickle.load(pkl_file)
pkl_file.close()


def setupArgs():
    # Setup the argparser to capture any arguments...
    parser = argparse.ArgumentParser(__file__,
                                     description="This is the ETL script for the GPM IMERG 30 Minute dataset!")
    # Optional argument
    parser.add_argument("-l", "--logging",
                        help="the logging level at which the script should report",
                        type=str, choices=['debug', 'DEBUG', 'info', 'INFO', 'warning', 'WARNING', 'error', 'ERROR'])
    return parser.parse_args()


# Common function used by many!!
def capture_exception():
    # Not clear on why "exc_type" has to be in this line - but it does...
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    s = '### ERROR ### [{}, LINE {} "{}"]: {}'.format(filename, lineno, line.strip(), exc_obj)
    return s


def getScriptPath():
    # Returns the path where this script is running
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def getScriptName():
    # Tries to get the name of the script being executed...  returns "" if not found...
    try:
        # Get the name of this script!
        scriptFullPath = sys.argv[0]
        if len(scriptFullPath) < 1:
            return ""
        else:
            # In case it is the full pathname, split it...
            scriptPath, scriptLongName = os.path.split(scriptFullPath)
            # Split again to separate extension...
            scriptName, scriptExt = os.path.splitext(scriptLongName)
            return scriptName

    except:
        return ""


# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % seconds
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)


# Get a new time object
def get_NewStart_Time():
    timeStart = time.time()
    return timeStart


# Get the amount of time elapsed from the input time.
def get_Elapsed_Time_As_String(timeInput):
    return timeElapsed(timeInput)


def GetConfigString(variable):
    try:
        global myConfig
        return myConfig[variable]
    except:
        logging.error("### ERROR ###: Config variable NOT FOUND: {0}".format(variable))
        return ""


def GetRasterDatasetCount(mosaicDS):
    """
    Creates a memory table view of the raster mosaic dataset and retrieves/returns the record count.
    Returns 0 if error.
    """
    try:
        arcpy.MakeTableView_management(mosaicDS, "RasterView")
        theCount = int(arcpy.GetCount_management("RasterView").getOutput(0))
        arcpy.Delete_management("RasterView")
        return theCount
    except:
        err = capture_exception()
        logging.error(err)
        return 0


def create_folder(thePath):
    # Creates a directory on the file system if it does not already exist.
    # Then checks to see if the folder exists.
    # Returns True if the folder exists, returns False if it does not.
    try:
        # Create a location for the file if it does not exist..
        if not os.path.exists(thePath):
            os.makedirs(thePath)
        # Return the status
        return os.path.exists(thePath)
    except:
        return False


# Not currently called, but keep for future use...
def deleteFolderContents(folder):
    """
    Clean out the folder; Note - Does not delete the folder - just empties all files from it.
    """
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception, e:
            logging.error('### Error occurred in deleteFolderContents removing files ###, %s' % e)
    try:
        shutil.rmtree(folder + 'info', ignore_errors=True)
    except Exception, e:
            logging.error('### Error occurred in deleteFolderContents removing info folder ###, %s' % e)
    try:
        shutil.rmtree(folder + 'temp', ignore_errors=True)
    except Exception, e:
            logging.error('### Error occurred in deleteFolderContents removing temp folder ###, %s' % e)


def Get_StartDateTime_FromString(theString, regExp_Pattern, source_dateFormat):
    """
    # Search a string (or filename) for a date by using the regular expression pattern passed in, then use the
    # date format passed in (which matches the filename date format) to convert the regular expression output
    # into a datetime. Return None if any step fails.
    """
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern, theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            # If needed, this is where to insert a log entry or other notification that no date was found.
            return None
        else:
            # Found a string similar to:  20150802-S083000
            sExpStr = reItemsList[0]
            # Get a datetime object using the format from the filename.
            # The source_dateFormat should be a string similar to '%Y%m%d-S%H%M%S'
            dateObj = datetime.datetime.strptime(sExpStr, source_dateFormat)
            return dateObj
    except:
        return None


def GetLatest_EarlyOrLateDate_fromMosaicDataset(mosaicDS, early_or_late):
    """
    Query the Raster Mosaic Dataset and return the latest date from the specified "early" or "late" raster entries.
    If a valid date is not found in the GDB, a semi-random default date is returned. The date is returned as a
    datetime object, not a string! If there is an error/exception, None is returned.
    """
    try:
        TimestampField = GetConfigString("rasterTimeProperty")
        DataAgeField = GetConfigString("rasterDataAgeProperty")
        GDBDateFormat = GetConfigString("GDB_DateFormat")

        # Query for the timestamp values from the GDB.
        # SQLWhere = "NAME IS NOT NULL AND " + TimestampField + " IS NOT NULL AND " + \
        #            DataAgeField + " = \'" + early_or_late + "\'"
        SQLWhere = TimestampField + " IS NOT NULL AND " + \
                   DataAgeField + " = \'" + early_or_late + "\'"
        # Sort most recent date first (descending) so we can just grab the first row then break out of the for loop!
        # Also, only request the Timestamp field as that is all we need and it should be faster.
        # rows = arcpy.SearchCursor(mosaicDS, SQLWhere, '', fields="Name; " + TimestampField,
        #                           sort_fields=TimestampField + " D")
        rows = arcpy.SearchCursor(mosaicDS, SQLWhere, '', fields=TimestampField,
                                  sort_fields=TimestampField + " D")

        theDate = None
        for r in rows:
            theDate = r.getValue(TimestampField)
            theDate = datetime.datetime.strptime(theDate.strftime(GDBDateFormat), GDBDateFormat)
            break

        if theDate is None:
            # Set a default date
            if "EARLY" in early_or_late:
                # Randomly default to 1 day back from today
                oneDayAgo = datetime.datetime.now() - datetime.timedelta(days=1)
                theDate = datetime.datetime.strptime(oneDayAgo.strftime(GDBDateFormat), GDBDateFormat)
            elif "LATE" in early_or_late:
                # Randomly default to 5 days back from today
                xDaysAgo = datetime.datetime.now() - datetime.timedelta(days=3)
                theDate = datetime.datetime.strptime(xDaysAgo.strftime(GDBDateFormat), GDBDateFormat)  # currently %Y%m%d%H%M  i.e. 201810311630 (2018/10/31 4:30 PM)

        return theDate

    except:
        err = capture_exception()
        logging.error(err)
        return None


def deleteOutOfDateRasters(mymosaicDS, sourceFolder):
    """
    Based on the calculated date using today's date minus the "number of days to keep a raster",
    remove rasters from the mosaic dataset and also delete the corresponding files from the source folder.
    The "number of days to keep a raster" is read from the configuration file. A date query is used to remove
    rasters from the GDB, and the start date string from the source raster filenames is used to compare against
    the calculated keep date.
    """
    try:

        # Get number of days to keep rasters from config file
        numDays = int(GetConfigString("DaysToKeepRasters"))

        # Calculate the latest "keep" date
        oKeepDate = datetime.datetime.now() - datetime.timedelta(days=numDays)
        # Format the keep date into %Y-%m-%d format to get rid of the time portion!!!
        oFormattedKeepDate = datetime.datetime.strptime(oKeepDate.strftime('%Y-%m-%d'), '%Y-%m-%d')

        # Build the query string with the date - minus the time portion
        query = "timestamp < date '" + oKeepDate.strftime('%Y-%m-%d') + "'"

        # Remove rasters based on date query
        logging.info('Deleting out of date rasters from Mosaic DS where: ' + query)
        arcpy.RemoveRastersFromMosaicDataset_management(mymosaicDS, query,
                                                        "UPDATE_BOUNDARY", "MARK_OVERVIEW_ITEMS",
                                                        "DELETE_OVERVIEW_IMAGES")

        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")

        # Grab all raster files from the source folder.
        # The resulting list includes the entire path and file name.
        files = glob.glob(sourceFolder + "\*.tif")
        # Build a list of the files to delete
        rastersToDelete = []
        for rfile in files:
            # Get the name of the raster file from the entire path
            theName = rfile.rsplit('\\', 1)[-1]

            # Get the start date string from the file name.
            oFileDate = Get_StartDateTime_FromString(rfile, RegEx_StartDatePattern, Filename_StartDateFormat)
            if oFileDate is not None:

                # Convert ofileDate into %Y-%m-%d format before doing the compare!!!
                oFormattedFileDate = datetime.datetime.strptime(oFileDate.strftime('%Y-%m-%d'), '%Y-%m-%d')

                if oFormattedFileDate < oFormattedKeepDate:
                    # ... add the file to the delete list.
                    rastersToDelete.append(theName)

        # Delete the rasters files that are stored in the list.
        arcpy.env.workspace = sourceFolder
        for oldraster in rastersToDelete:
            arcpy.Delete_management(oldraster)

        # Report the number of files that got deleted...
        logging.info("Deleted {0} physical raster files!".format(str(len(rastersToDelete))))
        del rastersToDelete[:]

    except:
        err = capture_exception()
        logging.error(err)


#  --- NOTE! NOTE! NOTE! ---
# For some unknown reason, our server (where this script will be running) cannot connect to the FTP site where we need
# to download files from. So, a "proxy" server/location has been established to retrieve the files from the FTP site.
# For this reason, we have implemented another "Proxy" function further below that uses URLLIB to retrieve the files
# from the proxy location vs. this function that uses FTPLIB to retrieve the files from the ftp location.
#  --- NOTE! NOTE! NOTE! ---
def ProcessLateFiles(oTodaysDateTime, oLastLateDateTime):
    """
    Connects to the FTP site (base folder) and based on today's date and the last LATE GDB Date passed in, processes
    through the FTP Late folder hierarchy (year and month) and retrieves a list of filenames from each folder.

    Note:  Files on the FTP site are broken down into folders by Year and then by Month.
        Late files are in FTP folder hierarchy:     /data/imerg/gis/<year>/<month>
            e.g  /data/imerg/gis/2017/01 ... /data/imerg/gis/2017/02 ... /data/imerg/gis/2018/01 ...
    So we must use the last LATE Date from the GDB to know how far back in the FTP folder hierarchy we
    need to go to get new files. And we will need to process all month and year folders up to today's current
    year and month.

    As it processes through the FTP folders, the list of filenames found is reduced to only files that we want
    to keep/download by omitting any files that are dated prior to the last LATE Date passed in.  Also, we only want
    files that contain the letter "L" in position 7 of the name, and end in ".30min.tif".
    Once the list of files for each FTP folder is trimmed, the files are then downloaded to the proper extract location.
    """
    try:
        ftp_Host = GetConfigString("ftp_host")
        ftp_baseLateFolder = GetConfigString("ftp_baseLateFolder")
        ftp_UserName = GetConfigString("ftp_user")
        ftp_UserPass = GetConfigString("ftp_pswrd")

        # Grab a few settings we might need later.
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")
        targetFolder = GetConfigString("extract_LateFolder")

        bConnectionCreated = False
        ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        # time.sleep(1)    # Don't believe this is needed...
        bConnectionCreated = True

        # Get the year and month from each date passed in
        oTodaysYear = oTodaysDateTime.year
        oTodaysMonth = oTodaysDateTime.month
        oFolderYear = oLastLateDateTime.year
        oFolderMonth = oLastLateDateTime.month

        # Starting with the last Late Date passed in, loop through the years and months and get all of the
        # files from the corresponding FTP folders
        while oFolderYear <= oTodaysYear:
            while oFolderMonth <= oTodaysMonth:

                # Change to the <baseFolder>/Year/Month FTP folder
                sYear = str(oFolderYear)
                sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
                ftpFolder = ftp_baseLateFolder + "/" + sYear + "/" + sMonth
                ftp_Connection.cwd(ftpFolder)

                # Initialize a list for names of ALL files that are in the FTP folder
                tmpList = []
                # Initialize a placeholder list for names of files that we ACTUALLY process/download
                actualList = []

                # Grab the list of ALL filenames from the current FTP folder...
                line = ftp_Connection.retrlines("NLST", tmpList.append)

                # Loop through each item in the tmpList and verify if we want to keep/download it!
                # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
                # To keep a file, it must:
                #   - contain an "L" at position 7 in the filename.
                #   - be the proper type of file (contain the string ".30min.tif")
                #   - have a start date/time that is greater than the oLastLateDateTime passed in from the GDB
                for ftpFile in tmpList:
                    # If it is a 30Min tif file
                    if ".30min.tif" in ftpFile:
                        # If the item is a "Late" entry
                        if ftpFile[7] == 'L':
                            # Ex. filename format: 3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.30min.tif
                            # The start time (represented by "20150802-S083000") is used as the timestamp for each file.
                            # If the item's timestamp is later than the oLastLateDateTime, we want to keep it.
                            fileDate = Get_StartDateTime_FromString(ftpFile, RegEx_StartDatePattern, Filename_StartDateFormat)
                            if (fileDate is not None) and (fileDate > oLastLateDateTime):
                                # Download the ftpFile to the extract_LateFolder

                                # Keep track of the files that we actually process...
                                actualList.append(ftpFile)

                                targetExtractFile = os.path.join(targetFolder, ftpFile)
                                with open(targetExtractFile, "wb") as f:

                                    ftp_Connection.retrbinary("RETR %s" % ftpFile, f.write)  # fully qualify ftpFile?
                                    # time.sleep(1)  # Don't believe this is needed...

                # Delete the temp list of filenames before moving to a new FTP folder
                del tmpList[:]

                # Report the number of files actually processed from this folder...
                logging.info("{0} Late files downloaded from folder: {1}.".format(str(len(actualList)), ftpFolder))
                for x in actualList:
                    logging.debug("\t\t{0}".format(x))
                # Delete the list of files actually processed before moving to a new FTP folder
                del actualList[:]

                # End of "month" while loop - Increment the month
                oFolderMonth = oFolderMonth + 1

            # End of "year" while loop - Increment the year
            oFolderYear = oFolderYear + 1

        # Disconnect from ftp
        ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        if bConnectionCreated:
            ftp_Connection.close()
        return False


#  --- NOTE! NOTE! NOTE! ---
# This function is a replacement for ProcessLateFiles() above. We cannot rely on FTP functionality, so we
# are using a proxy server that provides access to the needed ftp files via URLLIB functionality.
#  --- NOTE! NOTE! NOTE! ---
def ProcessLateFiles_FromProxy(oTodaysDateTime, oLastLateDateTime):
    """
    Connects to the Proxy site (via URLLIB) and based on today's date and the last LATE GDB Date passed in, processes
    through the FTP Late folder hierarchy (year and month) and retrieves a list of filenames from each folder.

    Note:  Files on the FTP site are broken down into folders by Year and then by Month.
        Late files are in FTP folder hierarchy:     /data/imerg/gis/<year>/<month>
            e.g  /data/imerg/gis/2017/01 ... /data/imerg/gis/2017/02 ... /data/imerg/gis/2018/01 ...
    So we must use the last LATE Date from the GDB to know how far back in the FTP folder hierarchy we
    need to go to get new files. And we will need to process all month and year folders up to today's current
    year and month.

    As it processes through the FTP folders, the list of filenames found is reduced to only files that we want
    to keep/download by omitting any files that are dated prior to the last LATE Date passed in.  Also, we only want
    files that contain the letter "L" in position 7 of the name, and end in ".30min.tif".
    Once the list of files for each FTP folder is trimmed, the files are then downloaded to the proper extract location.
    """
    try:
        # Grab a few settings we need later.
        ftpHost = "ftp://" + GetConfigString("ftp_host")
        ftp_baseLateFolder = GetConfigString("ftp_baseLateFolder")
        targetFolder = GetConfigString("extract_LateFolder")
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")

        # bConnectionCreated = False
        # ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        # bConnectionCreated = True

        # Get the year and month from each date passed in
        oTodaysYear = oTodaysDateTime.year
        oTodaysMonth = oTodaysDateTime.month
        oFolderYear = oLastLateDateTime.year
        oFolderMonth = oLastLateDateTime.month

        # Starting with the last Late Date passed in, loop through the years and months and get all of the
        # files from the corresponding FTP folders
        while oFolderYear <= oTodaysYear:
            while oFolderMonth <= oTodaysMonth:

                # Change to the <baseFolder>/Year/Month FTP folder
                sYear = str(oFolderYear)
                sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
                ftpFolder = ftp_baseLateFolder + "/" + sYear + "/" + sMonth
                proxyDir = "https://proxy.servirglobal.net/ProxyFTP.aspx?directory="
                logging.debug("FTPProxy Directory URL = {0}".format(proxyDir + ftpHost + ftpFolder + "/"))
                # ftp_Connection.cwd(ftpFolder)
                req = urllib2.Request(proxyDir + ftpHost + ftpFolder + "/")  # last slash is required
                response = urllib2.urlopen(req)

                # Initialize a placeholder list for names of files that we ACTUALLY process/download
                actualList = []

                # Grab the list of ALL filenames from the current FTP folder...
                # line = ftp_Connection.retrlines("NLST", tmpList.append)
                tmpList = response.read().split(",")

                # Loop through each item in the tmpList and verify if we want to keep/download it!
                # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
                # To keep a file, it must:
                #   - contain an "L" at position 7 in the filename.
                #   - be the proper type of file (contain the string ".30min.tif")
                #   - have a start date/time that is greater than the oLastLateDateTime passed in from the GDB
                for ftpFile in tmpList:
                    # If it is a 30Min tif file
                    if ".30min.tif" in ftpFile:
                        # If the item is a "Late" entry
                        if ftpFile[7] == 'L':
                            # Ex. filename format: 3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.30min.tif
                            # The start time (represented by "20150802-S083000") is used as the timestamp for each file.
                            # If the item's timestamp is later than the oLastLateDateTime, we want to keep it.
                            fileDate = Get_StartDateTime_FromString(ftpFile, RegEx_StartDatePattern, Filename_StartDateFormat)
                            if (fileDate is not None) and (fileDate > oLastLateDateTime):
                                # Download the ftpFile to the extract_LateFolder

                                # Keep track of the files that we actually process...
                                actualList.append(ftpFile)

                                sourceExtractFile = ftpHost + os.path.join(ftpFolder, ftpFile)
                                targetExtractFile = os.path.join(targetFolder, ftpFile)
                                # with open(targetExtractFile, "wb") as f:
                                #     ftp_Connection.retrbinary("RETR %s" % ftpFile, f.write)  # fully qualify ftpFile?
                                fx = open(targetExtractFile, "wb")
                                fx.close()
                                os.chmod(targetExtractFile, 0777)
                                try:
                                    urllib.urlretrieve("https://proxy.servirglobal.net/ProxyFTP.aspx?url=" +
                                                       sourceExtractFile, targetExtractFile)
                                except:
                                    logging.info("Error retrieving latest 1Day file from proxy: {0}".format(sourceExtractFile))
                                    os.remove(targetExtractFile)

                # Delete the temp list of filenames before moving to a new FTP folder
                del tmpList[:]

                # Report the number of files actually processed from this folder...
                logging.info("{0} Late files downloaded from folder: {1}.".format(str(len(actualList)), ftpHost + ftpFolder))
                for x in actualList:
                    logging.debug("\t\t{0}".format(x))
                # Delete the list of files actually processed before moving to a new FTP folder
                del actualList[:]

                # End of "month" while loop - Increment the month
                oFolderMonth = oFolderMonth + 1

            # End of "year" while loop - Increment the year
            oFolderYear = oFolderYear + 1

        # Disconnect from ftp
        # ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        # if bConnectionCreated:
        #     ftp_Connection.close()
        return False


#  --- NOTE! NOTE! NOTE! ---
# For some unknown reason, our server (where this script will be running) cannot connect to the FTP site where we need
# to download files from. So, a "proxy" server/location has been established to retrieve the files from the FTP site.
# For this reason, we have implemented another "Proxy" function further below that uses URLLIB to retrieve the files
# from the proxy location vs. this function that uses FTPLIB to retrieve the files from the ftp location.
#  --- NOTE! NOTE! NOTE! ---
def ProcessEarlyFiles(oLastLateDateTime, oTodaysDateTime, oLastEarlyDateTime):
    """
    Connects to the FTP site (base folder) and based on today's date, the latest LATE and EARLY GDB Dates passed in,
    processes through the FTP Early folder hierarchy (year and month) and retrieves a list of filenames to process.

    Note:  Files on the FTP site are broken down into folders by Year and then by Month.
        Early files are in FTP folder hierarchy:    /data/imerg/gis/early/<year>/<month>
            e.g  /data/imerg/gis/early/2017/01 ... /data/imerg/gis/early/2017/02 ... /data/imerg/gis/early/2018/01 ...
    We will use the last Late date from the GDB to know how far back in the FTP folder hierarchy we need to go to
    get any new Early files. We will need to process all month and year folders up to today's current year and month.

    Key Points:
    1.) We DO NOT want to add any "Early" files earlier than the latest "Late" file! Therefore, we must compare
    potential Early files against the last Late date passed in.
    2.) We also do not want to "re-add" any Early files that may have already been previously added to the GDB,
    therefore, we also must check if the potential Early ftp file is later than the last Early date passed in.

    As it processes through the FTP folders, the list of Early filenames found is reduced to only files that we want
    to keep/download by ensuring the files are dated later than the last Late Date passed in, as well as later than the
    last Early Date passed in.  Also, we only want files that contain the letter "E" in position 7 of the name, and
    end in ".30min.tif". Once the list of files for each FTP folder is trimmed, the files are then downloaded
    to the proper extract location.
    """
    try:
        ftp_Host = GetConfigString("ftp_host")
        ftp_baseEarlyFolder = GetConfigString("ftp_baseEarlyFolder")
        ftp_UserName = GetConfigString("ftp_user")
        ftp_UserPass = GetConfigString("ftp_pswrd")

        # Grab a few settings we might need later.
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")
        targetFolder = GetConfigString("extract_EarlyFolder")

        bConnectionCreated = False
        ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        # time.sleep(1)   # Don't believe this is needed...
        bConnectionCreated = True

        # Get the year and month from each date passed in
        oTodaysYear = oTodaysDateTime.year
        oTodaysMonth = oTodaysDateTime.month
        oFolderYear = oLastLateDateTime.year
        oFolderMonth = oLastLateDateTime.month

        # Starting with the last Early Date passed in, loop through the years and months and get all of the
        # files from the corresponding FTP folders
        while oFolderYear <= oTodaysYear:
            while oFolderMonth <= oTodaysMonth:

                # Change to the <baseFolder>/Year/Month FTP folder
                sYear = str(oFolderYear)
                sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
                ftpFolder = ftp_baseEarlyFolder + "/" + sYear + "/" + sMonth
                ftp_Connection.cwd(ftpFolder)

                # Initialize a list for names of ALL files that are in the FTP folder
                tmpList = []
                # Initialize a placeholder list for names of files that we ACTUALLY process/download
                actualList = []

                # Grab the list of ALL filenames from the current FTP folder...
                line = ftp_Connection.retrlines("NLST", tmpList.append)

                # Loop through each item in the tmpList and verify if we want to keep/download it!
                # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
                # To keep a file, it must:
                #   - contain an "E" at position 7 in the filename.
                #   - be the proper type of file (contain the string ".30min.tif")
                #   - have a start date/time that is greater than the oLastLateDateTime passed in from the GDB
                #   - have a start date/time that is greater than the oLastEarlyDateTime passed in from the GDB
                for ftpFile in tmpList:
                    # If it is a 30Min tif file
                    if ".30min.tif" in ftpFile:
                        # If the item is a "Late" entry
                        if ftpFile[7] == 'E':
                            # Ex. filename format: 3B-HHR-E.MS.MRG.3IMERG.20180801-S000000-E002959.0000.V05B.30min.tif
                            # The start time (represented by "20180801-S000000") is used as the timestamp for each file.
                            # If the item's timestamp is later than the oLastLateDateTime and the oLastEarlyDateTime,
                            # we want to keep it.
                            fileDate = Get_StartDateTime_FromString(ftpFile, RegEx_StartDatePattern, Filename_StartDateFormat)
                            if (fileDate is not None) and (fileDate > oLastLateDateTime)and (fileDate > oLastEarlyDateTime):
                                # Download the ftpFile to the extract_EarlyFolder

                                # Keep track of the files that we actually process...
                                actualList.append(ftpFile)

                                targetExtractFile = os.path.join(targetFolder, ftpFile)
                                with open(targetExtractFile, "wb") as f:

                                    ftp_Connection.retrbinary("RETR %s" % ftpFile, f.write)  # fully qualify ftpFile?
                                    # time.sleep(1)   # Don't believe this is needed...

                # Delete the temp list of filenames before moving to a new FTP folder
                del tmpList[:]

                # Report the number of files actually processed from this folder...
                logging.info("{0} Early files downloaded from folder: {1}.".format(str(len(actualList)), ftpFolder))
                for x in actualList:
                    logging.debug("\t\t{0}".format(x))
                # Delete the list of files actually processed before moving to a new FTP folder
                del actualList[:]

                # End of "month" while loop - Increment the month
                oFolderMonth = oFolderMonth + 1

            # End of "year" while loop - Increment the year
            oFolderYear = oFolderYear + 1

        # Disconnect from ftp
        ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        if bConnectionCreated:
            ftp_Connection.close()
        return False


#  --- NOTE! NOTE! NOTE! ---
# This function is a replacement for ProcessEarlyFiles() above. We cannot rely on FTP functionality, so we
# are using a proxy server that provides access to the needed ftp files via URLLIB functionality.
#  --- NOTE! NOTE! NOTE! ---
def ProcessEarlyFiles_FromProxy(oLastLateDateTime, oTodaysDateTime, oLastEarlyDateTime):
    """
    Connects to the Proxy site (via URLLIB) and based on today's date, the latest LATE and EARLY GDB Dates passed in,
    processes through the FTP Early folder hierarchy (year and month) and retrieves a list of filenames to process.

    Note:  Files on the FTP site are broken down into folders by Year and then by Month.
        Early files are in FTP folder hierarchy:    /data/imerg/gis/early/<year>/<month>
            e.g  /data/imerg/gis/early/2017/01 ... /data/imerg/gis/early/2017/02 ... /data/imerg/gis/early/2018/01 ...
    We will use the last Late date from the GDB to know how far back in the FTP folder hierarchy we need to go to
    get any new Early files. We will need to process all month and year folders up to today's current year and month.

    Key Points:
    1.) We DO NOT want to add any "Early" files earlier than the latest "Late" file! Therefore, we must compare
    potential Early files against the last Late date passed in.
    2.) We also do not want to "re-add" any Early files that may have already been previously added to the GDB,
    therefore, we also must check if the potential Early ftp file is later than the last Early date passed in.

    As it processes through the FTP folders, the list of Early filenames found is reduced to only files that we want
    to keep/download by ensuring the files are dated later than the last Late Date passed in, as well as later than the
    last Early Date passed in.  Also, we only want files that contain the letter "E" in position 7 of the name, and
    end in ".30min.tif". Once the list of files for each FTP folder is trimmed, the files are then downloaded
    to the proper extract location.
    """
    try:
        # Grab a few settings we need later.
        ftpHost = "ftp://" + GetConfigString("ftp_host")
        ftp_baseEarlyFolder = GetConfigString("ftp_baseEarlyFolder")
        targetFolder = GetConfigString("extract_EarlyFolder")
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")

        # bConnectionCreated = False
        # ftp_Connection = ftplib.FTP(ftp_Host, ftp_UserName, ftp_UserPass)
        # bConnectionCreated = True

        # Get the year and month from each date passed in
        oTodaysYear = oTodaysDateTime.year
        oTodaysMonth = oTodaysDateTime.month
        oFolderYear = oLastLateDateTime.year
        oFolderMonth = oLastLateDateTime.month

        # Starting with the last Early Date passed in, loop through the years and months and get all of the
        # files from the corresponding FTP folders
        while oFolderYear <= oTodaysYear:
            while oFolderMonth <= oTodaysMonth:

                # Change to the <baseFolder>/Year/Month FTP folder
                sYear = str(oFolderYear)
                sMonth = str(oFolderMonth).zfill(2)  # pad with zero if a single digit
                ftpFolder = ftp_baseEarlyFolder + "/" + sYear + "/" + sMonth
                proxyDir = "https://proxy.servirglobal.net/ProxyFTP.aspx?directory="
                logging.debug("FTPProxy Directory URL = {0}".format(proxyDir + ftpHost + ftpFolder + "/"))
                # ftp_Connection.cwd(ftpFolder)
                req = urllib2.Request(proxyDir + ftpHost + ftpFolder + "/")  # last slash is required
                response = urllib2.urlopen(req)

                # Initialize a placeholder list for names of files that we ACTUALLY process/download
                actualList = []

                # Grab the list of ALL filenames from the current FTP folder...
                # line = ftp_Connection.retrlines("NLST", tmpList.append)
                tmpList = response.read().split(",")

                # Loop through each item in the tmpList and verify if we want to keep/download it!
                # Note - There may be lots of different files/types in the FTP folder, we only need certain ones.
                # To keep a file, it must:
                #   - contain an "E" at position 7 in the filename.
                #   - be the proper type of file (contain the string ".30min.tif")
                #   - have a start date/time that is greater than the oLastLateDateTime passed in from the GDB
                #   - have a start date/time that is greater than the oLastEarlyDateTime passed in from the GDB
                for ftpFile in tmpList:
                    # If it is a 30Min tif file
                    if ".30min.tif" in ftpFile:
                        # If the item is a "Late" entry
                        if ftpFile[7] == 'E':
                            # Ex. filename format: 3B-HHR-E.MS.MRG.3IMERG.20180801-S000000-E002959.0000.V05B.30min.tif
                            # The start time (represented by "20180801-S000000") is used as the timestamp for each file.
                            # If the item's timestamp is later than the oLastLateDateTime and the oLastEarlyDateTime,
                            # we want to keep it.
                            fileDate = Get_StartDateTime_FromString(ftpFile, RegEx_StartDatePattern, Filename_StartDateFormat)
                            if (fileDate is not None) and (fileDate > oLastLateDateTime)and (fileDate > oLastEarlyDateTime):
                                # Download the ftpFile to the extract_EarlyFolder

                                # Keep track of the files that we actually process...
                                actualList.append(ftpFile)

                                sourceExtractFile = ftpHost + os.path.join(ftpFolder, ftpFile)
                                targetExtractFile = os.path.join(targetFolder, ftpFile)
                                # with open(targetExtractFile, "wb") as f:
                                #     ftp_Connection.retrbinary("RETR %s" % ftpFile, f.write)  # fully qualify ftpFile?
                                fx = open(targetExtractFile, "wb")
                                fx.close()
                                os.chmod(targetExtractFile, 0777)
                                try:
                                    urllib.urlretrieve("https://proxy.servirglobal.net/ProxyFTP.aspx?url=" +
                                                       sourceExtractFile, targetExtractFile)
                                except:
                                    logging.info("Error retrieving latest 1Day file from proxy: {0}".format(sourceExtractFile))
                                    os.remove(targetExtractFile)

                # Delete the temp list of filenames before moving to a new FTP folder
                del tmpList[:]

                # Report the number of files actually processed from this folder...
                logging.info("{0} Early files downloaded from folder: {1}.".format(str(len(actualList)), ftpHost + ftpFolder))
                for x in actualList:
                    logging.debug("\t\t{0}".format(x))
                # Delete the list of files actually processed before moving to a new FTP folder
                del actualList[:]

                # End of "month" while loop - Increment the month
                oFolderMonth = oFolderMonth + 1

            # End of "year" while loop - Increment the year
            oFolderYear = oFolderYear + 1

        # Disconnect from ftp
        # ftp_Connection.close()
        return True

    except:
        err = capture_exception()
        logging.error(err)
        # if bConnectionCreated:
        #     ftp_Connection.close()
        return False


def CheckEarlyRaster(sLateFile, mosaicDS):
    """
    Check the folder supporting the raster mosaic dataset to see if an "Early" raster corresponding to the "Late"
    raster passed in exists in the folder. If so, this indicates that we need to 1.) Remove the assoc. "Early"
    raster from the mosaic dataset, and 2.) Delete the "Early" physical file.
    """
    try:
        # Build the "Early" raster filename based on the "Late" raster filename passed in
        # (Basically update the 8th character in the filename from "L" to "E")
        # 3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.30min.tif
        sEarlyFile_firstPart = sLateFile[:7]  # "3B-HHR-"
        sEarlyFile_lastPart = sLateFile[8:]   # ".MS.MRG.3IMERG.20150802-S083000-E085959.0510.V05B.30min.tif"
        sEarlyFile = sEarlyFile_firstPart + "E" + sEarlyFile_lastPart

        # Get the folder supporting the raster mosaic dataset
        sourceFolder = GetConfigString('final_Folder')
        # Build the full path string to the "Early" raster file
        sFullPathEarlyRaster = os.path.join(sourceFolder, sEarlyFile)

        # If the "Early" file exists...
        if arcpy.Exists(sFullPathEarlyRaster):
            logging.debug("\t\t\tRemoving/deleting corresponding Early raster file.")
            # Get the name of the "Early" raster minus the .tif extension
            sEarlyFile_minusExt = os.path.splitext(sEarlyFile)[0]

            # Remove the "Early" raster from the mosaic dataset
            query = "Name = '" + sEarlyFile_minusExt + "'"
            arcpy.RemoveRastersFromMosaicDataset_management(mosaicDS, query,
                                                            "UPDATE_BOUNDARY", "MARK_OVERVIEW_ITEMS",
                                                            "DELETE_OVERVIEW_IMAGES")
            # Delete the physical file
            arcpy.Delete_management(sFullPathEarlyRaster)

    except:
        err = capture_exception()
        logging.error(err)


def LoadEarlyOrLateRasters(temp_workspace, early_or_late):
    """
    This function accepts a temp workspace (folder) and:
        1 - loads each raster .tif file from the temp folder into a mosaic dataset
        2 - deletes the temp_workspace copy of the raster after it is successfully added/moved to the mosaic dataset
        3 - populates certain attributes on each raster after it is loaded to the mosaic dataset
        4 - before loading the raster into the mosaic, if it is a "Late" raster, ensure that it's corresponding
            "Early" raster is first removed from the mosaic dataset and deleted from the source folder.
    """
    try:
        arcpy.CheckOutExtension("Spatial")
        # inSQLClause = "VALUE >= 0"
        # We do not want the zero values and we also do not want the "NoData" value of 29999.
        # So let's extract only the values above 0 and less than 29900.
        inSQLClause = "VALUE > 0"
        arcpy.env.workspace = temp_workspace
        arcpy.env.overwriteOutput = True

        # Grab some config settings that will be needed...
        final_RasterSourceFolder = GetConfigString('final_Folder')
        targetMosaic = os.path.join(GetConfigString('GDBPath'), GetConfigString('mosaicDSName'))
        RegEx_StartDatePattern = GetConfigString("RegEx_StartDateFilterString")
        Filename_StartDateFormat = GetConfigString("Filename_StartDateFormat")
        iCounter = 0

        # Build attribute name list for updates
        attrNameList = [GetConfigString('rasterTimeProperty'), GetConfigString('rasterStartTimeProperty'),
                        GetConfigString('rasterEndTimeProperty'), GetConfigString('rasterDataAgeProperty')]

        # List all raster in the temp_workspace
        rasters = arcpy.ListRasters()
        for raster in rasters:
            try:    # raster in rasters
                logging.debug('\t\tProcessing file: {0}'.format(raster))

                # If this is a "Late" raster, check the final source folder for a corresponding "Early" file.
                # If found, the "Early" raster needs to be removed from mosaic and the physical file deleted before
                # processing the "Late" raster.
                if early_or_late == 'LATE':
                    CheckEarlyRaster(raster, targetMosaic)

                # Save the file to the final source folder and load it into the mosaic dataset
                extract = arcpy.sa.ExtractByAttributes(raster, inSQLClause)
                finalRaster = os.path.join(final_RasterSourceFolder, raster)
                extract.save(finalRaster)
                arcpy.AddRastersToMosaicDataset_management(targetMosaic, "Raster Dataset", finalRaster,
                                                           "NO_CELL_SIZES", "NO_BOUNDARY", "NO_OVERVIEWS",
                                                           "2", "#", "#", "#", "#", "NO_SUBFOLDERS",
                                                           "OVERWRITE_DUPLICATES", "NO_PYRAMIDS",
                                                           "NO_STATISTICS", "NO_THUMBNAILS",
                                                           "Add Raster Datasets", "#")

                # If we get here, we have successfully added the raster to the mosaic and saved it to its final
                # source location, so lets go ahead and remove it from the temp extract folder now...
                arcpy.Delete_management(raster)
                iCounter += 1

                try:    # Set Attributes
                    # Update the attributes on the raster that was just added to the mosaic dataset

                    # Initialize and build attribute expression list
                    attrExprList = []

                    # Get the raster name minus the .tif extension
                    rasterName_minusExt = os.path.splitext(raster)[0]

                    # Get the start datetime stamp from the filename
                    dTimestamp = Get_StartDateTime_FromString(rasterName_minusExt, RegEx_StartDatePattern, Filename_StartDateFormat)
                    attrExprList.append(dTimestamp)

                    dStartTime = dTimestamp - datetime.timedelta(minutes=15)
                    attrExprList.append(dStartTime)

                    dEndTime = dTimestamp + datetime.timedelta(minutes=15)
                    attrExprList.append(dEndTime)

                    attrExprList.append(early_or_late)

                    # wClause = arcpy.AddFieldDelimiters(targetMosaic, "Name") + " = '" + rasterName + "'"
                    wClause = "Name = '" + rasterName_minusExt + "'"

                    with arcpy.da.UpdateCursor(targetMosaic, attrNameList, wClause) as cursor:
                        for row in cursor:
                            for idx in range(len(attrNameList)):
                                row[idx] = attrExprList[idx]
                            cursor.updateRow(row)

                    del cursor

                except:  # Set Attributes
                    err = capture_exception()
                    logging.warning("\t...Raster attributes not set for raster {0}. Error = {1}".format(raster, err))

            except:   # raster in rasters
                err = capture_exception()
                logging.warning('\t...Raster {0} not loaded into mosaic! Error = {1}'.format(raster, err))

        del rasters
        logging.info('{0} {1} files processed for the mosaic dataset.'.format(str(iCounter), early_or_late))

    except:
        err = capture_exception()
        logging.error(err)


def refreshService():
    """
    Restart the ArcGIS Service (Stop and Start) using the URL token service.
    """
    # Grab the needed config settings for this service
    svc_AdminURL = GetConfigString('svc_adminURL')
    svc_Username = GetConfigString('svc_username')
    svc_Password = GetConfigString('svc_password')
    svc_Folder = GetConfigString('svc_folder')
    svc_Type = GetConfigString('svc_Type')
    svc_Name = GetConfigString('svc_Name')

    # Try and stop the service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f": "json", "username": svc_Username,
                                        "password": svc_Password, "client": "requestip"})
        tokenResponse = urllib.urlopen(svc_AdminURL + "/generateToken?", tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the service
        stopParams = urllib.urlencode({"token": token, "f": "json"})
        stopResponse = urllib.urlopen(svc_AdminURL + "/services/" + svc_Folder + "/" + svc_Name + "." +
                                      svc_Type + "/stop?", stopParams).read()
        stopResponseJSON = json.loads(stopResponse)
        stopStatus = stopResponseJSON["status"]

        if stopStatus <> "success":
            logging.warning("UNABLE TO STOP SERVICE " + str(svc_Folder) + "/" + str(svc_Name) +
                            "/" + str(svc_Type) + " STATUS = " + stopStatus)
        else:
            logging.info("Service: " + str(svc_Name) + " has been stopped.")

    except Exception, e:
        logging.error("### ERROR ### - Stop Service failed for " + str(svc_Name) + ", System Error Message: " + str(e))

    # Try and start the service
    try:
        # Get a token from the Administrator Directory
        tokenParams = urllib.urlencode({"f": "json", "username": svc_Username,
                                        "password": svc_Password, "client":"requestip"})
        tokenResponse = urllib.urlopen(svc_AdminURL + "/generateToken?", tokenParams).read()
        tokenResponseJSON = json.loads(tokenResponse)
        token = tokenResponseJSON["token"]

        # Attempt to stop the current service
        startParams = urllib.urlencode({"token": token, "f": "json"})
        startResponse = urllib.urlopen(svc_AdminURL + "/services/" + svc_Folder + "/" + svc_Name + "." +
                                       svc_Type + "/start?", startParams).read()
        startResponseJSON = json.loads(startResponse)
        startStatus = startResponseJSON["status"]

        if startStatus == "success":
            logging.info("Started service: " + str(svc_Folder) + "/" + str(svc_Name) + "/" + str(svc_Type))
        else:
            logging.warning("UNABLE TO START SERVICE " + str(svc_Folder) + "/" + str(svc_Name) +
                            "/" + str(svc_Type) + " STATUS = " + startStatus)
    except Exception, e:
        logging.error("### ERROR ### - Start Service failed for " + str(svc_Name) + ", System Error Message: " + str(e))


def main():
    try:

        # Setup any required and/or optional arguments to be passed in.
        args = setupArgs()

        # Check if the user passed in a log level argument, either DEBUG, INFO, or WARNING. Otherwise, default to INFO.
        if args.logging:
            log_level = args.logging
        else:
            log_level = "INFO"    # Available values are: DEBUG, INFO, WARNING, ERROR

        # Setup logfile
        logDir = GetConfigString("logFileDir")
        logPrefix = GetConfigString("logFilePrefix")
        logFilename = logPrefix + "_" + datetime.date.today().strftime('%Y-%m-%d') + '.log'
        FullLogFile = os.path.join(logDir, logFilename)
        logging.basicConfig(filename=FullLogFile,
                            level=log_level,
                            format='%(asctime)s: %(levelname)s --- %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info('======================= SESSION START ==========================================================')
        logging.info("\t\t\t" + getScriptName())

        # Get a start time for the entire script run process.
        time_TotalScriptRun = get_NewStart_Time()

        # Get datetime for right now
        GDB_mosaic = os.path.join(GetConfigString("GDBPath"), GetConfigString("mosaicDSName"))
        DateTimeFormat = GetConfigString("GDB_DateFormat")
        o_today_DateTime = datetime.datetime.strptime(datetime.datetime.now().strftime(DateTimeFormat), DateTimeFormat)

        # ########################################################
        # Process LATE Files
        # ########################################################
        logging.info("-----------------------------------------")
        logging.info("Processing Late Files from FTP (proxy)...")
        logging.info("-----------------------------------------")

        # Grab a timer reference
        time_LateProcess = get_NewStart_Time()

        # -----------------
        # Get date from GDB
        # -----------------
        # Get the "late" datetime values from rasters in the GDB
        o_lastLate_DateTime = GetLatest_EarlyOrLateDate_fromMosaicDataset(GDB_mosaic, "LATE")

        # Create the Late Extract folders
        lateExtractFolder = GetConfigString("extract_LateFolder")
        if not create_folder(lateExtractFolder):
            logging.error("Could not create folder: {0}. Try to create manually and run again!".format(lateExtractFolder))
            return

        # ---------------
        # Do the FTP work
        # ---------------
        # Find and process a list of "Late" 30 Min rasters available from the FTP site whose
        # timestamp is later than the last "Late" Date from the GDB
        logging.info("...between dates {0} and {1}".format(o_lastLate_DateTime.strftime('%m/%d/%Y %I:%M:%S %p'),
                                                           o_today_DateTime.strftime('%m/%d/%Y %I:%M:%S %p')))
        # bGoodSoFar = ProcessLateFiles(o_today_DateTime, o_lastLate_DateTime)
        bGoodSoFar = ProcessLateFiles_FromProxy(o_today_DateTime, o_lastLate_DateTime)
        if not bGoodSoFar:
            logging.error("General Status: ProcessLateFiles_FromProxy() returned an invalid status code.")
            return

        # ----------------------
        # Load Rasters to Mosaic
        # ----------------------
        # At this point, all "Late" raster files should be downloaded from the FTP site into the "Late" extract folder
        # and be ready to load into the mosaic dataset.
        logging.info("Loading any LATE rasters to the mosaic dataset...")
        LoadEarlyOrLateRasters(lateExtractFolder, "LATE")
        logging.info("\t=== PERFORMANCE ===>: ProcessingLateFiles took: " +
                     get_Elapsed_Time_As_String(time_LateProcess))

        # #########################################################
        # Process EARLY Files
        # #########################################################
        # Note - We only want to add any Early rasters to the GDB that are dated "later" than the last "Late" entry in
        # the GDB as it exists right now!
        logging.info("------------------------------------------")
        logging.info("Processing EARLY Files from FTP (proxy)...")
        logging.info("------------------------------------------")

        # Grab a timer reference
        time_EarlyProcess = get_NewStart_Time()

        # -----------------------------------
        # Get the latest "Late" date from GDB ... AGAIN
        # -----------------------------------
        # Note that we just got through adding new "late" rasters to the GDB, so we
        # need to grab the latest "Late" date again!!!
        o_newestLastLate_DateTime = GetLatest_EarlyOrLateDate_fromMosaicDataset(GDB_mosaic, "LATE")

        # ----------------------------------------------
        # Also, get the latest "Early" date from the GDB
        # ----------------------------------------------
        # Note that we just got through adding new "late" rasters to the GDB, so we
        # need to grab the latest "Late" date again!!!
        o_lastEarly_DateTime = GetLatest_EarlyOrLateDate_fromMosaicDataset(GDB_mosaic, "EARLY")

        # Create the Early Extract folders
        earlyExtractFolder = GetConfigString("extract_EarlyFolder")
        if not create_folder(earlyExtractFolder):
            logging.error("Could not create folder: {0}. Try to create manually and run again!".format(
                                                            earlyExtractFolder))
            return

        # ---------------
        # Do the FTP work
        # ---------------
        # Find and process a list of "Early" 30 Min rasters available from the FTP site whose
        # timestamp is later than the last "Late" Date from the GDB and has not already been processed into the GDB.
        logging.info("...between dates {0} and {1} AND that have not already been added to the GDB.".format(
                                                            o_newestLastLate_DateTime.strftime('%m/%d/%Y %I:%M:%S %p'),
                                                            o_today_DateTime.strftime('%m/%d/%Y %I:%M:%S %p')))
        # bGoodSoFar = ProcessEarlyFiles(o_newestLastLate_DateTime, o_today_DateTime, o_lastEarly_DateTime)
        bGoodSoFar = ProcessEarlyFiles_FromProxy(o_newestLastLate_DateTime, o_today_DateTime, o_lastEarly_DateTime)
        if not bGoodSoFar:
            logging.error("General Status: ProcessEarlyFiles_FromProxy() returned an invalid status code.")
            return

        # ----------------------
        # Load Rasters to Mosaic
        # ----------------------
        # At this point, all "Early" raster files should be downloaded from the FTP site into the "Early" extract folder
        # and be ready to load into the mosaic dataset.
        logging.info("Loading any EARLY rasters to the mosaic dataset...")
        LoadEarlyOrLateRasters(earlyExtractFolder, "EARLY")
        logging.info("\t=== PERFORMANCE ===>: ProcessingEarlyFiles took: " +
                     get_Elapsed_Time_As_String(time_EarlyProcess))

        # ###########################################################################
        # Remove rasters from the mosaic dataset that are older than we want to keep.
        # ###########################################################################
        # ------------------------------
        # Get rid of out of date rasters
        # ------------------------------
        logging.info("-------------------------------")
        logging.info("Removing out of date rasters...")
        logging.info("-------------------------------")

        # Grab a timer reference
        time_CleanupProcess = get_NewStart_Time()

        # Delete all raster entries older than 90 days from the FileGDB Mosaic Dataset (including their source files)
        mosaicSourceFolder = GetConfigString("final_Folder")

        # Get a before and after count of the raster mosaic records before the delete!
        initialCount = GetRasterDatasetCount(GDB_mosaic)
        deleteOutOfDateRasters(GDB_mosaic, mosaicSourceFolder)
        finalCount = GetRasterDatasetCount(GDB_mosaic)

        # Report the difference in the number of raster mosaic records!
        logging.info("Removed {0} raster entries from mosaic dataset!".format(str(initialCount - finalCount)))

        logging.info("\t=== PERFORMANCE ===>: DeleteOutOfDateRasters took: " +
                     get_Elapsed_Time_As_String(time_CleanupProcess))

        # #########################################################################
        # Perform maintenance on the file geodatabase. i.e. Calc stats and compact.
        # #########################################################################
        # -----------------------------------------------------
        # Calculate statistics and compact the file geodatabase
        # -----------------------------------------------------
        logging.info("-------------------------------------")
        logging.info("Performing geodatabase maintenance...")
        logging.info("-------------------------------------")

        # Grab a timer reference
        time_GDBMaintenanceProcess = get_NewStart_Time()

        # Do some routine maintenance on the GDB mosaic...
        # arcpy.CalculateStatistics_management(in_raster_dataset=mosaicDSPath + '/' + mosaicDS, x_skip_factor="1",
        #                                      y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE",
        #                                      area_of_interest="Feature Set")
        logging.info("Calculating statistics...")
        arcpy.CalculateStatistics_management(GDB_mosaic, "1", "1", "#", "OVERWRITE", "#")
        logging.info("Compacting file geodatabase...")
        arcpy.Compact_management(GetConfigString("GDBPath"))
        logging.info("\t=== PERFORMANCE ===>: GDB Maintenance (Calc Stats and Compact) took: " +
                     get_Elapsed_Time_As_String(time_GDBMaintenanceProcess))

        # #######################################
        # Refresh the service!
        # #######################################
        # -----------------------------------------------------
        # Calculate statistics and compact the file geodatabase
        # -----------------------------------------------------
        logging.info("-----------------------------")
        logging.info("Refreshing the WMS service...")
        logging.info("-----------------------------")

        # Grab a timer reference
        time_RefreshServiceProcess = get_NewStart_Time()

        logging.info("Refreshing the service...")

        # Note the arcpy.PublishingTools.RefreshService() call must only be available at ArcGIS 10.6 and later
        # as it doesn't seem to work at 10.4
        ### arcpy.ImportToolbox(r'C:\temp\arcgis_localhost_siteadmin_USE_THIS_ONE.ags;System/Publishing Tools')
        ### arcpy.PublishingTools.RefreshService("IMERG_30Min_ImgSvc", "ImageServer", "Test", "#")
        # ToDo... Enable this call on the server...
        # refreshService()
        logging.info("\t=== PERFORMANCE ===>: RefreshServiceProcess took: " +
                     get_Elapsed_Time_As_String(time_RefreshServiceProcess))

        # Log the Grand total script execution time...
        logging.info("------------------------------------------------------------------------------------------------")
        logging.info("=== PERFORMANCE ===>: Grand Total Processing Time was: " +
                     get_Elapsed_Time_As_String(time_TotalScriptRun))

        logging.info("======================= SESSION END ============================================================")
        # Add a few lines so we can tell sessions apart in the log more quickly
        logging.info("")
        logging.info("")
        # END

    except:
        err = capture_exception()
        logging.error(err)


# Call Main Function
main()

