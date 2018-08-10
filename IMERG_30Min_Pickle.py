import pickle

mydict = {'extract_EarlyFolder': 'E:\ETLScratch\IMERG_Extract\Early',
          'extract_LateFolder': 'E:\ETLScratch\IMERG_Extract\Late',
          'final_Folder': 'E:\SERVIR\Data\Global\IMERG',
          'logFileDir': 'E:\Code\IMERG_30Min_ETL\Log',
          'logFilePrefix': 'IMERG_30min',
          'GDBPath': 'E:/SERVIR/DATA/Global/IMERG_SR3857.gdb',
          'mosaicDSName': 'IMERG',
          'DaysToKeepRasters': '90',
          'rasterTimeProperty': 'timestamp',
          'rasterStartTimeProperty': 'start_datetime',
          'rasterEndTimeProperty': 'end_datetime',
          'rasterDataAgeProperty': 'Data_Age',
          'RegEx_StartDateFilterString': '\d{4}[01]\d[0-3]\d-S[0-2]\d{5}',
          'GDB_DateFormat': '%Y%m%d%H%M',
          'Filename_StartDateFormat': '%Y%m%d-S%H%M%S',
          'ftp_host': 'jsimpson.pps.eosdis.nasa.gov',
          'ftp_user': 'SOMEVALUE',
          'ftp_pswrd': 'SOMEVALUE',
          'ftp_baseLateFolder': '/data/imerg/gis',
          'ftp_baseEarlyFolder': '/data/imerg/gis/early',
          'svc_adminURL': 'https://gis1.servirglobal.net/arcgis/admin',
          'svc_username': 'SOMEVALUE',
          'svc_password': 'SOMEVALUE',
          'svc_folder': 'Test',
          'svc_Name': 'IMERG_30Min_ImgSvc',
          'svc_Type': 'ImageServer'}

output = open('config.pkl', 'wb')
pickle.dump(mydict, output)
output.close()
