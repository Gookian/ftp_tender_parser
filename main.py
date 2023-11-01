#from datetime import date
from ftp_file_downloader import FtpFileDownloader
from tender_reader import TenderReader
from datetime import date

import ftplib

ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
ftp.login(user='free', passwd='free')

ftpDownloader = FtpFileDownloader(ftp, threadsCount=6)

source="/fcs_regions/Novosibirskaja_obl/notifications/"
directory_str="./regions/Novosibirskaja_obl/"
#ftpDownloader.downloadFiles(source, directory_str, date(2016, 1, 1), date(2023,10,24))

# Чтение файлов Xml
TenderReader.readFiles(directory_str)