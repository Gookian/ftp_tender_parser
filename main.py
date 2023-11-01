from ftp_file_downloader import FtpFileDownloader
from tender_reader import TenderReader
from datetime import date

import ftplib

source="/fcs_regions/Novosibirskaja_obl/notifications/"
directory_str="./regions/Novosibirskaja_obl/"

while True:
    print("Доступные команды: \n1) download\n2) read\n3) exit")
    commandStr = input("Введите комманду: ").lower()

    if commandStr == "download":
        ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
        ftp.login(user='free', passwd='free')

        ftpDownloader = FtpFileDownloader(ftp, threadsCount=6)
        ftpDownloader.downloadFiles(source, directory_str, date(2016, 1, 1), date(2023,10,24))

    elif commandStr == "read":
        TenderReader.readFiles(directory_str)

    elif commandStr == "exit":
        break

    else:
        print("Неизвестная каманда!")