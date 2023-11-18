from ftp.ftp_downloader import FtpDownloader
from parsers.tender_parser import TenderParser
from datetime import date
from parsers.structure_tender_parser import StructureTenderParser
from repositories.repository import Repository

import ftplib

source="/fcs_regions/Novosibirskaja_obl/notifications/"
directory_str="./regions/Novosibirskaja_obl/"

while True:
    print("Доступные команды: \n1) download\n2) parse\n3) parseStructure\n4) getJson\n5) exit")
    command_str = input("Введите комманду: ").lower()

    if command_str == "download":
        ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
        ftp.login(user='free', passwd='free')

        ftp_downloader = FtpDownloader(ftp, threadsCount=6)
        ftp_downloader.download_zip(source, directory_str, date(2016, 1, 1), date(2023,11,15))

    elif command_str == "parse":
        tender_parser = TenderParser()
        tender_parser.readFiles(directory_str)

    elif command_str == "parsestructure":
        structure_tender_parser = StructureTenderParser()
        structure_tender_parser.parse(directory_str)

    elif command_str == "getjson":
        repository = Repository()
        repository.save_to_json()

    elif command_str == "exit":
        break

    else:
        print("Неизвестная каманда!")