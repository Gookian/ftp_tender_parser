from ftp.ftp_downloader import FtpDownloader
from parsers.tender_parser import TenderParser
from datetime import date
from parsers.structure_tender_parser import StructureTenderParser
from repositories.repository import Repository
from util.dict_util import DictUtil
from data_name_column import result_compress_dict

import pandas
import ftplib
import numpy as np

source="/fcs_regions/Novosibirskaja_obl/notifications/"
directory_str="./regions/Novosibirskaja_obl/"

while True:
    print("Доступные команды: \n1) download\n2) parse\n3) parseStructure\n4) getJson\n5) parseJson\n6) exit")
    command_str = input("Введите комманду: ").lower()

    if command_str == "download":
        ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
        ftp.login(user='free', passwd='free')

        ftp_downloader = FtpDownloader(ftp, threadsCount=6)
        ftp_downloader.download_zip(source, directory_str, date(2016, 1, 1), date(2023,11,15))

    elif command_str == "parsejson":
        tender_parser = TenderParser()
        tender_parser.parse_json(directory_str)

    elif command_str == "parse":
        names_dict = DictUtil.dictionary_compression(result_compress_dict)
        names_array = np.array([], dtype=str)
        for key, value in names_dict.items():
            names_array = np.append(names_array, key)

        names_array = np.insert(names_array, 0, "typePurchase")
        names_array = np.insert(names_array, 0, "period")
        names_array = np.insert(names_array, 0, "zipDateStart")
        names_array = np.insert(names_array, 0, "zipDateEnd")

        print(names_array)

        print("Число признаков:", len(names_array), "\n")

        tender_parser = TenderParser()
        tender_parser.parse(directory_str, fieldnames=names_array)

    elif command_str == "parsename":
        tender_parser = TenderParser()
        tender_parser.get_names(directory_str)

    elif command_str == "parsestructure":
        structure_tender_parser = StructureTenderParser()
        structure_tender_parser.parse(directory_str)

    elif command_str == "getjson":
        repository = Repository()
        repository.save_to_json()

    elif command_str == "jsontodf":
        df = pandas.read_json('data.json', encoding="utf-8")
        print(df)

    elif command_str == "exit":
        break

    else:
        print("Неизвестная каманда!")