from zipfile import ZipFile
from xml.etree import ElementTree
from pymongo import MongoClient
from datetime import date
from util.date_range_parser import DataRangeParser
from util.work_timer import WorkTimer

import os

class TenderParser():
    def __init__(self) -> None:
        self.data_range_parser = DataRangeParser()
        self.work_timer = WorkTimer()

        self.structure_2014_2016_list = self.init_structure_data(date(2014, 1, 1), date(2017, 1, 1))
        self.structure_2016_2020_list = self.init_structure_data(date(2017, 1, 1), date(2021, 1, 1))
        self.structure_2020_now_list = self.init_structure_data(date(2021, 1, 1), date(2023, 11, 15))

    def init_structure_data(self, to_date: date, from_date: date) -> None:
        client = MongoClient("localhost", 27017, maxPoolSize=50)
        db = client.gpo_tender_db
        collection = db.structures
        
        query = { 
            "toDate": to_date.strftime('%d.%m.%Y'),
            "fromDate": from_date.strftime('%d.%m.%Y')
        }

        result = collection.find(query)

        structure_list = []

        for structure in result:
            structure_list.append(structure)

        return structure_list

    def readFiles(self, directory: str):
        client = MongoClient("localhost", 27017, maxPoolSize=50)
        db = client.gpo_tender_db
        collection = db.notifications

        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0

        with open("notifications.json", "w", encoding="utf-8") as file:
            file.write('[')
            file.close()

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                self.work_timer.start()
                date_range = self.data_range_parser.parse(filename)
                # if date_range.start >= date(2014, 1, 1) and date_range.end <= date(2017, 1, 1):

                with ZipFile(directory + filename, "r") as myzip:
                    file_zip_count = len(myzip.namelist())
                    file_zip_index = 0

                    for nameFileXml in myzip.namelist():
                        file_zip_index += 1

                        if nameFileXml.endswith(".xml"):
                            str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                            root = ElementTree.fromstring(str_data_xml)
                            
                            result_dict = xmltodict.parse(str_data_xml)

                            # Сохранения словаря
                            collection.insert_one(result_dict)

                        print('\033[F\033[K', end='')
                        print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')

                self.work_timer.calculate_time(file_index, file_count)
                continue
            else:
                continue