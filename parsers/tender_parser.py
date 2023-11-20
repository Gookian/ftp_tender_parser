from zipfile import ZipFile
from xml.etree import ElementTree
from datetime import date
from util.date_range_parser import DataRangeParser
from util.work_timer import WorkTimer
from util.dict_util import DictUtil

import os
import csv
import time

class TenderParser():
    def __init__(self) -> None:
        self.work_timer = WorkTimer()
        self.data_range_parser = DataRangeParser()
        self.ignore_tag = ["signature", "printForm", "printFormInfo", "cryptoSigns"]

    def parse_to_xml(self, element):
        # Инициализация пустого словаря для текущего элемента
        isArray = False
        if element.tag.split('}')[1][-1].lower() == 's':
            current_dict = []
            isArray = True
        else:
            current_dict = {}

        # # Перебор атрибутов текущего элемента
        # for key, value in element.attrib.items():
        #     print(value)
        #     current_dict[key] = value

        # Рекурсивный перебор дочерних элементов
        for child in element:
            if child.tag.split('}')[1] not in self.ignore_tag:
                # Рекурсивный вызов функции для каждого дочернего элемента
                child_dict = self.parse_to_xml(child)

                if isArray:
                    current_dict.append({child.tag.split('}')[1]: child_dict})
                else:
                    # Объединение словарей текущего элемента и его дочерних элементов
                    current_dict.setdefault(child.tag.split('}')[1], child_dict)

        # Если у текущего элемента нет дочерних элементов, сохраняем его текст
        if not current_dict:
            current_dict = str(element.text)

        return current_dict

    def get_names(self, directory: str) -> None:
        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0

        old_dictionary = {}

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                self.work_timer.start()

                with ZipFile(directory + filename, "r") as myzip:
                    file_zip_count = len(myzip.namelist())
                    file_zip_index = 0

                    for nameFileXml in myzip.namelist():
                        startT = time.time()
                        file_zip_index += 1

                        if nameFileXml.endswith(".xml"):
                            str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                            root = ElementTree.fromstring(str_data_xml)

                            try:
                                new_dictionary = self.parse_to_xml(root[0])
                                old_dictionary = DictUtil.merging_dictionaries(new_dictionary, old_dictionary)
                            except Exception as err:
                                print(f"Unexpected {err=}, {type(err)=}")
                                continue
                            # single_level_dictionary = DictUtil.get_names_dict(merging_dictionaries)

                            # for k in single_level_dictionary:
                            #     if k not in names_list:
                            #         names_list.append(k)

                        finish = time.time()
                        print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд\t|\t{finish - startT} s')

                self.work_timer.calculate_time(file_index, file_count)
        print(old_dictionary)

    def parse(self, directory: str, fieldnames: list) -> None:
        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0

        with open('names.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')# extrasaction="ignore", quoting=csv.QUOTE_NONNUMERIC
            writer.writeheader()
            csvfile.close()

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                self.work_timer.start()

                date_range = self.data_range_parser.parse(filename)
                zip_date_start = date_range.start.strftime('%d.%m.%Y')
                zip_date_end = date_range.end.strftime('%d.%m.%Y')
                period = ""
                if date_range.start >= date(2014, 1, 1) and date_range.end <= date(2016, 1, 1):
                    period = "01.01.2014-01.01.2016"
                elif date_range.start >= date(2016, 1, 1) and date_range.end <= date(2020, 1, 1):
                    period = "01.01.2016-01.01.2020"
                elif date_range.start >= date(2020, 1, 1) and date_range.end <= date(9999, 1, 1):
                    period = "01.01.2020-Настоящее время"

                with ZipFile(directory + filename, "r") as myzip:
                    file_zip_count = len(myzip.namelist())
                    file_zip_index = 0

                    for nameFileXml in myzip.namelist():
                        file_zip_index += 1

                        if nameFileXml.endswith(".xml"):
                            str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                            root = ElementTree.fromstring(str_data_xml)

                            try:
                                type_purchase = root[0].tag.split('}')[1]

                                result_dict = self.parse_to_xml(root[0])
                                marked_dict = DictUtil.dictionary_compression(result_dict)

                                marked_dict["typePurchase"] = type_purchase
                                marked_dict["period"] = period
                                marked_dict["zipDateStart"] = zip_date_start
                                marked_dict["zipDateEnd"] = zip_date_end
                                with open('names.csv', 'a', newline='', encoding='utf-8') as csvfile:
                                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', extrasaction="ignore")
                                    writer.writerow(marked_dict)
                                    csvfile.close()
                            except Exception as err:
                                print(f"Unexpected {err=}, {type(err)=}")
                                continue

                        print('\033[F\033[K', end='')
                        print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')

                self.work_timer.calculate_time(file_index, file_count)