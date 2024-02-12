from zipfile import ZipFile
from xml.etree import ElementTree
from datetime import date
from util.date_range_parser import DataRangeParser
from util.work_timer import WorkTimer
from util.dict_util import DictUtil
from queue import Queue
from threading import Thread

import numpy as np
import os
import csv
import time
import json
import sys

class TenderParser():
    def __init__(self) -> None:
        self.work_timer = WorkTimer()
        self.data_range_parser = DataRangeParser()
        self.ignore_tag = ["signature", "printForm", "printFormInfo", "cryptoSigns"]
        self.objects = []
        self.okpd2_class_target = ['02', '03', '63']
        self.types = ['fcsNotificationEP', 'fcsNotificationEF', 'fcsNotificationZK', 'fcsNotificationOK', 'fcsNotificationISM', 'fcsNotificationOKOU', 'fcsNotificationZP', 'fcsNotificationZakK', 'fcsNotificationZakA', 'fcsNotificationPO', 'fcsNotificationOKD']

    def parse_json(self, directory: str) -> None:
        path = os.fsencode(directory)
        file_count = len(os.listdir(path))
        file_index = 0
        name = "data_1.json"

        with open(name, "w", encoding="utf-8") as file:
            file.write('[')
            file.close()

        for file in os.listdir(path):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                file_index += 1

                if file_index >= 1 and file_index <= 1587:

                    self.work_timer.start()

                    with ZipFile(directory + filename, "r") as myzip:
                        file_zip_count = len(myzip.namelist())
                        file_zip_index = 0

                        for nameFileXml in myzip.namelist():
                            file_zip_index += 1

                            if nameFileXml.endswith(".xml"):
                                str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                                root = ElementTree.fromstring(str_data_xml)

                                if len(root) == 0: continue

                                try:
                                    marked_dict = {}
                                    type_purchase = root[0].tag.split('}')[1]

                                    number = ""
                                    if type_purchase in self.types:
                                        # Номер тендера
                                        for purchaseNumber in root.iter('{http://zakupki.gov.ru/oos/types/1}purchaseNumber'):
                                            number = purchaseNumber.text.strip()
                                        
                                        # Дата публикации
                                        docPublishDate = root[0].find('{http://zakupki.gov.ru/oos/types/1}docPublishDate').text.strip()

                                        # Ссылка на тендер
                                        href = root[0].find('{http://zakupki.gov.ru/oos/types/1}href').text.strip()

                                        # Объект закупки
                                        purchaseObjectInfo = root[0].find('{http://zakupki.gov.ru/oos/types/1}purchaseObjectInfo').text.strip()

                                        # Сумма тендера
                                        maxPrice = 0.0
                                        # Волюта
                                        currency = None
                                        for lot in root.iter('{http://zakupki.gov.ru/oos/types/1}lot'):
                                            lotMaxPrice = lot.find('{http://zakupki.gov.ru/oos/types/1}maxPrice')
                                            lotСurrency = lot.find('{http://zakupki.gov.ru/oos/types/1}currency')
                                            if lotMaxPrice != None:
                                                maxPrice += float(lotMaxPrice.text)
                                                currency = lotСurrency[0].text.strip()

                                        # Дата старта процедуры
                                        procedureStartDate = None
                                        # Дата окончания процедуры
                                        procedureEndDate = None
                                        procedureInfo = root[0].find('{http://zakupki.gov.ru/oos/types/1}procedureInfo')
                                        if procedureInfo != None:
                                            collecting = procedureInfo.find('{http://zakupki.gov.ru/oos/types/1}collecting')
                                            if collecting != None:
                                                start = collecting.find('{http://zakupki.gov.ru/oos/types/1}startDate')
                                                end = collecting.find('{http://zakupki.gov.ru/oos/types/1}endDate')
                                                if start != None:
                                                    procedureStartDate = start.text
                                                if end != None:
                                                    procedureEndDate = end.text

                                        # Номер тендера
                                        number = root[0].find('{http://zakupki.gov.ru/oos/types/1}purchaseNumber').text.strip()
                                        # number = href.split('=')[-1]
                                        
                                        # Сбор массива данных по объектам закупок
                                        okpd2s = np.array([])
                                        for purchaseObject in root.iter('{http://zakupki.gov.ru/oos/types/1}purchaseObject'):
                                            amount = purchaseObject.find('.//{http://zakupki.gov.ru/oos/types/1}sum')
                                            okpd2 = purchaseObject[0][0].text
                                            if amount != None:
                                                okpd2s = np.append(okpd2s, {'OKPD2': okpd2, 'sum': amount.text})
                                            else:
                                                okpd2s = np.append(okpd2s, {'OKPD2': okpd2, 'sum': None})
                                        
                                        # Сумма по классу объекта закупки
                                        okpd2_cost_class = 0.0
                                        # Класс объекта закупки по ОКПД2
                                        okpd2_code_class = ""
                                        isTarget = False
                                        for x in okpd2s:
                                            if x['sum'] != None:
                                                if x['OKPD2'].split('.')[0] in self.okpd2_class_target:
                                                    isTarget = True
                                                    okpd2_cost_class = okpd2_cost_class + float(x['sum'])
                                                    okpd2_code_class = x['OKPD2']

                                        if isTarget:
                                            marked_dict = {
                                                'number': number,
                                                'typePurchase': type_purchase,
                                                'status': "Определение поставщика завершено",
                                                'docPublishDate': docPublishDate,
                                                'purchaseObjectInfo': purchaseObjectInfo,
                                                'maxPrice': maxPrice,
                                                'currency': currency,
                                                'okpd2CodeClass': okpd2_code_class,
                                                'okpd2CostClass': okpd2_cost_class,
                                                'procedureStartDate': procedureStartDate,
                                                'procedureEndDate': procedureEndDate,
                                            }
                                            # self.objects.append(marked_dict)
                                            with open(name, "a", encoding="utf-8") as file:
                                                json.dump(marked_dict, file)
                                                file.write(',')
                                                file.close()
                                    elif type_purchase in ['fcsNotificationCancel', 'fcsNotificationCancelFailure', 'fcsNotificationLotCancel']:
                                        # Ссылка на тендер
                                        href = root[0].find('{http://zakupki.gov.ru/oos/types/1}href').text.strip()
                                        # Номер тендера
                                        # number = href.split('=')[-1]
                                        number = root[0].find('{http://zakupki.gov.ru/oos/types/1}purchaseNumber').text.strip()

                                        # Сбор массива данных по объектам закупок
                                        isTarget = False
                                        okpd2s = np.array([])
                                        for purchaseObject in root.iter('{http://zakupki.gov.ru/oos/types/1}purchaseObject'):
                                            okpd2 = purchaseObject[0][0].text
                                            okpd2s = np.append(okpd2s, okpd2)

                                        for x in okpd2s:
                                            if x['OKPD2'].split('.')[0] in self.okpd2_class_target:
                                                isTarget = True

                                        # Дата публикации
                                        docPublishDate = root[0].find('{http://zakupki.gov.ru/oos/types/1}docPublishDate').text.strip()

                                        # Причина закрытия
                                        cancelReason = root[0].find('{http://zakupki.gov.ru/oos/types/1}cancelReason')
                                        responsibleDecision = cancelReason.find('{http://zakupki.gov.ru/oos/types/1}responsibleDecision')
                                        decisionDate = responsibleDecision.find('{http://zakupki.gov.ru/oos/types/1}decisionDate').text.strip()

                                        if isTarget:
                                            marked_dict = {
                                                'number': number,
                                                'typePurchase': type_purchase,
                                                'status': "Определение поставщика отменено",
                                                'docPublishDate': docPublishDate,
                                                'decisionDate': decisionDate
                                            }

                                            with open(name, "a", encoding="utf-8") as file:
                                                json.dump(marked_dict, file)
                                                file.write(',')
                                                file.close()
                                except Exception as err:
                                    print(f"Unexpected {err=}, {type(err)=}")
                                    continue

                            print('\033[F\033[K', end='')
                            print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')

                    self.work_timer.calculate_time(file_index, file_count)

        with open(name, "rb+") as file:
            file.seek(-1, 2)
            file.truncate()
            file.close()

        with open(name, "r+", encoding="utf-8") as file:
            file.seek(0, 2)
            file.write(']')
            file.close()

    # def parse_to_xml(self, element):
    #     # Инициализация пустого словаря для текущего элемента
    #     isArray = False
    #     if element.tag.split('}')[1][-1].lower() == 's':
    #         current_dict = []
    #         isArray = True
    #     else:
    #         current_dict = {}

    #     # # Перебор атрибутов текущего элемента
    #     # for key, value in element.attrib.items():
    #     #     print(value)
    #     #     current_dict[key] = value

    #     # Рекурсивный перебор дочерних элементов
    #     for child in element:
    #         if child.tag.split('}')[1] not in self.ignore_tag:
    #             # Рекурсивный вызов функции для каждого дочернего элемента
    #             child_dict = self.parse_to_xml(child)

    #             if isArray:
    #                 current_dict.append({child.tag.split('}')[1]: str(child_dict)})
    #             else:
    #                 # Объединение словарей текущего элемента и его дочерних элементов
    #                 current_dict.setdefault(child.tag.split('}')[1], child_dict)

    #     # Если у текущего элемента нет дочерних элементов, сохраняем его текст
    #     if not current_dict:
    #         current_dict = str(element.text)

    #     return current_dict
    
    # def array_to_string_dict(self, dictX, result = {}):
    #     for key, value in dictX.items():
    #         if isinstance(value, dict):
    #             result[key] = self.array_to_string_dict(dictX)
    #         elif isinstance(value, list):
    #             result[key] = str(value)
    #         else:
    #             result[key] = value

    #     return result

    # def get_names(self, directory: str) -> None:
    #     path = os.fsencode(directory)
    #     file_count = len(os.listdir(path))
    #     file_index = 0

    #     old_dictionary = {}

    #     for file in os.listdir(path):
    #         filename = os.fsdecode(file)

    #         if filename.endswith(".zip"):
    #             file_index += 1

    #             self.work_timer.start()

    #             with ZipFile(directory + filename, "r") as myzip:
    #                 file_zip_count = len(myzip.namelist())
    #                 file_zip_index = 0

    #                 for nameFileXml in myzip.namelist():
    #                     startT = time.time()
    #                     file_zip_index += 1

    #                     if nameFileXml.endswith(".xml"):
    #                         str_data_xml = myzip.read(nameFileXml).decode('utf-8')
    #                         root = ElementTree.fromstring(str_data_xml)

    #                         try:
    #                             new_dictionary = self.parse_to_xml(root[0])
    #                             old_dictionary = DictUtil.merging_dictionaries(new_dictionary, old_dictionary)
    #                         except Exception as err:
    #                             print(f"Unexpected {err=}, {type(err)=}")
    #                             continue
    #                         # single_level_dictionary = DictUtil.get_names_dict(merging_dictionaries)

    #                         # for k in single_level_dictionary:
    #                         #     if k not in names_list:
    #                         #         names_list.append(k)

    #                     finish = time.time()
    #                     print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд\t|\t{finish - startT} s')

    #             self.work_timer.calculate_time(file_index, file_count)
    #     print(old_dictionary)

    # def parse(self, directory: str, fieldnames: list) -> None:
    #     path = os.fsencode(directory)
    #     file_count = len(os.listdir(path))
    #     file_index = 0

    #     sss = {}

    #     with open('names.csv', 'w', newline='', encoding='utf-8') as csvfile:
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')# extrasaction="ignore", quoting=csv.QUOTE_NONNUMERIC
    #         writer.writeheader()
    #         csvfile.close()

    #     for file in os.listdir(path):
    #         filename = os.fsdecode(file)

    #         if filename.endswith(".zip"):
    #             file_index += 1

    #             self.work_timer.start()

    #             # date_range = self.data_range_parser.parse(filename)
    #             # zip_date_start = date_range.start.strftime('%d.%m.%Y')
    #             # zip_date_end = date_range.end.strftime('%d.%m.%Y')
    #             # period = ""
    #             # if date_range.start >= date(2014, 1, 1) and date_range.end <= date(2016, 1, 1):
    #             #     period = "01.01.2014-01.01.2016"
    #             # elif date_range.start >= date(2016, 1, 1) and date_range.end <= date(2020, 1, 1):
    #             #     period = "01.01.2016-01.01.2020"
    #             # elif date_range.start >= date(2020, 1, 1) and date_range.end <= date(9999, 1, 1):
    #             #     period = "01.01.2020-Настоящее время"

    #             with ZipFile(directory + filename, "r") as myzip:
    #                 file_zip_count = len(myzip.namelist())
    #                 file_zip_index = 0

    #                 for nameFileXml in myzip.namelist():
    #                     file_zip_index += 1

    #                     if nameFileXml.endswith(".xml"):
    #                         str_data_xml = myzip.read(nameFileXml).decode('utf-8')
    #                         root = ElementTree.fromstring(str_data_xml)

    #                         try:
    #                             type_purchase = root[0]
    #                             for item in type_purchase:
    #                                 if item.tag.split('}')[1] == "purchaseNumber":
    #                                     if item.text.strip() in sss:
    #                                         # print(item.text.strip())
    #                                         sss[item.text.strip()] = sss[item.text.strip()] + 1
    #                                     else:
    #                                         sss[item.text.strip()] = 1
    #                         except Exception as err:
    #                             continue
    #                         # try:
    #                         #     type_purchase = root[0].tag.split('}')[1]

    #                         #     result_dict = self.parse_to_xml(root[0])
    #                         #     marked_dict = DictUtil.dictionary_compression(result_dict)

    #                         #     marked_dict["typePurchase"] = type_purchase
    #                         #     marked_dict["period"] = period
    #                         #     marked_dict["zipDateStart"] = zip_date_start
    #                         #     marked_dict["zipDateEnd"] = zip_date_end
    #                         #     with open('names.csv', 'a', newline='', encoding='utf-8') as csvfile:
    #                         #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', extrasaction="ignore")
    #                         #         writer.writerow(marked_dict)
    #                         #         csvfile.close()
    #                         # except Exception as err:
    #                         #     # print(f"Unexpected {err=}, {type(err)=}")
    #                         #     continue

    #                     print('\033[F\033[K', end='')
    #                     print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')

    #             self.work_timer.calculate_time(file_index, file_count)
    #     # print(sss)
    #     with open('data.txt', 'w') as outfile:
    #         json.dump(sss, outfile, indent=4)

    # def chunks(self, lst, n):
    #     """Yield successive n-sized chunks from lst."""
    #     for i in range(0, len(lst), n):
    #         yield lst[i:i + n]

    # def parse_json(self, directory: str) -> None:
    #     path = os.fsencode(directory)
    #     file_count = len(os.listdir(path))
    #     file_index = 0
    #     name = "data_1.json"

    #     with open(name, "w", encoding="utf-8") as file:
    #         file.write('[')
    #         file.close()

    #     for file in os.listdir(path):
    #         filename = os.fsdecode(file)

    #         if filename.endswith(".zip"):
    #             file_index += 1

    #             if file_index >= 1 and file_index <= 35:

    #                 self.work_timer.start()

    #                 date_range = self.data_range_parser.parse(filename)
    #                 zip_date_start = date_range.start.strftime('%d.%m.%Y')
    #                 zip_date_end = date_range.end.strftime('%d.%m.%Y')
    #                 period = ""
    #                 if date_range.start >= date(2014, 1, 1) and date_range.end <= date(2016, 1, 1):
    #                     period = "01.01.2014-01.01.2016"
    #                 elif date_range.start >= date(2016, 1, 1) and date_range.end <= date(2020, 1, 1):
    #                     period = "01.01.2016-01.01.2020"
    #                 elif date_range.start >= date(2020, 1, 1) and date_range.end <= date(9999, 1, 1):
    #                     period = "01.01.2020-Настоящее время"

    #                 with ZipFile(directory + filename, "r") as myzip:
    #                     file_zip_count = len(myzip.namelist())
    #                     file_zip_index = 0

    #                     for nameFileXml in myzip.namelist():
    #                         file_zip_index += 1

    #                         if nameFileXml.endswith(".xml"):
    #                             str_data_xml = myzip.read(nameFileXml).decode('utf-8')
    #                             root = ElementTree.fromstring(str_data_xml)

    #                             try:
    #                                 type_purchase = root[0].tag.split('}')[1]

    #                                 # if type_purchase in type_purchase_target:
    #                                 result_dict = self.parse_to_xml(root[0])
    #                                 marked_dict = DictUtil.dictionary_compression(result_dict)

    #                                 marked_dict["typePurchase"] = type_purchase
    #                                 marked_dict["period"] = period
    #                                 marked_dict["zipDateStart"] = zip_date_start
    #                                 marked_dict["zipDateEnd"] = zip_date_end
                                    
    #                                 marked_dict["purchaseNumber"] = marked_dict["href"].split('=')[-1]
                                    
    #                                 marked_array_str_dict = self.array_to_string_dict(marked_dict)
    #                                 with open(name, "a", encoding="utf-8") as file:
    #                                     json.dump(marked_array_str_dict, file)
    #                                     file.write(',')
    #                                     file.close()
    #                             except Exception as err:
    #                                 print(f"Unexpected {err=}, {type(err)=}")
    #                                 continue

    #                         print('\033[F\033[K', end='')
    #                         print(f'Просмотренно zip: {file_index}/{file_count}\t{round(file_index/file_count*100, 2)}%\t|\t{file_zip_index}/{file_zip_count}\t{round(file_zip_index/file_zip_count*100, 2)}%\t|\tВремя скачивания {self.work_timer.days} дней {self.work_timer.hours} часов {self.work_timer.minutes} минут {self.work_timer.seconds} секунд')

    #                 self.work_timer.calculate_time(file_index, file_count)
        
    #     with open(name, "rb+") as file:
    #         file.seek(-1, 2)
    #         file.truncate()
    #         file.close()

    #     with open(name, "r+", encoding="utf-8") as file:
    #         file.seek(0, 2)
    #         file.write(']')
    #         file.close()