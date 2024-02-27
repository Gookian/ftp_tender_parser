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