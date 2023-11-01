from zipfile import ZipFile
from xml.etree import ElementTree
from pymongo import MongoClient

import os
import time

class TenderReader():
    @staticmethod
    def readFiles(directory_str):
        client = MongoClient("localhost", 27017, maxPoolSize=50)
        db = client.gpo_tender_db
        collection = db.tenders

        directory = os.fsencode(directory_str)
        fileCount = len(os.listdir(directory))
        fileIndex = 0
        secondsTotal = 0
        days = 0
        hours = 0
        minutes = 0
        seconds = 0

        for file in os.listdir(directory):
            start_time = time.time()
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):
                fileIndex += 1

                with ZipFile(directory_str + filename, "r") as myzip:
                    fileZipCount = len(myzip.namelist())
                    fileZipIndex = 0

                    for nameFileXml in myzip.namelist():
                        fileZipIndex += 1

                        if nameFileXml.endswith(".xml"):
                            str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                            root = ElementTree.fromstring(str_data_xml)

                            tender_json = {
                                "region": "Новосибирская область",
                                "purchaseNumber": None,
                                "purchaseNumber": None,
                                "docPublishDate": None,
                                "purchaseObjectInfo": None,
                                "purchaseResponsible": {
                                    "responsibleOrg": {
                                        "regNum": None,
                                        "fullName": None,
                                        "postAddress": None,
                                        "factAddress": None,
                                        "INN": None,
                                        "KPP": None,
                                    },
                                    "responsibleRole": None
                                },
                                "placingWay": {
                                    "code": None,
                                    "name": None
                                },
                                "lots": None
                            }

                            for element in root:
                                for element1 in element:
                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}id"):
                                        tender_json["_id"] = int(element1.text)

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}purchaseNumber"):
                                        tender_json["purchaseNumber"] = element1.text

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}docPublishDate"):
                                        tender_json["docPublishDate"] = element1.text

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}purchaseObjectInfo"):
                                        tender_json["purchaseObjectInfo"] = element1.text

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}purchaseResponsible"):
                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}regNum"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["regNum"] = s.text
                                            break

                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}fullName"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["fullName"] = s.text
                                            break
                                            
                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}postAddress"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["postAddress"] = s.text
                                            break

                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}factAddress"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["factAddress"] = s.text
                                            break

                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}INN"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["INN"] = int(s.text)
                                            break

                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}KPP"):
                                            tender_json["purchaseResponsible"]["responsibleOrg"]["KPP"] = int(s.text)
                                            break

                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}responsibleRole"):
                                            tender_json["purchaseResponsible"]["responsibleRole"] = s.text
                                            break

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}placingWay"):
                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}code"):
                                            tender_json["placingWay"]["code"] = s.text
                                            break
                                        for s in element1.iter("{http://zakupki.gov.ru/oos/types/1}name"):
                                            tender_json["placingWay"]["name"] = s.text
                                            break

                                    if (element1.tag == "{http://zakupki.gov.ru/oos/types/1}lots"):
                                        lots_json = []
                                        for lotElement in element1.iter("{http://zakupki.gov.ru/oos/types/1}lot"):
                                            lot_json = {
                                                "lotNumber": None,
                                                "maxPrice": None,
                                                "OKPD2": {
                                                    "code": None,
                                                    "name": None
                                                }
                                            }

                                            for s in lotElement.iter("{http://zakupki.gov.ru/oos/types/1}lotNumber"):
                                                lot_json["lotNumber"] = int(s.text)
                                                break

                                            for s in lotElement.iter("{http://zakupki.gov.ru/oos/types/1}maxPrice"):
                                                lot_json["maxPrice"] = float(s.text)
                                                break

                                            for okpdElement in lotElement.iter("{http://zakupki.gov.ru/oos/types/1}OKPD2"):
                                                for s in okpdElement.iter("{http://zakupki.gov.ru/oos/types/1}code"):
                                                    lot_json["OKPD2"]["code"] = s.text
                                                    break

                                                for s in okpdElement.iter("{http://zakupki.gov.ru/oos/types/1}name"):
                                                    lot_json["OKPD2"]["name"] = s.text
                                                    break
                                                break

                                            lots_json.append(lot_json)
                                        tender_json["lots"] = lots_json
                            try:
                                collection.insert_one(tender_json)
                            except:
                                print(tender_json)
                        print('\033[F\033[K', end='')
                        print(f'Просмотренно zip: {fileIndex}/{fileCount}\t{round(fileIndex/fileCount*100, 2)}%\t|\t{fileZipIndex}/{fileZipCount}\t{round(fileZipIndex/fileZipCount*100, 2)}%\t|\tВремя скачивания {days} дней {hours} часов {minutes} минут {seconds} секунд')

                secondsDelta = (time.time() - start_time)
                secondsTotal += secondsDelta
                secondsMean = secondsTotal / fileIndex * fileCount
                days = round(secondsMean / 86400)
                hours = round((secondsMean - days * 86400) / 3600)
                minutes = round((secondsMean - days * 86400 - hours * 3600) / 60)
                seconds = round((secondsMean - days * 86400 - hours * 3600 - minutes * 60))
                if (hours < 0):
                    hours = 0
                if (minutes < 0):
                    minutes = 0
                if (seconds < 0):
                    seconds = 0
                continue
            else:
                continue
