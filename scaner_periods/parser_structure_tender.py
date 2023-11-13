from zipfile import ZipFile
from xml.etree import ElementTree
from date_range_parser import DataRangeParser
from datetime import date

import os
import time

type_structure = {}

class StructureTenderParser():
    def parse(self, directory_str):
        dataRangeParser = DataRangeParser()
        
        directory = os.fsencode(directory_str)
        fileIndex = 0

        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            fileCount = len(os.listdir(directory))

            if filename.endswith(".zip"):
                fileIndex += 1

                date_range = dataRangeParser.parse(filename)

                # Смотрим промежуток с начала 2014 года по конец 2016 года
                if date_range.start >= date(2014, 1, 1) and date_range.end <= date(2017, 1, 1):
                    with ZipFile(directory_str + filename, "r") as myzip:
                        fileZipIndex = 0

                        for nameFileXml in myzip.namelist():
                            fileZipIndex += 1

                            if nameFileXml.endswith(".xml"):
                                str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                                root = ElementTree.fromstring(str_data_xml)

                                type_notification = ""
                                index = 0
                                for element in root.iter():
                                    if (index == 1):
                                        type_tender = element.tag.split('}')[1]

                                        if (type_tender not in type_structure):
                                            type_structure[type_tender] = list()
                                            self.tagCounting(element, type_tender)
                                            
                                        else:
                                            self.tagCounting(element, type_tender)

                                    index += 1
                    print(f'{fileIndex / fileCount * 100} %')
                continue
            else:
                continue
            
        for key, values in type_structure.items():
            fileCountCheck = 0
            index = 0
            typeField = ""
            print(key)
            for idx, value in enumerate(values):
                for tag, valueTag in value.items():
                    if (tag == "count"):
                        if (index == 0):
                            fileCountCheck = valueTag
                        else:
                            if (valueTag % fileCountCheck == 0): # Если кратно числу файлов, то обязательное или используется n раз в файле
                                typeField = "reqired"
                            elif (valueTag >= 15 and valueTag < fileCountCheck or valueTag > fileCountCheck):
                                typeField = "optional"
                            elif (valueTag < 15):
                                typeField = "unifid"
                        index += 1
                    print(typeField)
                    

            print("\n")
        #print(type_structure)

    def updateCount(self, type_tender, tag):
        for idx, item in enumerate(type_structure[type_tender]):
            if tag in item:
                type_structure[type_tender][idx]["count"] = type_structure[type_tender][idx]["count"] + 1

    def existTag(self, type_tender, tag):
        for idx, item in enumerate(type_structure[type_tender]):
            if tag in item:
                return True
        return False
    
    def tagCounting(self, element, type_tender):
        for item in element.iter():
            if self.existTag(type_tender, item.tag.split('}')[1]):
                self.updateCount(type_tender, item.tag.split('}')[1])
            else:
                tag_count = {
                    item.tag.split('}')[1]: "tag",
                    "count": 1
                }
                type_structure[type_tender].append(tag_count)