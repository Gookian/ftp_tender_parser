from zipfile import ZipFile
from xml.etree import ElementTree

import os

class TenderReader():
    @staticmethod
    def readFiles(directory_str):
        directory = os.fsencode(directory_str)

        for file in os.listdir(directory):
            filename = os.fsdecode(file)

            if filename.endswith(".zip"):

                with ZipFile(directory_str + filename, "r") as myzip:

                    for nameFileXml in myzip.namelist():

                        if nameFileXml.endswith(".xml"):
                            str_data_xml = myzip.read(nameFileXml).decode('utf-8')
                            root = ElementTree.fromstring(str_data_xml)
                            for element in root.iter("{http://zakupki.gov.ru/oos/types/1}id"):
                                print(f'{element.text}')
                            for element in root.iter("{http://zakupki.gov.ru/oos/types/1}purchaseNumber"):
                                print(f'{element.text}')
                print("\n")
                continue
            else:
                continue