from datetime import date
from ftplib import FTP
from date_range_parser import DataRangeParser

import os
import re 

class FtpFileDownloader():
    def __init__(self, ftp: FTP, threadsCount: int) -> None:
        self.dataRangeParser = DataRangeParser()
        self.ftp = ftp
        self.threadsCount = threadsCount

    def downloadFiles(self, source: str, path: str, dateStart: date, dateEnd: date) -> None:
        directory = self.ftp.nlst(source)

        fileCount = 0

        if len(directory) > 0:
            self._tryCreateDirectory(path)

            for file in directory:
                fileCount += 1

                if re.fullmatch(r'^.+(\.xml)+(\.zip)$', file):
                    file_name = file.split('/')[-1]
                    date_range = self.dataRangeParser.parse(file_name)

                    if date_range.start >= dateStart and date_range.end <= dateEnd:
                        self.ftp.retrbinary('RETR ' + file, open(path + file_name, 'wb').write)
                
                print(f'Download file {int(fileCount / len(directory) * 100)} %')

    def _tryCreateDirectory(self, path) -> None:
        try:
            os.makedirs(path)
        except OSError as exception:
            print("Error: The directory already exists or could not be created")