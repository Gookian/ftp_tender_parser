from datetime import date
from ftplib import FTP
from date_range_parser import DataRangeParser

import os
import re
import time

class FtpFileDownloader():
    def __init__(self, ftp: FTP, threadsCount: int) -> None:
        self.dataRangeParser = DataRangeParser()
        self.ftp = ftp
        self.threadsCount = threadsCount

    def downloadFiles(self, source: str, path: str, dateStart: date, dateEnd: date) -> None:
        directory = self.ftp.nlst(source)

        fileCount = 0
        fileSkipCount = 0
        secondsTotal = 0

        if len(directory) > 0:
            self._tryCreateDirectory(path)

            for file in directory:
                fileCount += 1
                start_time = time.time()

                if re.fullmatch(r'^.+(\.xml)+(\.zip)$', file):
                    file_name = file.split('/')[-1]
                    date_range = self.dataRangeParser.parse(file_name)

                    if date_range.start >= dateStart and date_range.end <= dateEnd:
                        self.ftp.retrbinary('RETR ' + file, open(path + file_name, 'wb').write)
                    else:
                        fileSkipCount += 1
                else:
                    fileSkipCount += 1
                
                secondsDelta = (time.time() - start_time)
                secondsTotal += secondsDelta
                secondsMean = secondsTotal / (fileCount - fileSkipCount + 1) * (len(directory) - fileSkipCount)
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
                print('\033[F\033[K', end='')
                print(f'Скачивание фвйлов {int(fileCount / len(directory) * 100)}%\t|\tВремя скачивания {days} дней {hours} часов {minutes} минут {seconds} секунд')

    def _tryCreateDirectory(self, path) -> None:
        try:
            os.makedirs(path)
        except OSError as exception:
            print("Error: The directory already exists or could not be created")