import ftplib

ftp = ftplib.FTP(host='95.167.245.94', timeout=10)
ftp.login(user='free', passwd='free')

directory = ftp.nlst()
print(directory)