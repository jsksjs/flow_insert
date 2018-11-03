import mysql_connector as con
import time
import meta as m

if __name__ == '__main__':
    __spec__ = None
    # read cfg for host and database to connect to
    with open("db.cfg") as f:
        host = f.readline().rstrip('\n')
        database = f.readline().rstrip('\n')

    # read cfg for credentials (username and password to DB)
        # TODO: decrypt using a key found in another file
    with open("cred.cfg") as f:
        usr = f.readline().rstrip('\n')
        pwd = f.readline().rstrip('\n')

    # connect
    db = con.MYSQL(host, database, usr, pwd)

    # time query
    start = time.perf_counter()

    # variable to be injected
    inj = []
    # query that injects ID into %s
    statement = ("insert into image (ImageDescription, Orientation, "
                 "XResolution, YResolution, "
                 "ResolutionUnit, Software, DateTime, "
                 "ExposureTime, FNumber, ExposureProgram, "
                 "ISOSpeedRatings, ExifVersion, "
                 "ComponentsConfiguration, "
                 "CompressedBitsPerPixel, "
                 "ShutterSpeedValue, ApertureValue, "
                 "ExposureBiasValue, "
                 "MaxApertureValue, MeteringMode, Flash, "
                 "Checksum, Data) values ")

    query = ''

    scrape = m.get_meta("C:/Users/Joey/Desktop/images",
                        ("Image ImageDescription, Image Orientation, "
                         "Image XResolution, Image YResolution, "
                         "Image ResolutionUnit, Image Software, "
                         "Image DateTime, EXIF ExposureTime, EXIF FNumber, "
                         "EXIF ExposureProgram, EXIF ISOSpeedRatings, "
                         "EXIF ExifVersion, EXIF ComponentsConfiguration, "
                         "EXIF CompressedBitsPerPixel, "
                         "EXIF ShutterSpeedValue, "
                         "EXIF ApertureValue, EXIF ExposureBiasValue, "
                         "EXIF MaxApertureValue, "
                         "EXIF MeteringMode, EXIF Flash"
                         ), "C:/Users/Joey/Desktop/quarantine")

    print(scrape)




"""
buffer_size = 1
i = 500
for row in values:
    i = i + 1
    # the indices of the columns whose types/formats need to be changed
    dates = [3]
    ints = [2]
    chars = [5]
    # replace null values with None and change types
    for j in range(0, len(row)):
        if row[j].rstrip(' ') == '':
            row[j] = None
        elif j in dates:
            temp = row[j].split('/')
            year = temp[2]
            month = temp[0]
            day = temp[1]
            month.zfill(2)
            day.zfill(2)
            row[j] = year + '-' + month + '-' + day
        elif j in ints:
            row[j] = int(row[j])
        elif j in chars:
            row[j] = str(row[j])
    query += '(' + '%s,'*(len(row)-1) + '%s),'
    inj.extend(row)
    if i >= buffer_size:
        query = statement + query[:-1] + ";"
        r = db.query(query, inj)
        i = 0
        inj = []
        query = ''
if i != 0:
    query = statement + query[:-1] + ";"
    r = db.query(query, inj)
    query = ''

# stop timing
stop = time.perf_counter()
# print the result and rounded query time
print(f'\n{r}\ncompleted query in {round(stop-start, 6)} sec')
[print(i) for i in (db.errors.split("query():"))]
"""
