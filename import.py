import mysql_connector as con
import time
import os
import meta as m

# TODO: FIX get_exif_tags in utils.py
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

    in_dir = "C:/Users/Joey/Desktop/images"
    tag_set = ("Image ImageDescription, Image Orientation, "
               "Image XResolution, Image YResolution, "
               "Image ResolutionUnit, Image Software, "
               "Image DateTime, EXIF ExposureTime, EXIF FNumber, "
               "EXIF ExposureProgram, EXIF ISOSpeedRatings, "
               "EXIF ExifVersion, EXIF ComponentsConfiguration, "
               "EXIF CompressedBitsPerPixel, "
               "EXIF ShutterSpeedValue, "
               "EXIF ApertureValue, EXIF ExposureBiasValue, "
               "EXIF MaxApertureValue, "
               "EXIF MeteringMode, EXIF Flash")
    
    q_dir = "C:/Users/Joey/Desktop/quarantine"
    cpus = os.cpu_count()

    start = time.perf_counter()

    total = m.get_meta(in_dir, tag_set, q_dir, cpus)

    stop = time.perf_counter()

    # for result in total:
    #   print(result)

    # print(total)    
    with open("oog.txt", "w") as f:
        for t in total:
            for j in t:                
                f.write(str(t[j]))
