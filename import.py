import mysql_connector as con
import time
import os
import meta as m

# TODO: fail gracefully during batch-inserts
# (coninue on to single-inserts, use "Path" key to get file).
# TODO: sort success and failure files by moving them

def cleaned_meta(meta):
    values = []
    for d in meta:
        for s in d:
            vals = {}
            for v in d[s]:
                tag = v.split(' ')
                if len(tag) > 1:
                    tag = tag[1]
                else:
                    tag = tag[0]
                vals[tag] = d[s][v]
            values.append(vals)
    return values


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

    # time query
    start = time.perf_counter()

    # variable to be injected
    inj = []
    # query that injects ID into %s
    columns = ("CameraNumber,Orientation,"
               "XResolution,YResolution,"
               "ResolutionUnit,Software,DateTime,"
               "ExposureTime,FNumber,ExposureProgram,"
               "ISOSpeedRatings,ExifVersion,"
               "ComponentsConfiguration,"
               "CompressedBitsPerPixel,"
               "ShutterSpeedValue,ApertureValue,"
               "ExposureBiasValue,"
               "MaxApertureValue,MeteringMode,Flash,"
               "Checksum,Data")
    tags = ("ImageDescription,Orientation,"
            "XResolution,YResolution,"
            "ResolutionUnit,Software,DateTime,"
            "ExposureTime,FNumber,ExposureProgram,"
            "ISOSpeedRatings,ExifVersion,"
            "ComponentsConfiguration,"
            "CompressedBitsPerPixel,"
            "ShutterSpeedValue,ApertureValue,"
            "ExposureBiasValue,"
            "MaxApertureValue,MeteringMode,Flash,"
            "Checksum,Data")
    statement = (f"insert into image ({columns}) values ")

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
    d_dir = "C:/Users/Joey/Desktop/del"
    cpus = os.cpu_count()

    (total, meta), = m.get_meta(in_dir, tag_set, q_dir, cpus).items()
    values = cleaned_meta(meta)

    # TODO: run automations of this script, returning and logging the
    # total time of the inserts and tweaking the buffer size argument
    # based on consistent increases in time
    cols = tags.split(',')
    buffer_size = 1
    i = 0
    timestamps = ["DateTime"]
    ints = ["ImageDescription", "XResolution", "YResolution"]
    floats = ["ExposureTime"]
    files_pending = []
    deletes = []
    quarantines = []
    start = time.perf_counter()
    # connect and query
    with con.MYSQL(host, database, usr, pwd) as db:
        for row in values:
            i = i + 1
            fRow = []
            for c in cols:
                try:
                    if c not in row or row[c].rstrip(' ') == '':
                        row[c] = None
                    elif c in timestamps:
                        temp = row[c].split(' ')
                        row[c] = temp[0].replace(':', '-') + " " + temp[1]
                    elif c in ints:
                        row[c] = int(row[c])
                    elif c in floats:
                        if '/' in row[c]:
                            nums = row[c].split('/')
                            row[c] = round(float(nums[0])/float(nums[1]), 6)
                        else:
                            row[c] = float(row[c])
                    fRow.append(row[c])
                except Exception as e:
                    print(e)
            files_pending.append(row["Path"])
            row = fRow
            query += '(' + '%s,'*(len(cols)-1) + '%s),'
            inj.extend(row)
            if i >= buffer_size:
                query = statement + query[:-1] + ";"
                r = db.query(query, inj)
                if not db.errors:
                    deletes.extend(files_pending)
                    files_pending = []
                i = 0
                inj = []
                query = ''
        if i != 0:
            query = statement + query[:-1] + ";"
            r = db.query(query, inj)
            query = ''
        stop = time.perf_counter()
        print(f'Attempted insertion for {total} files in {round(stop-start, 6)} sec')
        for f in deletes:
            folder = os.path.basename(os.path.dirname(f))
            del_folder = os.path.join(d_dir, folder)
            file = os.path.join(del_folder, os.path.basename(f))
            if not os.path.exists(del_folder):
                os.mkdir(del_folder)
            os.rename(f, file)
            cur_folder = os.path.split(f)[0]
            try:
                os.rmdir(cur_folder)
            except OSError:
                pass
