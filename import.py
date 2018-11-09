import mysql_connector as con
from time import perf_counter
from datetime import datetime
import time
import os
import meta as m
import argparse

des = """ECO DB Import Tool"""

parser = argparse.ArgumentParser(description=des,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i',
                    '--in_dir',
                    type=str,
                    help='image input directory')
parser.add_argument('-q',
                    '--q_dir',
                    type=str,
                    help='image quarantine directory')
parser.add_argument('-d',
                    '--d_dir',
                    type=str,
                    help='image delete directory')
parser.add_argument('-p',
                    '--cpus',
                    type=int,
                    help='number of processors')
parser.add_argument('-b',
                    '--buffer_size',
                    type=int,
                    help='buffer size')
args = parser.parse_args()

if args.in_dir is not None and os.path.exists(args.in_dir):
    in_dir = args.in_dir
else:
    raise IOError
if args.q_dir is not None and os.path.exists(args.q_dir):
    q_dir = args.q_dir
else:
    raise IOError
if args.d_dir is not None and os.path.exists(args.d_dir):
    d_dir = args.d_dir
else:
    raise IOError
if args.cpus is not None:
    cpus = args.cpus
else:
    cpus = os.cpu_count()
if args.buffer_size is not None:
    buffer_size = args.buffer_size
else:
    buffer_size = 1


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


def clean_dir(out, files):
    for f in files:
        folder = os.path.basename(os.path.dirname(f))
        out_folder = os.path.join(out, folder)
        file = os.path.join(out_folder, os.path.basename(f))
        if not os.path.exists(out_folder):
            os.mkdir(out_folder)
        os.rename(f, file)
        cur_folder = os.path.split(f)[0]
        if(cur_folder != in_dir):
            try:
                os.rmdir(cur_folder)
            except OSError:
                pass


if __name__ == '__main__':
    __spec__ = None
    # read cfg for host and database to connect to
    with open("db.cfg") as f:
        host = f.readline().strip('\n')
        database = f.readline().strip('\n')

    # read cfg for credentials (username and password to DB)
    # TODO: decrypt using a key found in another file
    with open("cred.cfg") as f:
        usr = f.readline().strip('\n')
        pwd = f.readline().strip('\n')

    # variable to be injected
    inj = []
    # failure injectable
    fail = []
    # query that injects ID into %s
    columns = ("CameraNumber,Orientation,"
               "XResolution,YResolution,"
               "ResolutionUnit,Software,TimeTaken,"
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
    log = ("insert into importrun (Start, End, Attempted) values (%s, %s, %s);")
    failure = ("insert into failure (Start, Checksum, Note) values ")
    fail_query = ''

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

    (total, meta), = m.get_meta(in_dir, tag_set, q_dir, cpus).items()
    values = cleaned_meta(meta)

    # TODO: run automations of this script, returning and logging the
    # total time of the inserts and tweaking the buffer size argument
    # based on consistent increases in time
    cols = tags.split(',')
    i = 0
    buffer_dumps = 0
    timestamps = ["DateTime"]
    ints = ["ImageDescription", "XResolution", "YResolution"]
    floats = ["ExposureTime",
              "FNumber",
              "CompressedBitsPerPixel",
              "ShutterSpeedValue"]
    files_pending = []
    deletes = []
    quarantines = []
    start = perf_counter()
    l_start = time.time()
    # connect and query
    with con.MYSQL(host, database, usr, pwd) as db:
        for row in values:
            fRow = []
            try:
                for c in cols:                    
                    if c not in row or row[c] is not None and row[c].strip(' ') == '':
                        row[c] = None
                    elif c in timestamps:
                        d = datetime.strptime(row[c], "%Y:%m:%d %H:%M:%S")
                        ts = int(time.mktime(d.timetuple()))
                        row[c] = ts
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
                quarantines.append(row["Path"])
                fail.extend([l_start, row["Checksum"], str(e)[0:800]])
                fail_query += '(' + '%s,'*2 + '%s),'
                continue
            i = i + 1
            files_pending.append(row["Path"])
            row = fRow
            query += '(' + '%s,'*(len(cols)-1) + '%s),'
            inj.extend(row)
            if i >= buffer_size:
                query = statement + query[:-1] + ';'
                r = db.query(query, inj)
                if not db.errors:
                    deletes.extend(files_pending)
                    files_pending = []
                else:
                    buffer_dumps += 1
                    data = [inj[x:x+len(cols)] for x in range(0, len(inj), len(cols))]
                    query = statement + '(' + '%s,'*(len(cols)-1) + '%s);'
                    for d, j in zip(data, files_pending):
                        inj = d
                        r = db.query(query, inj)
                        if db.errors:
                            quarantines.append(j)
                            fail.extend([l_start, d[20], str(db.errors)[0:800]])
                            fail_query += '(' + '%s,'*2 + '%s),'
                            db.errors = ''
                i = 0
                inj = []
                query = ''
        if i != 0:
            query = statement + query[:-1] + ";"
            r = db.query(query, inj)
            if not db.errors:
                deletes.extend(files_pending)
                files_pending = []
            else:
                buffer_dumps += 1
                data = [inj[x:x+len(cols)] for x in range(0, len(inj), len(cols))]
                query = statement + '(' + '%s,'*(len(cols)-1) + '%s);'
                for d, j in zip(data, files_pending):
                    inj = d
                    r = db.query(query, inj)
                    if db.errors:
                        quarantines.append(j)
                        fail.extend([l_start, d[20], str(db.errors)[0:800]])
                        fail_query += '(' + '%s,'*2 + '%s),'
                        db.errors = ''
            query = ''
        stop = perf_counter()
        if total > 0:
            l_stop = time.time()
            db.query(log, [l_start, l_stop, total])
            if fail:
                fail_query = failure + fail_query[:-1] + ";"
                r = db.query(fail_query, fail)

            r = db.query("insert into performance (Start, Time, Buffer, Attempted, BufferDumps) values (%s, %s, %s, %s, %s);", [l_start, round(stop-start, 6), buffer_size, total, buffer_dumps])
            #r = db.query("delete from image;", [])
            #clean_dir(d_dir, deletes)
            #clean_dir(q_dir, quarantines)
