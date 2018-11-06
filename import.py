import mysql_connector as con
from time import perf_counter
from datetime import datetime
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
        host = f.readline().rstrip('\n')
        database = f.readline().rstrip('\n')

    # read cfg for credentials (username and password to DB)
    # TODO: decrypt using a key found in another file
    with open("cred.cfg") as f:
        usr = f.readline().rstrip('\n')
        pwd = f.readline().rstrip('\n')

    # variable to be injected
    inj = []
    # failure injectable
    fail = []
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
    buffer_size = 1
    i = 0
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
    l_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # connect and query
    with con.MYSQL(host, database, usr, pwd) as db:
        for row in values:
            fRow = []
            try:
                for c in cols:
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
                quarantines.append(row["Path"])
                fail.extend([l_start, row["Checksum"], str(e)[:255]])
                fail_query += '(' + ('%s,'*2) + '%s),'
                continue
            i = i + 1
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
        stop = perf_counter()
        if total > 0:
            l_stop = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db.query(log, [l_start, l_stop, total])
            if fail:
                fail_query = failure + fail_query[:-1] + ";"
                r = db.query(fail_query, fail)

        clean_dir(d_dir, deletes)
        clean_dir(q_dir, quarantines)
