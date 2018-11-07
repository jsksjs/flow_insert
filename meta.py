import os
import time
import multiprocessing as mp
import utils
from collections import defaultdict

"""ImageDescription, Orientation,
XResolution, YResolution,
ResolutionUnit, Software, DateTime,
ExposureTime, FNumber, ExposureProgram,
ISOSpeedRatings, ExifVersion,
ComponentsConfiguration, CompressedBitsPerPixel,
ShutterSpeedValue, ApertureValue, ExposureBiasValue,
MaxApertureValue, MeteringMode, Flash,
Checksum, Data"""

des = """image meta data crawler"""

results = []
hash_results = []


def collect_results(result):
    results.extend(result)


def insert_results(result):
    hash_results.extend(result)


def exif(sid_images, tag_set):
    S = []
    for path in sid_images:
        if os.name == 'nt':
            p = path.rsplit('\\')[-1]
            S.append({p: utils.get_exif_tags(path, tag_set)})
        else:
            p = path.rsplit('/')[-1]
            S.append({p: utils.get_exif_tags(path, tag_set)})
    return S


def h(sid_images):
    S = []
    for path in sid_images:
        if os.name == 'nt':
            p = path.rsplit('\\')[-1]
            S.append({p: utils.hash_it(path)})
        else:
            p = path.rsplit('/')[-1]
            S.append({p: utils.hash_it(path)})
    return S


def clean_dir(out, in_dir, files):
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


def get_meta(in_d, tag_s, q_d, c=os.cpu_count()):
    if in_d is not None and os.path.exists(in_d):
        in_dir = in_d
    else:
        raise IOError
    if tag_s is not None:
        tag_set = tag_s.replace("\n", "").split(', ')
    else:
        tag_set = []
    if q_d is not None:
        if os.path.exists(q_d):
            q_dir = q_d
        else:
            os.mkdir(q_d)
    if c is not None:
        cpus = c
    else:
        cpus = os.cpu_count()

    quarantines = []

    paths = defaultdict(lambda: [])
    for dirpath, dirnames, filenames in os.walk(in_dir):
        if dirnames:
            for d in dirnames:
                p = str(d.rsplit('_')[0])
                paths[p].append(os.path.join(dirpath, d))
    # map/scatter---------------------------------------------------------------------------------------------------
    p1 = mp.Pool(processes=cpus)
    total = 0
    for path in paths:
        for p in paths[path]:
            folder_info = os.path.basename(p).split('_')
            folder_info[3] = folder_info[3].split(' ')[0]
            # each site in ||
            for dirpath, dirnames, filenames in os.walk(p):
                sid_images = []
                for f in filenames:
                    if f.lower().endswith(".jpg"):   
                        file_info = f.split('_')
                        if ' ' in file_info[3]:
                            file_info[3] = file_info[3].split(' ')[0]
                        if bool(set(file_info) == set(folder_info)):
                            sid_images.append(os.path.join(dirpath, f))
                        else:
                            quarantines.append(os.path.join(dirpath, f))
                total += len(sid_images)
                if len(sid_images) > 0:
                    p1.apply_async(exif,
                                   args=(sid_images, tag_set),
                                   callback=collect_results)
                    time.sleep(0.1)
    p1.close()
    p1.join()

    #clean_dir(q_dir, in_dir, quarantines)

    return {total: results}
