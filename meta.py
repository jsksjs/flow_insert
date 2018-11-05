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

    start = time.perf_counter()

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
            # each site in ||
            for dirpath, dirnames, filenames in os.walk(p):
                sid_images = []
                for f in filenames:
                    if f.lower().endswith(".jpg"):
                        if os.path.basename(p).lower() in f.lower():
                            sid_images.append(os.path.join(dirpath, f))
                        else:
                            os.rename(os.path.join(dirpath, f),
                                      os.path.join(q_dir, f))
                total += len(sid_images)
                if len(sid_images) > 0:
                    p1.apply_async(exif,
                                   args=(sid_images, tag_set),
                                   callback=collect_results)
                    time.sleep(0.1)
    p1.close()
    p1.join()
    return results

    p1 = mp.Pool(processes=cpus)
    for path in paths:
        for p in paths[path]:
            # each site in ||
            for dirpath, dirnames, filenames in os.walk(p):
                sid_images = []
                for f in filenames:
                    if f.lower().endswith(".jpg"):
                        if os.path.basename(p).lower() in f.lower():
                            sid_images.append(os.path.join(dirpath, f))
                if len(sid_images) > 0:
                    p1.apply_async(h,
                                   args=([sid_images]),
                                   callback=insert_results)
                    time.sleep(0.1)
    p1.close()
    p1.join()

    for t in hash_results:
        for j in t:
            results[j].update(t[j])
            f.write(str(t[j])+"\n")

    stop = time.perf_counter()
    print(f'completed meta scrape for {total} files in {round(stop-start, 6)} sec')
    return results
