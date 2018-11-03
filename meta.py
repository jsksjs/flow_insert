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


def get_meta(in_d, tag_s, q_d, c=os.cpu_count()):
    if in_d is not None and os.path.exists(in_d):
        in_dir = in_d
    else:
        raise IOError
    if tag_s is not None:
        tag_set = tag_s.replace("\n", "").split(', ')
        print(tag_set)
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
    return process(in_dir, tag_set, q_dir, cpus)


def collect_results(result):
    results.extend(result)


def exif(sid_images, sid, tag_set):
    S = []
    for path in sid_images:
        if os.name == 'nt':
            p = path.rsplit('\\')[-1]
            S.append({p: utils.get_exif_tags(path, tag_set)})
        else:
            p = path.rsplit('/')[-1]
            S.append({p: utils.get_exif_tags(path, tag_set)})
    return S


def process(in_dir, tag_set, q_dir, cpus):
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
                sid = os.path.basename(path).rsplit('_')[0]
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
                                   args=(sid_images, sid, tag_set),
                                   callback=collect_results)
                    time.sleep(0.1)
    p1.close()
    p1.join()

    stop = time.perf_counter()
    print(f'completed meta scrape for {total} files in {round(stop-start, 6)} sec')
    return results


# print summarized sets
"""def procSets(resultL):
    found = {}
    for result in resultL:
        for r in result:
            for j in r:
                # go through keys, access values
                for k in r[j].keys():
                    if k in found:
                        found[k].add("%s:%s" % (r[j]['Image Make'],
                                     r[j]['Image Model']))
                    else:
                        found[k] = set(["%s:%s" % (r[j]['Image Make'],
                                       r[j]['Image Model'])])
    with open("log.txt", "w") as file:
        print("\n%-40s\t%-125s\t%s" % ("Tag", "Value", "Set Length"))
        file.write("\n%-40s\t%-125s\t%s\n" % ("Tag", "Value", "Set Length"))
        for f in found:
            file.write("%-40s\t%-125s\t%s\n" % (f, found[f], len(found[f])))
            print("%-40s\t%-125s\t%s" % (f, found[f], len(found[f])))


# print raw exif
def procRaw(self, resultL):
    # number of entries
    i = 0
    with open("raw.tsv", "w") as file:
        for result in resultL:
            file.write("Tag\tValue\n")
            for r in result:
                i += 1
                print("\n%d" % i)
                for j in r:
                    h = 0;
                    # go through keys, access values
                    for k in r[j].keys():
                        self.results.append
                        file.write("%s\t%s\n" % (k, r[j][k]))
                        print("%-50s\t%-50s" % (k, r[j][k]))
                    file.write("%d Tags\n\n" % h)
                    print("%d Tags" % h);"""

