import exifread
import hashlib
from collections import defaultdict

# TODO:FIX
# given an image file with exif metadata return set of tags that are required
def get_exif_tags(path, tag_set=[]):
    tags = {}
    T = defaultdict(lambda: None)
    with open(path, 'rb') as f:
        tags = defaultdict(lambda: None, exifread.process_file(f))
    if not tag_set:
        for t in tags:
            if t not in ('JPEGThumbnail', 'TIFFThumbnail'):
                T[t] = str(tags[t]).rstrip(' ')
    else:
        for t in tag_set:
            if tags[t] is not None:
                T[t] = str(tags[t]).rstrip(' ')
            else:
                T[t] = None
        check = hashlib.shake_128()
        T["Data"] = f.read()
        check.update(T["Data"])
        T["Checksum"] = check.hexdigest(128)
    return T
