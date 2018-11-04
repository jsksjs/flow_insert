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
    return T


def hash_it(image):
    data = b''
    checksum = ''
    hasher = hashlib.sha256()
    with open(image, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            data += chunk
            hasher.update(chunk)
    checksum = hasher.hexdigest()
    return {"Data": data, "Checksum": checksum}
