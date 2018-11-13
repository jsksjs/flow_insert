import exifread
import hashlib
import binascii

# Given an image file with exif metadata, return set of tags that are required
# as well as a checksum and the hex of the file.
def get_exif_tags(path, tag_set=[]):
    data = b''
    checksum = ''
    hasher = hashlib.sha256()
    tags, T = {}, {}
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(524288), b""):
            data += chunk
            hasher.update(chunk)
        f.seek(0)
        tags = exifread.process_file(f)
    checksum = hasher.hexdigest()
    if not tag_set:
        for t in tags:
            if t not in ('JPEGThumbnail', 'TIFFThumbnail'):
                T[t] = str(tags[t]).rstrip(' ')
    else:
        for t in tag_set:
            if tags.get(t) is not None:
                T[t] = str(tags[t]).rstrip(' ')
            else:
                T[t] = None
    T["Data"] = str(binascii.hexlify(data))
    T["Checksum"] = checksum
    T["Path"] = path
    return T
