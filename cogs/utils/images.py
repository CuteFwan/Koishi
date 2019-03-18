from PIL import Image
from io import BytesIO

def resize_to_limit(data, limit):
    '''
        Downsize it for huge PIL images.
        Half the resolution until the byte count is within the limit.
    '''
    current_size = data.getbuffer().nbytes
    while current_size > limit:
        with Image.open(data) as im:
            im = im.resize([i//2 for i in im.size], resample=Image.BICUBIC)
            data = BytesIO()
            im.save(data, 'png')
            data.seek(0)
            current_size = data.getbuffer().nbytes
    return data