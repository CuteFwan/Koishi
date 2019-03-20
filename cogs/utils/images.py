from PIL import Image, ImageSequence
from io import BytesIO

def resize_to_limit(data, limit):
    '''
        Downsize it for huge PIL images.
        Half the resolution until the byte count is within the limit.
    '''
    current_size = data.getbuffer().nbytes
    while current_size > limit:
        with Image.open(data) as im:
            data = BytesIO()
            if im.format == 'PNG':
                im = im.resize([i//2 for i in im.size], resample=Image.BICUBIC)
                im.save(data, 'png')
            elif im.format == 'GIF':
                durations = []
                new_frames = []
                for frame in ImageSequence.Iterator(im):
                    durations.append(frame.info['duration'])
                    new_frames.append(frame.resize([i//2 for i in im.size], resample=Image.BICUBIC))
                new_frames[0].save(
                    data,
                    save_all=True,
                    append_images=new_frames[1:],
                    format='gif',
                    version=im.info['version'],
                    duration=durations,
                    loop=0,
                    background=im.info['background'],
                    palette=im.getpalette())
            data.seek(0)
            current_size = data.getbuffer().nbytes
    return data