from yt_dlp import YoutubeDL

PROXY = [
    'socks5://98.181.137.80:4145',
    'socks5://74.119.147.209:4145',
    'socks5://199.229.254.129:4145',
    'socks5://142.54.237.34:4145',
    'socks5://142.54.228.193:4145',
    'socks5://184.181.217.210:4145',
    'socks5://184.170.248.5:4145',
    'socks5://24.249.199.12:4145',
    'socks5://192.111.137.35:4145',
    'socks5://192.111.134.10:4145',
    'socks5://184.178.172.17:4145',
    'socks5://98.188.47.150:4145',
    'socks5://72.37.217.3:4145',
    'socks5://107.181.168.145:4145',
]

def download_video(
    url: str, 
    output: str, 
    max_retries: int = len(PROXY)
) -> None:
    """
    This function downloads video from the given url and save as provided path.
    """
    retries = 0
    while retries < max_retries:
        try:
            # Set up the ydl options
            ydl_opts = {
                'format': 'bestvideo+bestaudio/best',
                'outtmpl': output,
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'merge_output_format': 'mp4',
                'prefer_ffmpeg': True,
                'geo_bypass': True,
                'nocheckcertificate': True,
            }

            if retries != 0:
                ydl_opts['proxy'] = PROXY[retries - 1]

            with YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)
                return
        except Exception:
            retries += 1
    return
