from .download import download_video
from typing import List
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os

def get_all_video_links_from_a_channel(
    channel_url: str
) -> List[str]:
    """
    This function scrapes all the videos from a Tiktok channel.
    """

    # Initialize selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(channel_url)

    # Find all <a> tags with the specified class
    a_tags = driver.find_elements(
        By.CSS_SELECTOR, 'a.css-1mdo0pl-AVideoContainer.e19c29qe4'
    )

    # Retrieve the href attributes of all found <a> tags
    videos = []
    for a in a_tags:
        videos.append(a.get_attribute('href'))

    # Close the browser window
    driver.quit()
    return videos

def read_file(
    filepath: str
) -> List[str]:
    """
    This function reads a text file and returns a list of items.
    """
    items = []
    if not os.path.exists(filepath):
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write('')
    else:
        with open(filepath, 'r', encoding='utf-8') as file:
            items = [item.strip() for item in file.readlines()]
    return items

def save_file(
    filepath: str,
    items: List[str]
) -> None:
    """
    This function saves unique items in a text file.
    """
    old = read_file(filepath)
    with open(filepath, 'w', encoding='utf-8') as file:
        for item in items:
            if item in old:
                file.write(item + '\n')
    return None

def scrape_list_channels(
    channels: List[str],
    file_output: str = os.path.join('static', 'tiktok_metadata'),
    video_output: str = os.path.join('static', 'tiktok_video')
) -> None:
    """
    This function scrapes and downloads all the videos from multiple Tiktok channels.
    """
    
    # Setup folders and metadata
    os.makedirs(file_output, exist_ok=True)
    os.makedirs(video_output, exist_ok=True)
    file_channel = os.path.join(file_output, 'tiktok_channels.txt')
    file_video = os.path.join(file_output, 'tiktok_videos.txt')
    _ = read_file(file_channel)
    videos = read_file(file_video)
    
    # Retrieve all the video urls from Tiktok channels
    for channel in tqdm(channels, total=len(channels), desc='Channels', colour='green'):
        try:
            videos.extend(get_all_video_links_from_a_channel(channel))
        except Exception as e:
            continue

    # Save scraped channels
    save_file(
        filepath=file_channel,
        items=channels
    )

    # Save scraped videos
    save_file(
        filepath=file_video,
        items=videos
    )

    # Download videos
    for video in tqdm(videos, total=len(videos), desc='Videos', colour='green'):
        try:
            output = os.path.join(video_output, video.split('video/')[-1] + '.mp4')
            if os.path.exists(output):
                continue
            download_video(video, output)
        except Exception as e:
            print(e)
            continue