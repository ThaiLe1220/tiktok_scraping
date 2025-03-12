import os
import time
from typing import Dict, List
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from yt_dlp import YoutubeDL


PROXY = [
    "socks5://98.181.137.80:4145",
    "socks5://74.119.147.209:4145",
    "socks5://199.229.254.129:4145",
    "socks5://142.54.237.34:4145",
    "socks5://142.54.228.193:4145",
    "socks5://184.181.217.210:4145",
    "socks5://184.170.248.5:4145",
    "socks5://24.249.199.12:4145",
    "socks5://192.111.137.35:4145",
    "socks5://192.111.134.10:4145",
    "socks5://184.178.172.17:4145",
    "socks5://98.188.47.150:4145",
    "socks5://72.37.217.3:4145",
    "socks5://107.181.168.145:4145",
]


def download_video(url: str, output: str, max_retries: int = len(PROXY)) -> None:
    """
    This function downloads video from the given url and save as provided path.
    """
    retries = 0
    while retries < max_retries:
        try:
            # Set up the ydl options
            ydl_opts = {
                "format": "bestvideo+bestaudio/best",
                "outtmpl": output,
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "merge_output_format": "mp4",
                "prefer_ffmpeg": True,
                "geo_bypass": True,
                "nocheckcertificate": True,
            }

            if retries != 0:
                ydl_opts["proxy"] = PROXY[retries - 1]

            with YoutubeDL(ydl_opts) as ydl:
                ydl.extract_info(url, download=True)
                return
        except Exception:
            retries += 1
    return


def get_all_video_links_from_a_channel(channel_url: str) -> List[str]:
    """
    This function scrapes all the videos from a Tiktok channel by simulating scrolling.
    """
    print(f"Scraping videos from {channel_url}")
    start_time = time.time()

    # Initialize selenium
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(channel_url)

    # Initial wait for page to load
    time.sleep(60)

    # Set to store unique video links
    videos = set()

    # Prepare for scrolling
    last_video_count = 0
    no_new_videos_count = 0
    max_attempts_without_new_videos = 3
    max_scrolls = 50

    for scroll_num in range(max_scrolls):
        # Find all video links
        a_tags = driver.find_elements(
            By.CSS_SELECTOR, "a.css-1mdo0pl-AVideoContainer.e19c29qe4"
        )

        # Extract and store the links
        for a in a_tags:
            href = a.get_attribute("href")
            if href:
                videos.add(href)

        # Check if we found new videos
        if len(videos) > last_video_count:
            print(
                f"Scroll {scroll_num + 1}: Found {len(videos) - last_video_count} new videos. Total: {len(videos)}"
            )
            last_video_count = len(videos)
            no_new_videos_count = 0
        else:
            no_new_videos_count += 1
            print(
                f"Scroll {scroll_num + 1}: No new videos found. Attempts without new videos: {no_new_videos_count}/{max_attempts_without_new_videos}"
            )

            # If we haven't found new videos for several attempts, assume we're done
            if no_new_videos_count >= max_attempts_without_new_videos:
                print("No new videos found after multiple scrolls. Stopping.")
                break

        # Scroll down to load more content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait for new content to load
        time.sleep(3)

    # Close the browser window
    driver.quit()

    duration = time.time() - start_time
    print(f"Found {len(videos)} videos in {duration:.2f} seconds")
    return list(videos)


def update_video_tracking_file(file_path: str, video_urls: List[str]) -> None:
    """
    Update the video tracking file with all known video URLs.

    Args:
        file_path: Path to the tracking file
        video_urls: List of video URLs to save
    """
    with open(file_path, "w", encoding="utf-8") as file:
        for video_url in video_urls:
            file.write(video_url + "\n")


def scrape_list_channels(
    channels: List[str],
    file_output: str = os.path.join("static", "tiktok_metadata"),
    video_output: str = os.path.join("static", "tiktok_video"),
) -> Dict[str, Dict[str, int]]:
    """
    This function scrapes and downloads all the videos from multiple Tiktok channels.

    Returns:
        Dictionary with statistics about processed videos for each channel
    """

    # Setup metadata folder
    os.makedirs(file_output, exist_ok=True)
    file_video = os.path.join(file_output, "tiktok_videos.txt")

    # Initialize a dict to track all video URLs by their ID
    all_videos_dict = {}

    # First, scan all existing channel directories to build a record of downloaded videos
    static_dir = "static"
    if os.path.exists(static_dir):
        for dir_name in os.listdir(static_dir):
            channel_dir = os.path.join(static_dir, dir_name)
            if os.path.isdir(channel_dir):
                src_dir = os.path.join(channel_dir, "src")
                if os.path.exists(src_dir):
                    for file_name in os.listdir(src_dir):
                        if file_name.endswith(".mp4"):
                            video_id = file_name.replace(".mp4", "")
                            # Create a placeholder URL based on the directory structure
                            all_videos_dict[video_id] = (
                                f"https://www.tiktok.com/@{dir_name}/video/{video_id}"
                            )

    # Store statistics for each channel
    channel_stats = {}

    # Process each requested channel
    for channel in tqdm(channels, total=len(channels), desc="Channels", colour="green"):
        try:
            channel_name = (
                channel.split("@")[-1].split("?")[0] if "@" in channel else channel
            )
            print(f"\n{'='*50}")
            print(f"Processing channel: @{channel_name}")

            # Create channel-specific directory
            channel_src_dir = os.path.join("static", channel_name, "src")
            os.makedirs(channel_src_dir, exist_ok=True)

            # Track statistics for this channel
            channel_stats[channel_name] = {
                "total_found": 0,
                "already_downloaded": 0,
                "new_downloads": 0,
                "failed_downloads": 0,
            }

            # Get videos for this channel
            channel_videos = get_all_video_links_from_a_channel(channel)
            channel_stats[channel_name]["total_found"] = len(channel_videos)

            if not channel_videos:
                print(f"Warning: No videos found for channel @{channel_name}")
                continue

            # Update our all_videos_dict with correct URLs
            for video_url in channel_videos:
                video_id = video_url.split("video/")[-1]
                all_videos_dict[video_id] = video_url

            # Check existing videos in the channel's src directory
            existing_video_files = (
                [f for f in os.listdir(channel_src_dir) if f.endswith(".mp4")]
                if os.path.exists(channel_src_dir)
                else []
            )
            existing_video_ids = {
                file.replace(".mp4", "") for file in existing_video_files
            }

            # Identify which videos need to be downloaded (don't exist in the src directory)
            new_videos = []
            for video in channel_videos:
                video_id = video.split("video/")[-1]
                if video_id not in existing_video_ids:
                    new_videos.append(video)

            channel_stats[channel_name]["already_downloaded"] = len(existing_video_ids)

            print(f"Found {len(channel_videos)} videos for @{channel_name}")
            print(f"  - {len(existing_video_ids)} videos already downloaded")
            print(f"  - {len(new_videos)} new videos to download")

            # Download only this channel's new videos
            failed = 0
            for video in tqdm(
                new_videos,
                total=len(new_videos),
                desc=f"Downloading videos for @{channel_name}",
                colour="blue",
            ):
                try:
                    video_id = video.split("video/")[-1]
                    output = os.path.join(channel_src_dir, video_id + ".mp4")
                    download_video(video, output)

                except Exception as e:
                    print(f"Error downloading {video}: {str(e)}")
                    failed += 1

            channel_stats[channel_name]["new_downloads"] = len(new_videos) - failed
            channel_stats[channel_name]["failed_downloads"] = failed

            print(f"Download complete for @{channel_name}:")
            print(f"  - Successfully downloaded {len(new_videos) - failed} new videos")
            if failed > 0:
                print(f"  - Failed to download {failed} videos")

        except Exception as e:
            print(f"Error processing channel {channel}: {str(e)}")

    # Update the videos.txt file with all videos (including those from other channels)
    update_video_tracking_file(file_video, list(all_videos_dict.values()))

    # Print summary
    print(f"\n{'='*50}")
    print("Download Summary:")
    for channel, stats in channel_stats.items():
        print(f"@{channel}:")
        print(f"  - Total videos found: {stats['total_found']}")
        print(f"  - Already downloaded: {stats['already_downloaded']}")
        print(f"  - Newly downloaded: {stats['new_downloads']}")
        if stats["failed_downloads"] > 0:
            print(f"  - Failed downloads: {stats['failed_downloads']}")
    print(f"{'='*50}")

    return channel_stats
