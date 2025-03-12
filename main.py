from tiktok_lib import scrape_list_channels, post_process
from gemini_lib import gemini_process
from typing import List, Union
import os


def main(channels: Union[List[str], str] = "") -> None:
    vid_folder = os.path.join("static", "tiktok_video")
    json_folder = os.path.join("static", "tiktok_json")
    cut_folder = os.path.join("static", "tiktok_cut")

    if isinstance(channels, str):
        channels = [channels]

    if len(channels) > 0:
        scrape_list_channels(channels)

    videos = [os.path.join(vid_folder, video) for video in os.listdir(vid_folder)]
    gemini_process(json_folder, cut_folder, videos)
    post_process(os.path.join(json_folder, "*-final.json"))
    print("Process done")


if __name__ == "__main__":
    # Done https://www.tiktok.com/@wildfusionai?lang=en
    main("https://www.tiktok.com/@wildfusionai?lang=en")
