import os
import argparse
import re
import sys
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from utils import scrape_list_channels, gemini_process, process_video_cuts, post_process


def extract_channel_name(url: str) -> Optional[str]:
    """Extract channel name from TikTok URL."""
    match = re.search(r"@([a-zA-Z0-9_\.]+)", url)
    if match:
        return match.group(1)
    return None


def create_channel_directories(channel_name: str) -> tuple:
    """Create directory structure for a channel."""
    base_dir = os.path.join("static", channel_name)
    src_dir = os.path.join(base_dir, "src")
    json_dir = os.path.join(base_dir, "json")
    vid_dir = os.path.join(base_dir, "video")

    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(json_dir, exist_ok=True)
    os.makedirs(vid_dir, exist_ok=True)

    return src_dir, json_dir, vid_dir


def scrape_channel_videos(channel_url: str) -> Optional[str]:
    """Download videos from a TikTok channel."""
    channel_name = extract_channel_name(channel_url)
    if not channel_name:
        print(f"Error: Could not extract channel name from URL: {channel_url}")
        return None

    try:
        src_dir, _, _ = create_channel_directories(channel_name)
        print(f"Downloading videos from channel: @{channel_name}")

        file_output = os.path.join("static", channel_name)
        scrape_list_channels(
            [channel_url], file_output=file_output, video_output=src_dir
        )

        print(f"Successfully downloaded videos for channel: @{channel_name}")
        return channel_name
    except Exception as e:
        print(f"Error downloading videos for channel @{channel_name}: {str(e)}")
        return None


def process_video_batch(json_dir, video_batch):
    """Process a batch of videos with Gemini AI."""
    try:
        videos_to_process = []
        skipped_videos = 0

        for video_path in video_batch:
            video_id = os.path.splitext(os.path.basename(video_path))[0]
            output_path = os.path.join(json_dir, f"{video_id}-result.json")

            if os.path.exists(output_path):
                skipped_videos += 1
                print(
                    f"Skipping video '{video_id}': '{video_id}-result.json' already exists."
                )
            else:
                videos_to_process.append(video_path)

        if videos_to_process:
            gemini_process(json_dir, videos_to_process)

        return True, len(videos_to_process), skipped_videos
    except Exception as e:
        print(f"Error processing video batch: {str(e)}")
        return False, 0, 0


def analyze_channel_videos(channel_name: str, num_threads: int = 3) -> bool:
    """Analyze videos for a specific channel using Gemini AI."""
    try:
        src_dir, json_dir, vid_dir = create_channel_directories(channel_name)

        if not os.path.exists(src_dir):
            print(f"Error: Source directory not found for channel: @{channel_name}")
            return False

        videos = [
            os.path.join(src_dir, video)
            for video in os.listdir(src_dir)
            if video.endswith(".mp4")
        ]

        if not videos:
            print(f"Warning: No videos found for channel: @{channel_name}")
            return False

        print(f"Found {len(videos)} videos for channel: @{channel_name}")

        batch_size = max(1, len(videos) // num_threads)
        video_batches = [
            videos[i : i + batch_size] for i in range(0, len(videos), batch_size)
        ]

        total_processed = 0
        total_skipped = 0

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            results = list(
                executor.map(
                    lambda batch: process_video_batch(json_dir, batch),
                    video_batches,
                )
            )

        for success, processed, skipped in results:
            if success:
                total_processed += processed
                total_skipped += skipped

        print(
            f"AI Analysis: @{channel_name} - Processed: {total_processed}, Skipped: {total_skipped}"
        )

        if all(result[0] for result in results):
            print(f"Successfully analyzed videos for channel: @{channel_name}")
            return True
        else:
            print(f"Some analysis tasks failed for channel: @{channel_name}")
            return False

    except Exception as e:
        print(f"Error analyzing videos for channel @{channel_name}: {str(e)}")
        return False


def cut_channel_videos(channel_name: str) -> bool:
    """Cut videos into scenes based on Gemini analysis results."""
    try:
        src_dir, json_dir, vid_dir = create_channel_directories(channel_name)

        if not os.path.exists(json_dir):
            print(f"Error: JSON directory not found for channel: @{channel_name}")
            return False

        # Remove all JSON files ending with "-final.json" before processing
        for file in os.listdir(json_dir):
            if file.endswith("-final.json"):
                os.remove(os.path.join(json_dir, file))
                print(f"Removed file: {file}")

        print(f"Cutting videos into scenes for channel: @{channel_name}")

        # Call process_video_cuts directly
        try:
            process_video_cuts(json_dir, vid_dir, src_dir)
            print(f"Successfully cut videos for channel: @{channel_name}")
            return True
        except Exception as e:
            print(f"Error during video cutting: {str(e)}")
            return False

    except Exception as e:
        print(f"Error cutting videos for channel @{channel_name}: {str(e)}")
        return False


def post_process_channel(channel_name: str) -> bool:
    """Post-process videos for a specific channel."""
    try:
        _, json_dir, _ = create_channel_directories(channel_name)

        if not os.path.exists(json_dir):
            print(f"Error: JSON directory not found for channel: @{channel_name}")
            return False

        print(f"Post-processing results for channel: @{channel_name}")
        post_process(os.path.join(json_dir, "*-final.json"))
        print(f"Successfully post-processed results for channel: @{channel_name}")
        return True
    except Exception as e:
        print(f"Error post-processing results for channel @{channel_name}: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description="TikTok Auto AI Fusion Tool")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Download command
    download_parser = subparsers.add_parser(
        "download", help="Download videos from TikTok channels"
    )
    download_parser.add_argument("channels", nargs="+", help="TikTok channel URLs")

    # Gemini analysis command
    analyze_parser = subparsers.add_parser(
        "analyze", help="Run Gemini AI analysis on downloaded videos"
    )
    analyze_parser.add_argument(
        "channels", nargs="+", help="Channel names to analyze (without @)"
    )
    analyze_parser.add_argument(
        "--threads",
        type=int,
        default=3,
        help="Number of threads for parallel processing",
    )

    # Video cutting command
    cut_parser = subparsers.add_parser(
        "cut", help="Cut videos into scenes based on Gemini analysis results"
    )
    cut_parser.add_argument(
        "channels", nargs="+", help="Channel names to cut (without @)"
    )

    # Combined process command (for backward compatibility)
    process_parser = subparsers.add_parser(
        "process", help="Run both Gemini analysis and video cutting"
    )
    process_parser.add_argument(
        "channels", nargs="+", help="Channel names to process (without @)"
    )

    # Post-process command
    postprocess_parser = subparsers.add_parser(
        "postprocess", help="Post-process processed videos"
    )
    postprocess_parser.add_argument(
        "channels", nargs="+", help="Channel names to post-process (without @)"
    )

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return

    if args.action == "download":
        successful_channels = 0
        for channel_url in args.channels:
            if scrape_channel_videos(channel_url):
                successful_channels += 1

        print(
            f"Download complete. Successfully processed {successful_channels}/{len(args.channels)} channels."
        )

    elif args.action == "analyze":
        successful_channels = 0
        num_threads = args.threads

        for channel_name in args.channels:
            clean_name = channel_name.lstrip("@")
            if analyze_channel_videos(clean_name, num_threads):
                successful_channels += 1

        print(
            f"Gemini analysis complete. Successfully analyzed {successful_channels}/{len(args.channels)} channels."
        )

    elif args.action == "cut":
        successful_channels = 0

        for channel_name in args.channels:
            clean_name = channel_name.lstrip("@")
            if cut_channel_videos(clean_name):
                successful_channels += 1

        print(
            f"Video cutting complete. Successfully cut {successful_channels}/{len(args.channels)} channels."
        )

    elif args.action == "postprocess":
        successful_channels = 0
        for channel_name in args.channels:
            clean_name = channel_name.lstrip("@")
            if post_process_channel(clean_name):
                successful_channels += 1

        print(
            f"Post-processing complete. Successfully post-processed {successful_channels}/{len(args.channels)} channels."
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)
