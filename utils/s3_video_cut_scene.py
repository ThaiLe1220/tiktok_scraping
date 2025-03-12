import os
import json
import glob
from tqdm import tqdm
from typing import List, Dict, Any, Tuple
from moviepy.editor import VideoFileClip


# Existing functions remain unchanged
def time_str_to_seconds(time_str):
    """Converts a time string in MM:SS format to seconds."""
    parts = time_str.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time format: {time_str}")
    minutes, seconds_part = parts

    # Check if seconds part contains milliseconds
    if "." in seconds_part:
        seconds, milliseconds = seconds_part.split(".")
        return int(minutes) * 60 + int(seconds) + float(f"0.{milliseconds}")
    else:
        return int(minutes) * 60 + int(seconds)


def get_effective_start(scene, idx):
    """
    Returns the effective start time for a scene.
    For the first scene, no modification is applied.
    For subsequent scenes, add 1.5 seconds.
    """
    raw_time = time_str_to_seconds(scene["time"])
    if idx == 0:
        return raw_time
    else:
        return raw_time + 0.5


def cut_video_scenes(video_file_path: str, output: str, scenes: List[Dict[str, Any]]):
    """Cuts the video into scenes based on adjusted start times."""
    # Existing implementation remains the same
    video_name = os.path.splitext(os.path.basename(video_file_path))[0]
    clip = VideoFileClip(video_file_path)
    video_duration = clip.duration

    # Create the "invalid" subdirectory within the output directory
    invalid_dir = os.path.join(output, "invalid")
    os.makedirs(invalid_dir, exist_ok=True)

    final_scene_data = []

    # Compute effective start times for all scenes
    # Skip applying offsets for videos shorter than 30 seconds
    if video_duration < 30:
        effective_starts = [time_str_to_seconds(scene["time"]) for scene in scenes]
    else:
        effective_starts = [
            get_effective_start(scene, idx) for idx, scene in enumerate(scenes)
        ]

    for idx, scene in enumerate(scenes):
        try:
            start_time = effective_starts[idx]

            if idx < len(scenes) - 1:
                end_time = effective_starts[idx + 1] - 1
            else:
                end_time = video_duration

            scene_duration = end_time - start_time

            # Define output file name e.g., "1-1.mp4", "1-2.mp4", etc.
            output_filename = f"{video_name}-{idx+1}.mp4"

            # Determine output directory based on other_texts field
            if scene.get("other_texts", "") == "yes":
                # Save to the "invalid" subdirectory
                output_dir = invalid_dir
            else:
                # Save to the regular output directory
                output_dir = output

            output_filepath = os.path.join(output_dir, output_filename)

            # Extract subclip and write the video file
            scene_clip = clip.subclip(start_time, end_time)
            scene_clip.write_videofile(
                output_filepath, codec="libx264", audio_codec="aac", logger=None
            )

            # Build the scene object for the final JSON
            scene_info = {
                "name": output_filepath,
                "original_entities": scene.get("original_entities", []),
                "total time": scene_duration,
                "description": scene.get("text", ""),
                "watermark": scene.get("watermark", ""),
                "other_texts": scene.get("other_texts", ""),
            }
            final_scene_data.append(scene_info)
        except Exception as e:
            print(f"Error processing scene {idx+1}: {e}")
            continue

    clip.close()
    return final_scene_data


# New function to process a single video
def process_single_video(
    json_file: str, vid_dir: str, src_dir: str = None
) -> Tuple[bool, str]:
    """Process a single video file based on its JSON data."""
    # Extract video name from the JSON filename
    video_name = os.path.basename(json_file).replace("-result.json", "")
    json_dir = os.path.dirname(json_file)

    try:
        # Determine the video file path
        if src_dir:
            # Check for multiple possible extensions
            for ext in [".mp4", ".mov", ".avi", ".webm"]:
                video_path = os.path.join(src_dir, f"{video_name}{ext}")
                if os.path.exists(video_path):
                    break
            else:
                return False, f"Could not find video file for {video_name}"
        else:
            # Look for the video in the same directory as the JSON
            base_dir = os.path.dirname(json_file)
            for ext in [".mp4", ".mov", ".avi", ".webm"]:
                video_path = os.path.join(base_dir, f"{video_name}{ext}")
                if os.path.exists(video_path):
                    break
            else:
                return False, f"Could not find video file for {video_name}"

        # Check if the video has already been cut
        pattern_regular = os.path.join(vid_dir, f"{video_name}-*.mp4")
        pattern_invalid = os.path.join(vid_dir, "invalid", f"{video_name}-*.mp4")
        existing_cuts = glob.glob(pattern_regular) + glob.glob(pattern_invalid)

        # Load the JSON data
        with open(json_file, "r", encoding="utf-8") as f:
            result_data = json.load(f)

        scenes = result_data.get("scenes", [])

        # Skip if already processed
        if len(existing_cuts) == len(scenes):
            return True, f"Skipping {video_name} - all {len(scenes)} scenes already cut"

        # Cut the video
        final_scenes = cut_video_scenes(video_path, vid_dir, scenes)

        # Create final directory if it doesn't exist
        final_dir = os.path.join(os.path.dirname(json_dir), "final")
        os.makedirs(final_dir, exist_ok=True)

        # Save only to the new final JSON file
        final_json_path = os.path.join(final_dir, f"{video_name}-final.json")
        with open(final_json_path, "w", encoding="utf-8") as f:
            json.dump({"scenes": final_scenes}, f, indent=2)

        return True, f"Successfully processed {video_name}"
    except Exception as e:
        return False, f"Error processing {video_name}: {str(e)}"


def process_video_cuts(json_dir: str, vid_dir: str, src_dir: str = None) -> None:
    """
    Process video cuts based on JSON files.
    If src_dir is None, assumes video files are in the same directory as JSON files.
    """
    os.makedirs(vid_dir, exist_ok=True)
    os.makedirs(os.path.join(vid_dir, "invalid"), exist_ok=True)

    # Get all result JSON files
    json_files = sorted(glob.glob(os.path.join(json_dir, "*-result.json")))

    print(f"Found {len(json_files)} videos to process")

    # Process each video sequentially
    for json_file in tqdm(json_files, desc="Processing videos", colour="blue"):
        success, message = process_single_video(json_file, vid_dir, src_dir)
        print(message)
