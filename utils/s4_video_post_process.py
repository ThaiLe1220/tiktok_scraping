import json
import glob
import os
import re
import pandas as pd
import shutil
from fuzzywuzzy import fuzz
from typing import Optional


def merge_final_json_files(
    pattern: str = os.path.join("static", "tiktok_json", "*-final.json")
) -> str:
    all_scenes = []
    json_files = glob.glob(pattern)

    for file_path in json_files:
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                if "scenes" in data and isinstance(data["scenes"], list):
                    all_scenes.extend(data["scenes"])
                    print(f"Added {len(data['scenes'])} scenes from {file_path}")
                else:
                    print(
                        f"Warning: {file_path} doesn't have the expected 'scenes' array"
                    )
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")

    # Sort scenes by name
    all_scenes.sort(key=lambda x: x.get("name", ""))

    final_data = {"scenes": all_scenes}
    output_path = "FUSION.json"
    with open(output_path, "w") as output_file:
        json.dump(final_data, output_file, indent=2)

    print(f"Successfully merged {len(all_scenes)} scenes into {output_path}")
    return output_path


class CharacterManager:
    def __init__(self):
        self.characters = {"characters": [], "appearances": []}

    def normalize_name(self, name: str) -> str:
        name = name.lower().strip()
        name = re.sub(
            r"[^\w]", "", name
        )  # Remove all non-alphanumeric chars including dashes
        return name

    def find_character_id(self, name: str) -> Optional[str]:
        normalized = self.normalize_name(name)

        for char in self.characters["characters"]:
            if not normalized in char["variations"]:
                continue
            if normalized != self.normalize_name(char["canonical_name"]):
                continue

            return char["id"]

        for char in self.characters["characters"]:
            for variation in char["variations"] + [char["canonical_name"]]:
                if fuzz.ratio(normalized, self.normalize_name(variation)) > 85:
                    return char["id"]
        return None

    def add_character(self, name: str) -> str:
        if char_id := self.find_character_id(name):
            return char_id

        new_id = f"{len(self.characters['characters'])+1:03d}"

        self.characters["characters"].append(
            {
                "id": new_id,
                "canonical_name": name,
                "variations": [self.normalize_name(name)],
            }
        )
        return new_id

    def add_appearance(self, character_name: str, video_name: str) -> None:
        char_id = self.add_character(character_name)

        for app in self.characters["appearances"]:
            if app["character_id"] == char_id and app["video_name"] == video_name:
                return

        self.characters["appearances"].append(
            {"character_id": char_id, "video_name": video_name}
        )

    def process_video_json(self, json_path: str) -> None:
        with open(json_path, "r") as f:
            data = json.load(f)

        for scene in data.get("scenes", []):
            video_name = scene.get("name", "")
            for entity in scene.get("original_entities", []):
                self.add_appearance(entity, video_name)

    def generate_table(self) -> None:
        rows = []
        for char in self.characters["characters"]:
            videos = [
                app["video_name"]
                for app in self.characters["appearances"]
                if app["character_id"] == char["id"]
            ]
            rows.append(
                {
                    "ID": char["id"],
                    "NAME": self.normalize_name(char["canonical_name"]),
                    "VIDEO": ", ".join(videos),
                }
            )
        df = pd.DataFrame(rows)
        df.to_csv("Characters.csv", index=False)
        print(f"Character table saved to Characters.csv")


def update_character_database(new_fusion_json_path: str) -> None:
    """
    Update character database with new fusion data without changing existing character IDs.
    Creates a new file CHARACTERS_UPDATED.csv instead of modifying the original.

    Args:
        new_fusion_json_path: Path to the new fusion JSON file

    Returns:
        DataFrame containing updated character table
    """
    # Initialize manager
    manager = CharacterManager()

    # Check if CHARACTERS.csv exists
    if os.path.exists("CHARACTERS.csv"):
        # Load existing data
        existing_characters = pd.read_csv("CHARACTERS.csv")

        # Initialize with existing data
        for _, row in existing_characters.iterrows():
            char_id = row["ID"]
            name = row["NAME"]
            videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []

            manager.characters["characters"].append(
                {"id": char_id, "canonical_name": name, "variations": [name]}
            )

            for video in videos:
                manager.characters["appearances"].append(
                    {"character_id": char_id, "video_name": video}
                )

    # Process new fusion data
    manager.process_video_json(new_fusion_json_path)

    # Generate updated table to a new file
    manager.generate_table()


def rename_and_copy_videos(character_csv_path: str = "CHARACTERS.csv") -> None:
    """
    Rename and copy videos based on character pairings, ensuring IDs use 3-digit format.
    Also creates a CHARACTERS_FINAL.csv with updated video names.
    Skips copying if destination file already exists.
    """

    # Load character data
    characters_df = pd.read_csv(character_csv_path)

    # Create FUSION directory
    fusion_dir = "FUSION"
    os.makedirs(fusion_dir, exist_ok=True)

    # Build video to characters mapping
    video_to_chars = {}

    for _, row in characters_df.iterrows():
        char_id = row["ID"]
        videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []

        for video in videos:
            if video in video_to_chars:
                video_to_chars[video].append(char_id)
            else:
                video_to_chars[video] = [char_id]

    # Process videos that have exactly 2 characters
    renamed_videos = {}

    for video, char_ids in video_to_chars.items():
        if len(char_ids) != 2:
            continue

        # Sort IDs to ensure smaller ID comes first
        char_ids.sort()
        new_filename = f"{char_ids[0]}_{char_ids[1]}.mp4"
        new_path = os.path.join(fusion_dir, new_filename)
        renamed_videos[video] = new_path

        # Skip if destination file already exists
        if os.path.exists(new_path):
            continue

        # Actually copy the file
        try:
            shutil.copy2(video, new_path)
            print(f"Copied {video} to {new_path}")
        except Exception as e:
            print(f"Error copying {video}: {str(e)}")

    # Save the mapping
    with open(os.path.join(fusion_dir, "MAPPING.json"), "w", encoding="utf-8") as f:
        json.dump(renamed_videos, f, indent=2)

    # Create CHARACTERS_FINAL.csv with updated video names
    final_df = characters_df.copy()
    final_df["NEW_VIDEO"] = ""

    # Map old video paths to new ones
    for _, row in final_df.iterrows():
        char_id = row["ID"]
        videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []
        new_videos = []

        for video in videos:
            if video in renamed_videos:
                # Get just the filename, not the full path
                new_name = os.path.basename(renamed_videos[video])
                new_videos.append(new_name)
            else:
                # Keep original if not renamed
                new_videos.append(os.path.basename(video))

        final_df.loc[final_df["ID"] == char_id, "NEW_VIDEO"] = (
            ", ".join(new_videos) if new_videos else ""
        )

    # Save the updated CSV
    final_df.to_csv("CHARACTERS_FINAL.csv", index=False)
    print("Updated character data saved to CHARACTERS_FINAL.csv")


def post_process(pattern: str):
    fusion_path = merge_final_json_files(pattern)
    manager = CharacterManager()
    manager.process_video_json(fusion_path)
    update_character_database(fusion_path)
    rename_and_copy_videos()
