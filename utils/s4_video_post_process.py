import json
import glob
import os
import re
import pandas as pd
import shutil
from fuzzywuzzy import fuzz
from typing import Optional, List, Dict, Any
from collections import defaultdict
from pathlib import Path


def load_json(file_path: str) -> Dict[str, Any]:
    """Load and parse a JSON file with error handling."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading {file_path}: {str(e)}")
        return {}


def save_json(data: Dict[str, Any], file_path: str) -> None:
    """Save data to a JSON file with error handling."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        print(f"Successfully saved data to {file_path}")
    except Exception as e:
        print(f"Error saving to {file_path}: {str(e)}")


def merge_final_json_files(channel_name: str) -> str:
    """
    Merge all final JSON files for a channel, filtering out scenes with missing videos.

    Args:
        channel_name: Name of the channel

    Returns:
        Path to the merged output JSON file
    """
    all_scenes = []
    channel_dir = Path("static") / channel_name
    json_pattern = channel_dir / "final" / "*-final.json"
    json_files = glob.glob(str(json_pattern))
    videos_dir = channel_dir / "video"

    if not videos_dir.exists():
        print(f"Warning: {videos_dir} directory doesn't exist")

    for file_path in json_files:
        data = load_json(file_path)
        if not data or "scenes" not in data or not isinstance(data["scenes"], list):
            print(f"Warning: {file_path} doesn't have the expected 'scenes' array")
            continue

        valid_scenes = 0
        for scene in data["scenes"]:
            video_filename = os.path.basename(scene.get("name", ""))
            video_path = videos_dir / video_filename

            if video_path.exists():
                all_scenes.append(scene)
                valid_scenes += 1
            else:
                print(f"Skipping scene with missing video: {video_filename}")

        print(f"Added {valid_scenes} scenes from {file_path}")

    # Sort scenes by name
    all_scenes.sort(key=lambda x: x.get("name", ""))

    output_path = f"{channel_name}.json"
    save_json({"scenes": all_scenes}, output_path)
    print(f"Successfully merged {len(all_scenes)} scenes into {output_path}")

    return output_path


class CharacterManager:
    def __init__(self):
        self.characters = {"characters": [], "appearances": []}

    def normalize_name(self, name: str) -> str:
        """Normalize a character name by removing non-alphanumeric characters and lowercasing."""
        return re.sub(r"[^\w]", "", name.lower().strip())

    def find_character_id(self, name: str) -> Optional[str]:
        """Find a character ID by name, using exact match or fuzzy matching."""
        normalized = self.normalize_name(name)

        # Try exact match first
        for char in self.characters["characters"]:
            if normalized in char["variations"] and normalized == self.normalize_name(
                char["canonical_name"]
            ):
                return char["id"]

        # Fall back to fuzzy matching
        for char in self.characters["characters"]:
            for variation in char["variations"] + [char["canonical_name"]]:
                if fuzz.ratio(normalized, self.normalize_name(variation)) > 60:
                    return char["id"]

        return None

    def add_character(self, name: str) -> str:
        """Add a character if not already exists, return the character ID."""
        char_id = self.find_character_id(name)
        if char_id:
            return char_id

        # Ensure ID is a 3-digit number (001-999)
        new_id_num = len(self.characters["characters"]) + 1
        if new_id_num > 999:
            raise ValueError("Cannot create more than 999 character IDs")

        new_id = f"{new_id_num:03d}"
        self.characters["characters"].append(
            {
                "id": new_id,
                "canonical_name": name,
                "variations": [self.normalize_name(name)],
            }
        )

        return new_id

    def add_appearance(self, character_name: str, video_name: str) -> None:
        """Add a character appearance in a video if not already exists."""
        char_id = self.add_character(character_name)

        # Check if this appearance already exists
        for app in self.characters["appearances"]:
            if app["character_id"] == char_id and app["video_name"] == video_name:
                return

        self.characters["appearances"].append(
            {"character_id": char_id, "video_name": video_name}
        )

    def process_video_json(self, json_path: str) -> None:
        """Process a video JSON file to extract character appearances."""
        data = load_json(json_path)

        for scene in data.get("scenes", []):
            video_name = scene.get("name", "")
            for entity in scene.get("original_entities", []):
                self.add_appearance(entity, video_name)

    def get_character_videos(self, char_id: str) -> List[str]:
        """Get all videos for a specific character ID."""
        return [
            app["video_name"]
            for app in self.characters["appearances"]
            if app["character_id"] == char_id
        ]

    def generate_table(self, output_path: str = "CHARACTERS.csv") -> None:
        """Generate a CSV table of characters and their appearances."""
        rows = []
        for char in self.characters["characters"]:
            videos = self.get_character_videos(char["id"])
            rows.append(
                {
                    "ID": char["id"],
                    "NAME": self.normalize_name(char["canonical_name"]),
                    "VIDEO": ", ".join(videos),
                }
            )

        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        print(f"Character table saved to {output_path}")


def load_existing_characters(manager: CharacterManager, csv_path: str) -> None:
    """Load existing character data from a CSV file into a CharacterManager."""
    if not os.path.exists(csv_path):
        return

    existing_characters = pd.read_csv(csv_path)

    for _, row in existing_characters.iterrows():
        char_id = row["ID"]
        name = row["NAME"]
        videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []

        # Add character to manager
        manager.characters["characters"].append(
            {"id": char_id, "canonical_name": name, "variations": [name]}
        )

        # Add all appearances
        for video in videos:
            manager.characters["appearances"].append(
                {"character_id": char_id, "video_name": video}
            )


def update_character_database(fusion_json_path: str) -> None:
    """
    Update character database with new fusion data without changing existing character IDs.
    Creates a new file CHARACTERS.csv.

    Args:
        fusion_json_path: Path to the fusion JSON file
    """
    manager = CharacterManager()

    # Load existing data if available
    if os.path.exists("CHARACTERS.csv"):
        load_existing_characters(manager, "CHARACTERS.csv")

    # Process new fusion data
    manager.process_video_json(fusion_json_path)

    # Generate updated table
    manager.generate_table()


def copy_file_if_not_exists(source: str, destination: str) -> bool:
    """Copy a file if the destination doesn't exist yet."""
    if os.path.exists(destination):
        return False

    try:
        shutil.copy2(source, destination)
        print(f"Copied {source} to {destination}")
        return True
    except Exception as e:
        print(f"Error copying {source}: {str(e)}")
        return False


def rename_and_copy_videos(
    character_csv_path: str = "CHARACTERS.csv", channel_name: str = ""
) -> None:
    """
    Rename and copy videos based on character pairings, using 3-digit format for IDs.
    Creates CHARACTERS_FINAL.csv with updated video names.

    Args:
        character_csv_path: Path to the character CSV file
        channel_name: Name of the channel
    """
    # Load character data
    characters_df = pd.read_csv(character_csv_path)

    # Create fusion directory
    fusion_dir = Path("static") / channel_name / "fusion"
    fusion_dir.mkdir(exist_ok=True)

    # Build video to characters mapping
    video_to_chars = defaultdict(list)

    for _, row in characters_df.iterrows():
        char_id = row["ID"]
        videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []

        for video in videos:
            video_to_chars[video].append(char_id)

    # Process videos with exactly 2 characters
    renamed_videos = {}

    for video, char_ids in video_to_chars.items():
        if len(char_ids) != 2:
            continue

        # Sort IDs to ensure smaller ID comes first
        # Ensure all IDs are 3-digit format (e.g., "1" becomes "001")
        formatted_ids = [f"{int(id_str):03d}" for id_str in char_ids]
        formatted_ids.sort()
        new_filename = f"{formatted_ids[0]}_{formatted_ids[1]}.mp4"
        new_path = fusion_dir / new_filename
        renamed_videos[video] = str(new_path)

        # Copy the file if it doesn't exist
        copy_file_if_not_exists(video, str(new_path))

    # Save the mapping
    mapping_path = fusion_dir / "MAPPING.json"
    save_json(renamed_videos, str(mapping_path))

    # Create final CSV with only ID, NAME, and NEW_VIDEO columns
    final_df = pd.DataFrame(
        {"ID": characters_df["ID"], "NAME": characters_df["NAME"], "NEW_VIDEO": ""}
    )

    # Map old video paths to new ones
    for idx, row in characters_df.iterrows():
        char_id = row["ID"]
        videos = row["VIDEO"].split(", ") if isinstance(row["VIDEO"], str) else []
        new_videos = []

        for video in videos:
            if video in renamed_videos:
                new_videos.append(os.path.basename(renamed_videos[video]))
            else:
                new_videos.append(os.path.basename(video))

        final_df.at[idx, "NEW_VIDEO"] = ", ".join(new_videos) if new_videos else ""

    # Save the updated CSV
    final_df.to_csv("CHARACTERS_FINAL.csv", index=False)
    print("Updated character data saved to CHARACTERS_FINAL.csv")


def post_process(channel_name: str) -> None:
    """
    Main processing function that orchestrates the entire workflow.

    Args:
        channel_name: Name of the channel to process
    """
    fusion_path = merge_final_json_files(channel_name)
    update_character_database(fusion_path)
    rename_and_copy_videos(channel_name=channel_name)
