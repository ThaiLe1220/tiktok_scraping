import os
import time
import json
import re
from google import genai
from google.genai import types, Client
from typing import List, Any, Optional
from tqdm import tqdm


def upload_video_and_poll(client: "Client", video_file_path: str) -> Optional[str]:
    """
    Uploads a video file and polls its status until it becomes ACTIVE.
    Returns the video file object if ACTIVE, or None if it fails.
    """
    video_file = client.files.upload(file=video_file_path)

    while video_file.state.name == "PROCESSING":
        time.sleep(1)
        video_file = client.files.get(name=video_file.name)

    if video_file.state.name == "ACTIVE":
        return video_file

    return None


def process_video(video_file_path: str):
    """
    Initializes the client, uploads the video, and generates content using the Gemini model.
    Returns the response from the content generation.
    """
    # Initialize the client with the API key from the environment
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

    # Upload the video file and wait until it is ACTIVE
    video_file = upload_video_and_poll(client, video_file_path)
    if video_file is None:
        return None

    try:
        # Define the prompt with watermark checking instructions
        prompt = (
            "Generate concise bullet points summarizing key scenes in a TikTok video featuring fusion of characters, animals, or objects. "
            "Each scene begins with two distinct entities shown together, followed by a fusion event creating a combined version of both entities. "
            "Provide timestamps as concise and accurate as possible since they will be used to precisely cut the video into multiple scenes. "
            "For each scene, clearly describe the entities involved and their fused result. Additionally, check if there is a watermark present in the scene. "
            "Separately, also check whether there are any other texts displayed on the screen, such as subtitles, captions, or interactive textboxes prompting actions like 'follow,' 'share,' 'subscribe,' 'like,' or 'comment.'"
            "Structure your response in JSON format as follows:\n\n"
            "{\n"
            '  "scenes": [\n'
            "    {\n"
            '      "text": "Brief description of the fusion scene.",\n'
            '      "time": "MM:SS.mmm",\n'
            '      "original_entities": ["Entity 1", "Entity 2"],\n'
            '      "fused_result": "Description of fused entity",\n'
            '      "watermark": "yes/no"\n'
            '      "other_texts": "yes/no"\n'
            "    },\n"
            "    {\n"
            '      "text": "Another fusion scene description.",\n'
            '      "time": "MM:SS.mmm",\n'
            '      "original_entities": ["Entity 1", "Entity 2"],\n'
            '      "fused_result": "Description of fused entity",\n'
            '      "watermark": "yes/no"\n'
            '      "other_texts": "yes/no"\n'
            "    }\n"
            "    // Add additional fusion scenes as needed\n"
            "  ]\n"
            "}"
        )

        # Build the content with both the video file and the text prompt
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_uri(
                        file_uri=video_file.uri,
                        mime_type=video_file.mime_type,
                    ),
                    types.Part.from_text(text=prompt),
                ],
            )
        ]

        # Set the generation configuration
        config = types.GenerateContentConfig(
            temperature=0.5,
            top_p=0.95,
            top_k=40,
            max_output_tokens=8192,
            response_mime_type="text/plain",
        )

        # Generate content using the Gemini model
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=contents,
            config=config,
        )

    finally:
        # Delete the uploaded video file regardless of the generation outcome
        try:
            deletion_response = client.files.delete(name=video_file.name)
        except Exception as e:
            pass

    return response


def process_response_from_generated_data(response):
    """
    Processes the raw response from the Gemini model to extract:
      - The JSON block with scenes from the candidate's text.
      - The prompt token count, candidates token count, and total token count.
      - Calculates the cost based on token usage (rounded to 4 decimal places).
    Returns a dictionary with these values.
    """
    # Extract the candidate text and parse the JSON block
    try:
        # Access the response correctly using dot notation
        candidate_text = response.candidates[0].content.parts[0].text
        match = re.search(r"```json\s*(.*?)\s*```", candidate_text, re.DOTALL)
        if not match:
            raise ValueError("JSON code block not found in candidate text.")
        json_str = match.group(1)
        scenes_data = json.loads(json_str)  # Expecting a dict with a "scenes" key
    except Exception as e:
        print(f"Error processing candidate text: {e}")
        scenes_data = {}

    # Accessing token usage directly from the response
    usage = response.usage_metadata
    prompt_tokens = usage.prompt_token_count or 0
    candidates_tokens = usage.candidates_token_count or 0
    total_tokens = usage.total_token_count or 0

    # Calculate costs
    cost_per_million_input = 0.10  # $0.10 per 1M input tokens
    cost_per_million_output = 0.40  # $0.40 per 1M output tokens

    input_cost = (prompt_tokens / 1_000_000) * cost_per_million_input
    output_cost = (candidates_tokens / 1_000_000) * cost_per_million_output
    total_cost = input_cost + output_cost

    rounded_input_cost = float(f"{input_cost:.5f}")
    rounded_output_cost = float(f"{output_cost:.5f}")
    rounded_total_cost = float(f"{total_cost:.4f}")

    result_data = {
        "scenes": scenes_data.get("scenes", []),
        "prompt_token_count": prompt_tokens,
        "candidates_token_count": candidates_tokens,
        "total_token_count": total_tokens,
        "cost": {
            "input_cost": rounded_input_cost,
            "output_cost": rounded_output_cost,
            "total_cost": rounded_total_cost,
        },
    }
    return result_data


def save_as_json(video_id: str, output: str, data: Any) -> None:
    """
    Saves data to JSON files.
    """
    with open(
        os.path.join(output, f"{video_id}-result.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2)


def gemini_process(json_output: str, videos: List[str]) -> None:
    """
    Processes videos using Gemini and saves the results as JSON.
    """
    os.makedirs(json_output, exist_ok=True)

    videos.sort()

    for video_file_path in tqdm(
        videos, total=len(videos), desc="Gemini Process", colour="green"
    ):
        video_name = os.path.splitext(os.path.basename(video_file_path))[0]

        # Skip if the result JSON already exists
        if os.path.exists(os.path.join(json_output, f"{video_name}-result.json")):
            print(f"Skipping {video_name} - result already exists")
            continue

        response = process_video(video_file_path)
        if response is None:
            print(f"Failed to process video: {video_file_path}")
            continue

        result = process_response_from_generated_data(response)
        save_as_json(video_name, json_output, result)
        # print(f"Processed {video_name} with {len(result.get('scenes', []))} scenes")
