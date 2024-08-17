import os
import requests
import subprocess
import musicbrainzngs
import discogs_client
import mutagen
from mutagen.easyid3 import EasyID3
from ollama import OllamaClient
import sqlite3
from rich import print
import pandas as pd

# Initialize musicbrainz and discogs clients
musicbrainzngs.set_useragent("MusicOrganizer", "1.0", "your-email@example.com")
discogs = discogs_client.Client("MusicOrganizer/1.0", user_token="your_discogs_token")

# Initialize Ollama client
ollama_client = OllamaClient("http://localhost:8000")

# SQLite database for progress tracking
conn = sqlite3.connect("music_organizer.db")
cursor = conn.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS progress
                  (file_path TEXT PRIMARY KEY, status TEXT, target_path TEXT)"""
)
conn.commit()


def scan_directory(input_dir):
    music_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith((".mp3", ".flac", ".aac", ".wav")):
                music_files.append(os.path.join(root, file))
    return music_files


def extract_metadata(file_path):
    try:
        audio = mutagen.File(file_path, easy=True)
        tags = {k: v[0] for k, v in audio.tags.items()} if audio and audio.tags else {}
        return tags
    except Exception as e:
        print(f"[red]Error extracting metadata from {file_path}: {e}[/red]")
        return None


def query_musicbrainz(tags):
    try:
        results = musicbrainzngs.search_recordings(
            artist=tags.get("artist", ""), recording=tags.get("title", "")
        )
        return results["recording-list"][0] if results["recording-list"] else None
    except Exception as e:
        print(f"[red]MusicBrainz query failed: {e}[/red]")
        return None


def query_discogs(tags):
    try:
        search_query = " ".join(
            [tags.get("artist", ""), tags.get("album", ""), tags.get("title", "")]
        )
        results = discogs.search(search_query, type="release")
        return results[0] if results else None
    except Exception as e:
        print(f"[red]Discogs query failed: {e}[/red]")
        return None


def validate_llm_response(response):
    # Check for hallucinations, inconsistent structures, or poor formatting in LLM response
    if (
        "artist" not in response
        or "album" not in response
        or "track_number" not in response
        or "track_name" not in response
    ):
        return False
    return True


def refine_llm_response(file_path, response):
    # If the response is invalid, re-prompt the LLM with additional context and corrective instructions
    prompt = (
        f"The response provided for the file '{file_path}' seems incomplete or incorrect. "
        "Please provide a well-structured and accurate output with the following details:\n"
        "- Artist name\n"
        "- Album name\n"
        "- Track number\n"
        "- Track name\n"
        "Ensure the data is consistent and does not contain fabricated information."
    )
    refined_response = ollama_client.request(prompt)
    return refined_response if validate_llm_response(refined_response) else None


def get_llm_suggestions(file_path, tags, db_results):
    # Generate a sophisticated prompt
    prompt = (
        f"We have a music file located at '{file_path}' with the following extracted metadata: {tags}. "
        "We also queried music databases (MusicBrainz and Discogs) and found the following results: {db_results}. "
        "Please help organize this file by confirming the correct information or suggesting better alternatives."
        "Please provide the following information in a clear, structured format:\n"
        "- Artist name\n"
        "- Album name\n"
        "- Track number\n"
        "- Track name\n"
        "Ensure that your response is well-structured, consistent, and avoids hallucinations or errors. "
        "If you are uncertain about any details, suggest the most plausible options based on the provided data."
    )
    response = ollama_client.request(prompt)

    if not validate_llm_response(response):
        response = refine_llm_response(file_path, response)

    return response


def generate_target_path(metadata):
    artist = metadata.get("artist", "Unknown Artist")
    album = metadata.get("album", "Unknown Album")
    track_number = metadata.get("track_number", "01")
    track_name = metadata.get("track_name", "Unknown Track")
    extension = metadata.get("extension", "mp3")

    target_path = os.path.join(
        artist, album, f"{artist} - {album} - {track_number} - {track_name}.{extension}"
    )
    return target_path


def process_file(file_path):
    cursor.execute(
        "SELECT status, target_path FROM progress WHERE file_path = ?", (file_path,)
    )
    result = cursor.fetchone()

    if result and result[0] == "completed":
        print(f"[yellow]File already processed: {file_path}[/yellow]")
        return

    tags = extract_metadata(file_path)
    if not tags:
        return

    musicbrainz_result = query_musicbrainz(tags)
    discogs_result = query_discogs(tags)

    db_results = {"musicbrainz": musicbrainz_result, "discogs": discogs_result}

    llm_suggestions = get_llm_suggestions(file_path, tags, db_results)

    target_path = generate_target_path(llm_suggestions)

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    os.rename(file_path, target_path)

    cursor.execute(
        "INSERT OR REPLACE INTO progress (file_path, status, target_path) VALUES (?, ?, ?)",
        (file_path, "completed", target_path),
    )
    conn.commit()


def main(input_dir, output_dir):
    music_files = scan_directory(input_dir)
    for file_path in music_files:
        process_file(file_path)

    print("[green]Music library organization completed![/green]")


if __name__ == "__main__":
    input_dir = "/path/to/your/input/music/library"
    output_dir = "/path/to/your/output/music/library"
    main(input_dir, output_dir)
