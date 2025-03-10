from pip._internal.commands import download
from pytubefix import YouTube
import os
import subprocess

base_url = "https://www.youtube.com/watch?v="
video_code = "7zp1TbLFPp8"
video_code = "YnopHCL1Jk8"
url = f"{base_url}{video_code}"

yt = YouTube(url)
video_title = yt.title.replace(" ", "_").replace("/", "_")

output_path = "/media/sf_debian/youtube"
tmp_path = f"{output_path}/tmp"

download = True

video_file = os.path.join(tmp_path, 'video.mp4')
audio_file = os.path.join(tmp_path, 'audio.m4a')

if download:
    if os.path.isfile(video_file):
        os.remove(video_file)

    if os.path.isfile(audio_file):
        os.remove(audio_file)

    video_stream = yt.streams.filter(adaptive=True, file_extension='mp4', only_video=True).order_by('resolution').desc().first()
    video_stream.download(output_path=tmp_path, filename='video.mp4')

    audio_stream = yt.streams.filter(only_audio=True).order_by('abr').desc().first()
    audio_stream.download(output_path=tmp_path, filename='audio.m4a')

try:
    merged_file = os.path.join(output_path, f"{video_title}.mp4")

    ffmpeg_command = [
        'ffmpeg', '-i', video_file, '-i', audio_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental',
        merged_file]
    subprocess.run(ffmpeg_command, check=True)
    print(f"Merging completed! File saved at: {merged_file}")
except subprocess.CalledProcessError as e:
    print(f"Merging failed, reason: {e}, changing file name to use url_ID")
    merged_file = os.path.join(output_path, f"{video_code}.mp4")
    ffmpeg_command[-1] = merged_file
    subprocess.run(ffmpeg_command, check=True)
    print(f"Merging completed! File saved at: {merged_file}")

if os.path.isfile(video_file):
    os.remove(video_file)

if os.path.isfile(audio_file):
    os.remove(audio_file)
