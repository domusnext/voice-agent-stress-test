#!/usr/bin/env python3
"""gen_audio.py — 生成压测用 WAV 音频文件"""

import asyncio
import os
import subprocess
import tempfile

# pip install edge-tts
import edge_tts

QUESTIONS = [
    ("question_greeting.wav", "Hello, who are you?"),
]

VOICE = "zh-CN-XiaoxiaoNeural"
OUTPUT_DIR = "audio"


async def generate_one(filename: str, text: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    mp3_path = os.path.join(OUTPUT_DIR, filename.replace(".wav", ".mp3"))
    wav_path = os.path.join(OUTPUT_DIR, filename)

    communicate = edge_tts.Communicate(text, VOICE)
    await communicate.save(mp3_path)

    # 转换为 16kHz 16-bit mono WAV（Daily.co 兼容格式）
    subprocess.run(
        [
            "ffmpeg", "-y", "-i", mp3_path,
            "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
            wav_path,
        ],
        check=True,
        capture_output=True,
    )
    os.remove(mp3_path)
    print(f"Generated: {wav_path} ({os.path.getsize(wav_path)} bytes)")


async def main():
    for filename, text in QUESTIONS:
        await generate_one(filename, text)
    print(f"\nAll audio files generated in {OUTPUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
