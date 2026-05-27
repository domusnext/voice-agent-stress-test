#!/usr/bin/env python3
"""gen_audio.py — 生成压测用 WAV 音频文件"""

import asyncio
import os
import subprocess
import tempfile

# pip install edge-tts
import edge_tts

QUESTIONS = [
    ("question_greeting.wav", "Hey Nori, what do we have planned for the weekend?"),
    # ("question_greeting.wav", "Hey, um, so I was just thinking about this earlier today when I was talking to my friend Sarah—you know, the one who lives in Boston and works at that tech startup? Anyway, we were having coffee and she mentioned something about AI assistants, which reminded me that I've been meaning to ask someone about this. So, like, I know this might sound like a basic question, and honestly I'm not even sure if this is the right way to phrase it, but I've always been curious and never really got a straight answer... so, who are you?"),
]

VOICE = "zh-CN-XiaoxiaoNeural"
OUTPUT_DIR = "audio"

# 语音结束后追加的静音时长（秒），确保服务端 VAD 自然过渡到 QUIET
SILENCE_PADDING_SECS = 10


async def generate_one(filename: str, text: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    mp3_path = os.path.join(OUTPUT_DIR, filename.replace(".wav", ".mp3"))
    wav_path = os.path.join(OUTPUT_DIR, filename)

    communicate = edge_tts.Communicate(text, VOICE)
    await communicate.save(mp3_path)

    # 转换为 16kHz 16-bit mono WAV，并在末尾追加静音
    # 使用 ffmpeg 的 apad 滤镜追加指定时长的静音
    pad_samples = int(16000 * SILENCE_PADDING_SECS)
    subprocess.run(
        [
            "ffmpeg", "-y", "-i", mp3_path,
            "-af", f"apad=pad_len={pad_samples}",
            "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
            wav_path,
        ],
        check=True,
        capture_output=True,
    )
    os.remove(mp3_path)
    print(f"Generated: {wav_path} ({os.path.getsize(wav_path)} bytes, +{SILENCE_PADDING_SECS}s silence)")


async def main():
    for filename, text in QUESTIONS:
        await generate_one(filename, text)
    print(f"\nAll audio files generated in {OUTPUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
