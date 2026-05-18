FROM python:3.11-slim

# Install system dependencies (FFmpeg is required to stream audio to Discord)
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /app/logs /app/.runtime /app/.cache/yt-dlp

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source; secrets and runtime state stay in env files or mounted volumes.
COPY . .

# Start the bot
# CMD handled by compose
