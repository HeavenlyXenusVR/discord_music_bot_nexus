FROM music-bot-base:latest

WORKDIR /app

# Copy bot source; secrets and runtime state stay in env files or mounted volumes.
COPY . .

# Start the bot
# CMD handled by compose
