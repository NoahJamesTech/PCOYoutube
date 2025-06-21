HOST_IP=$(hostname -I | awk '{print $1}')
docker run --rm -it -p 500:500 -e HOST_IP=${HOST_IP} -v "$(pwd):/app" -w /app youtube-stream-creator