HOST=$1
PORT=$((10#$2))

export LOCAL_HOST=$HOST
export LOCAL_PORT=$PORT + 1
echo $PORT + 1
export REDIS_HOST=$HOST
export REDIS_PORT=$PORT
