#!/bin/sh
# 会員一括管理システム 起動スクリプト
# 使い方: ./start.sh   （PORTなどを変えたい場合は .env.example を参照して環境変数で指定）
cd "$(dirname "$0")"
PORT="${PORT:-3000}"
HOST="${HOST:-0.0.0.0}"
export PORT HOST
echo "起動します: http://localhost:${PORT}"
exec node server.js
