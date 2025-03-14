# Python 3.9をベースイメージとして使用（ARMアーキテクチャ対応）
FROM python:3.10-slim

# 作業ディレクトリを設定
WORKDIR /app

# タイムゾーンを設定
ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 必要なパッケージをインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    sqlite3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 仮想環境の利用を避け、システム全体にパッケージをインストール
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションのソースコードをコピー
COPY . /app/

# 開発用にアプリケーションをインストール
RUN pip install -e .

# コマンドラインツールへのシンボリックリンクを作成
RUN ln -sf $(which mlb-pitcher-analyzer) /usr/local/bin/mlb-pitcher-analyzer || \
    (echo '#!/bin/bash\npython -m src.data_collection.cli "$@"' > /usr/local/bin/mlb-pitcher-analyzer && \
    chmod +x /usr/local/bin/mlb-pitcher-analyzer)

# 環境変数の設定
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# データディレクトリの作成と権限の設定
RUN mkdir -p /app/data/raw /app/data/processed /app/data/backups /app/logs \
    && chmod -R 777 /app/data /app/logs

# コマンドラインツールを実行可能に
RUN chmod +x /app/src/data_collection/cli.py

# デフォルトのコマンド
CMD ["bash"]