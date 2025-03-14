FROM python:3.11-slim

WORKDIR /app

# M2 Mac向け最適化
ENV PYTHONUNBUFFERED=1

# 基本ツール
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 依存関係インストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 作業ディレクトリをマウントポイントとする
VOLUME /app

# Jupyter & Dashboard用ポート
EXPOSE 8888 8050

# 開発環境では常駐させる
CMD ["bash", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"]