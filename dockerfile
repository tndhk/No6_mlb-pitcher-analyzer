FROM python:3.10-slim

# 環境変数設定
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VERSION=1.4.2

# タイムゾーン設定
ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 必要なパッケージインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Poetryインストール（依存関係管理用）
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# 作業ディレクトリ設定
WORKDIR /app

# Poetryの依存関係ファイルをコピー
COPY pyproject.toml poetry.lock* ./

# 依存関係インストール（開発用を含む）
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# アプリケーションコードをコピー
COPY . .

# デフォルトコマンド
CMD ["python", "-m", "mlb_pitcher_analyzer"]