# MLB投手分析ツール

MLBの投手データを収集、分析、可視化するためのツールです。

## 概要

このツールは以下の機能を提供します：

- **データ収集**：MLB Stats APIや他のソースからデータを収集します
- **データ保存**：収集したデータをSQLiteデータベースに保存します
- **データ分析**：投手のパフォーマンス、投球スタイル、傾向などを分析します
- **可視化**：データをグラフやダッシュボードで視覚化します（フェーズ4）

## インストール

### 要件

- Python 3.8+
- pip

### インストール手順

1. リポジトリをクローンします

```bash
git clone https://github.com/yourusername/mlb-pitcher-analyzer.git
cd mlb-pitcher-analyzer
```

2. 依存パッケージをインストールします

```bash
pip install -r requirements.txt
```

3. ローカルパッケージとしてインストールします

```bash
pip install -e .
```

## 使い方

### データベースの初期化

```bash
mlb-pitcher-analyzer init
```

### チームデータの取得

```bash
mlb-pitcher-analyzer get-teams --season 2023
```

### 投手データの取得

```bash
mlb-pitcher-analyzer get-pitchers --season 2023
```

### 選手の検索

```bash
mlb-pitcher-analyzer search-player "Ohtani"
```

### 投手の成績取得

```bash
mlb-pitcher-analyzer get-stats 660271 --season 2023
```

### StatcastデータのCSVインポート

```bash
mlb-pitcher-analyzer import-statcast path/to/statcast.csv --process
```

### 球種構成の分析

```bash
mlb-pitcher-analyzer analyze-pitch-mix 660271 --season 2023
```

### 最近の投手成績取得

```bash
mlb-pitcher-analyzer get-recent --days 7
```

### データベースのバックアップ

```bash
mlb-pitcher-analyzer backup --tag "pre_update"
```

## プロジェクト構造

```
mlb-pitcher-analyzer/
├── data/                  # データディレクトリ
│   ├── raw/               # 生データファイル
│   ├── processed/         # 処理済みデータファイル
│   └── backups/           # バックアップファイル
├── docs/                  # ドキュメント
├── src/                   # ソースコード
│   ├── data_collection/   # データ収集モジュール
│   │   ├── api/           # API通信
│   │   ├── parsers/       # データパース
│   │   └── storage/       # データ保存
│   ├── analysis/          # データ分析モジュール（フェーズ3）
│   ├── visualization/     # データ可視化モジュール（フェーズ4）
│   └── utils/             # ユーティリティモジュール
└── tests/                 # テストコード
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 注意事項

このツールはMLBのデータに依存しています。MLBデータの使用に関しては、MLBの利用規約を遵守してください。このツールは非商用目的での使用を想定しています。