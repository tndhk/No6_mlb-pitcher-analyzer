"""
MLB投手データ分析用のデータスキーマ定義

このモジュールでは、MLB投手データを保存・管理するためのデータモデルを定義します。
データベース構造とPythonオブジェクトのマッピングを提供します。
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Union
from enum import Enum


class PitchType(str, Enum):
    """投球の種類を定義する列挙型"""
    FOUR_SEAM_FASTBALL = "FF"  # フォーシームファストボール
    TWO_SEAM_FASTBALL = "FT"   # ツーシームファストボール
    CUTTER = "FC"              # カッター
    SLIDER = "SL"              # スライダー
    CURVEBALL = "CU"           # カーブボール
    CHANGEUP = "CH"            # チェンジアップ
    SPLITTER = "FS"            # スプリッター
    SINKER = "SI"              # シンカー
    KNUCKLEBALL = "KN"         # ナックルボール
    EEPHUS = "EP"              # イーファス
    SCREWBALL = "SC"           # スクリューボール
    SWEEPER = "SW"             # スウィーパー
    UNKNOWN = "UN"             # 不明


class GameType(str, Enum):
    """試合の種類を定義する列挙型"""
    REGULAR_SEASON = "R"       # レギュラーシーズン
    POSTSEASON = "P"           # ポストシーズン
    SPRING_TRAINING = "S"      # スプリングトレーニング
    ALL_STAR = "A"             # オールスター
    EXHIBITION = "E"           # エキシビション


@dataclass
class Player:
    """選手情報を格納するクラス"""
    id: int                     # 選手ID（MLB API準拠）
    full_name: str              # フルネーム
    first_name: str             # ファーストネーム
    last_name: str              # ラストネーム
    primary_number: Optional[str] = None  # 背番号
    birth_date: Optional[datetime] = None  # 生年月日
    throws: Optional[str] = None  # 投球腕（R:右, L:左）
    height_feet: Optional[int] = None  # 身長（フィート）
    height_inches: Optional[int] = None  # 身長（インチ）
    weight: Optional[int] = None  # 体重（ポンド）
    mlb_debut_date: Optional[datetime] = None  # MLBデビュー日
    active: bool = True  # 現役かどうか
    primary_position: Optional[str] = None  # 主なポジション
    team_id: Optional[int] = None  # 所属チームID
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class Team:
    """チーム情報を格納するクラス"""
    id: int                     # チームID（MLB API準拠）
    name: str                   # チーム名
    abbreviation: str           # チーム略称
    team_code: str              # チームコード
    league_id: int              # リーグID
    division_id: int            # ディビジョンID
    venue_id: Optional[int] = None  # 本拠地ID
    city: Optional[str] = None  # 所在都市
    active: bool = True         # アクティブかどうか
    first_year_of_play: Optional[str] = None  # 創設年
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class SeasonPitchingStats:
    """シーズンの投手成績を格納するクラス"""
    player_id: int              # 選手ID
    season: int                 # シーズン年度
    team_id: int                # チームID
    games_played: int = 0       # 試合数
    games_started: int = 0      # 先発試合数
    wins: int = 0               # 勝利数
    losses: int = 0             # 敗戦数
    saves: int = 0              # セーブ数
    save_opportunities: int = 0  # セーブ機会
    holds: int = 0              # ホールド数
    blown_saves: int = 0        # ブローセーブ数
    innings_pitched: float = 0.0  # 投球回数
    hits: int = 0               # 被安打数
    runs: int = 0               # 失点
    earned_runs: int = 0        # 自責点
    home_runs: int = 0          # 被本塁打数
    strike_outs: int = 0        # 奪三振数
    base_on_balls: int = 0      # 与四球数
    intentional_walks: int = 0  # 敬遠数
    hit_batsmen: int = 0        # 死球数
    wild_pitches: int = 0       # 暴投数
    balks: int = 0              # ボーク数
    ground_outs: int = 0        # ゴロアウト数
    fly_outs: int = 0           # フライアウト数
    complete_games: int = 0     # 完投数
    shutouts: int = 0           # 完封数
    batters_faced: int = 0      # 対戦打者数
    pitches_thrown: int = 0     # 投球数
    strikes: int = 0            # ストライク数
    # 計算済み指標
    era: float = 0.0            # 防御率
    whip: float = 0.0           # WHIP
    avg: float = 0.0            # 被打率
    game_type: GameType = GameType.REGULAR_SEASON  # 試合種別
    # 先進指標
    fip: Optional[float] = None  # FIP
    xfip: Optional[float] = None  # xFIP
    babip: Optional[float] = None  # BABIP
    lob_percentage: Optional[float] = None  # LOB%
    k_per_9: Optional[float] = None  # K/9
    bb_per_9: Optional[float] = None  # BB/9
    hr_per_9: Optional[float] = None  # HR/9
    k_percentage: Optional[float] = None  # K%
    bb_percentage: Optional[float] = None  # BB%
    k_minus_bb: Optional[float] = None  # K-BB%
    ground_ball_percentage: Optional[float] = None  # GB%
    fly_ball_percentage: Optional[float] = None  # FB%
    line_drive_percentage: Optional[float] = None  # LD%
    soft_hit_percentage: Optional[float] = None  # Soft%
    medium_hit_percentage: Optional[float] = None  # Medium%
    hard_hit_percentage: Optional[float] = None  # Hard%
    war: Optional[float] = None  # WAR
    # データ管理用
    source: str = "mlb_api"     # データソース
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class GamePitchingStats:
    """1試合の投手成績を格納するクラス"""
    player_id: int              # 選手ID
    game_id: int                # 試合ID
    game_date: datetime         # 試合日
    team_id: int                # チームID
    opponent_id: int            # 対戦チームID
    home_away: str              # ホーム/アウェイ ('H'/'A')
    innings_pitched: float = 0.0  # 投球回数
    hits: int = 0               # 被安打数
    runs: int = 0               # 失点
    earned_runs: int = 0        # 自責点
    home_runs: int = 0          # 被本塁打数
    strike_outs: int = 0        # 奪三振数
    base_on_balls: int = 0      # 与四球数
    win: bool = False           # 勝利
    loss: bool = False          # 敗戦
    save: bool = False          # セーブ
    hold: bool = False          # ホールド
    blown_save: bool = False    # ブローセーブ
    pitches_thrown: int = 0     # 投球数
    strikes: int = 0            # ストライク数
    batters_faced: int = 0      # 対戦打者数
    game_score: Optional[int] = None  # ゲームスコア（Bill James方式）
    # 試合詳細
    season: int = 0             # シーズン年度
    game_type: GameType = GameType.REGULAR_SEASON  # 試合種別
    # データ管理用
    source: str = "mlb_api"     # データソース
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class Pitch:
    """個々の投球データを格納するクラス"""
    id: str                     # 投球ID
    player_id: int              # 投手ID
    game_id: int                # 試合ID
    at_bat_id: int              # 打席ID
    pitch_number: int           # 打席内の投球数
    pitch_type: PitchType       # 球種
    start_speed: Optional[float] = None  # 初速（mph）
    end_speed: Optional[float] = None  # 終速（mph）
    spin_rate: Optional[float] = None  # 回転数（rpm）
    spin_direction: Optional[float] = None  # 回転方向（度）
    break_angle: Optional[float] = None  # 変化角度
    break_length: Optional[float] = None  # 変化量
    break_y: Optional[float] = None  # 縦変化量
    zone: Optional[int] = None  # ストライクゾーン（1-9）
    # 投球位置データ
    plate_x: Optional[float] = None  # 水平位置（インチ）
    plate_z: Optional[float] = None  # 垂直位置（インチ）
    px: Optional[float] = None  # x位置（ft）
    pz: Optional[float] = None  # z位置（ft）
    x0: Optional[float] = None  # リリースx座標
    y0: Optional[float] = None  # リリースy座標
    z0: Optional[float] = None  # リリースz座標
    vx0: Optional[float] = None  # x方向初速
    vy0: Optional[float] = None  # y方向初速
    vz0: Optional[float] = None  # z方向初速
    # 結果
    description: Optional[str] = None  # 結果の説明
    type: Optional[str] = None  # タイプ（B:ボール, S:ストライク, X:インプレー）
    strike_type: Optional[str] = None  # ストライクタイプ（C:コール, S:スイングストライク）
    ball_type: Optional[str] = None  # ボールタイプ
    event: Optional[str] = None  # イベント結果
    # 状況データ
    inning: int = 0             # イニング
    top_bottom: str = ""        # 表裏（'top'/'bottom'）
    outs: int = 0               # アウトカウント
    balls: int = 0              # ボールカウント
    strikes: int = 0            # ストライクカウント
    batter_id: int = 0          # 打者ID
    batter_side: str = ""       # 打者の向き（'L'/'R'）
    # メタデータ
    game_date: datetime = field(default_factory=datetime.now)  # 試合日
    pitcher_hand: str = ""      # 投手の利き手（'L'/'R'）
    # データ管理用
    source: str = "statcast"    # データソース
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class PitchingMixStats:
    """投手の球種構成データを格納するクラス"""
    player_id: int              # 投手ID
    season: int                 # シーズン年度
    pitch_type: PitchType       # 球種
    count: int = 0              # 投球数
    percentage: float = 0.0     # 割合（%）
    avg_speed: Optional[float] = None  # 平均球速
    max_speed: Optional[float] = None  # 最高球速
    avg_spin_rate: Optional[float] = None  # 平均回転数
    avg_break_angle: Optional[float] = None  # 平均変化角度
    avg_break_length: Optional[float] = None  # 平均変化量
    avg_break_y: Optional[float] = None  # 平均縦変化量
    whiff_rate: Optional[float] = None  # 空振り率
    put_away_rate: Optional[float] = None  # 2ストライク後の決め球成功率
    batting_avg: Optional[float] = None  # 打率
    slugging_pct: Optional[float] = None  # 長打率
    woba: Optional[float] = None  # wOBA
    launch_angle: Optional[float] = None  # 平均打球角度
    exit_velocity: Optional[float] = None  # 平均打球速度
    barrel_rate: Optional[float] = None  # バレル率
    hard_hit_rate: Optional[float] = None  # 強い当たり率
    # データ管理用
    source: str = "statcast"    # データソース
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


@dataclass
class SplitStats:
    """状況別統計を格納するクラス"""
    player_id: int              # 投手ID
    season: int                 # シーズン年度
    split_type: str             # 分割タイプ（vs_left, vs_right, home, away, first_half, second_half等）
    split_value: str            # 分割値
    innings_pitched: float = 0.0  # 投球回数
    hits: int = 0               # 被安打数
    runs: int = 0               # 失点
    earned_runs: int = 0        # 自責点
    home_runs: int = 0          # 被本塁打数
    strike_outs: int = 0        # 奪三振数
    base_on_balls: int = 0      # 与四球数
    batters_faced: int = 0      # 対戦打者数
    # 計算済み指標
    era: float = 0.0            # 防御率
    whip: float = 0.0           # WHIP
    avg: float = 0.0            # 被打率
    obp: float = 0.0            # 被出塁率
    slg: float = 0.0            # 被長打率
    ops: float = 0.0            # 被OPS
    # データ管理用
    source: str = "mlb_api"     # データソース
    updated_at: datetime = field(default_factory=datetime.now)  # 最終更新日時


# Database Schema Definition (SQLite用)
SCHEMA_DEFINITIONS = {
    "players": """
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY,
        full_name TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        primary_number TEXT,
        birth_date TEXT,
        throws TEXT,
        height_feet INTEGER,
        height_inches INTEGER,
        weight INTEGER,
        mlb_debut_date TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        primary_position TEXT,
        team_id INTEGER,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (team_id) REFERENCES teams (id)
    )
    """,
    
    "teams": """
    CREATE TABLE IF NOT EXISTS teams (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        abbreviation TEXT NOT NULL,
        team_code TEXT NOT NULL,
        league_id INTEGER NOT NULL,
        division_id INTEGER NOT NULL,
        venue_id INTEGER,
        city TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        first_year_of_play TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    
    "season_pitching_stats": """
    CREATE TABLE IF NOT EXISTS season_pitching_stats (
        player_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        team_id INTEGER NOT NULL,
        games_played INTEGER NOT NULL DEFAULT 0,
        games_started INTEGER NOT NULL DEFAULT 0,
        wins INTEGER NOT NULL DEFAULT 0,
        losses INTEGER NOT NULL DEFAULT 0,
        saves INTEGER NOT NULL DEFAULT 0,
        save_opportunities INTEGER NOT NULL DEFAULT 0,
        holds INTEGER NOT NULL DEFAULT 0,
        blown_saves INTEGER NOT NULL DEFAULT 0,
        innings_pitched REAL NOT NULL DEFAULT 0,
        hits INTEGER NOT NULL DEFAULT 0,
        runs INTEGER NOT NULL DEFAULT 0,
        earned_runs INTEGER NOT NULL DEFAULT 0,
        home_runs INTEGER NOT NULL DEFAULT 0,
        strike_outs INTEGER NOT NULL DEFAULT 0,
        base_on_balls INTEGER NOT NULL DEFAULT 0,
        intentional_walks INTEGER NOT NULL DEFAULT 0,
        hit_batsmen INTEGER NOT NULL DEFAULT 0,
        wild_pitches INTEGER NOT NULL DEFAULT 0,
        balks INTEGER NOT NULL DEFAULT 0,
        ground_outs INTEGER NOT NULL DEFAULT 0,
        fly_outs INTEGER NOT NULL DEFAULT 0,
        complete_games INTEGER NOT NULL DEFAULT 0,
        shutouts INTEGER NOT NULL DEFAULT 0,
        batters_faced INTEGER NOT NULL DEFAULT 0,
        pitches_thrown INTEGER NOT NULL DEFAULT 0,
        strikes INTEGER NOT NULL DEFAULT 0,
        era REAL NOT NULL DEFAULT 0,
        whip REAL NOT NULL DEFAULT 0,
        avg REAL NOT NULL DEFAULT 0,
        game_type TEXT NOT NULL DEFAULT 'R',
        fip REAL,
        xfip REAL,
        babip REAL,
        lob_percentage REAL,
        k_per_9 REAL,
        bb_per_9 REAL,
        hr_per_9 REAL,
        k_percentage REAL,
        bb_percentage REAL,
        k_minus_bb REAL,
        ground_ball_percentage REAL,
        fly_ball_percentage REAL,
        line_drive_percentage REAL,
        soft_hit_percentage REAL,
        medium_hit_percentage REAL,
        hard_hit_percentage REAL,
        war REAL,
        source TEXT NOT NULL DEFAULT 'mlb_api',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (player_id, season, team_id, game_type),
        FOREIGN KEY (player_id) REFERENCES players (id),
        FOREIGN KEY (team_id) REFERENCES teams (id)
    )
    """,
    
    "game_pitching_stats": """
    CREATE TABLE IF NOT EXISTS game_pitching_stats (
        player_id INTEGER NOT NULL,
        game_id INTEGER NOT NULL,
        game_date TEXT NOT NULL,
        team_id INTEGER NOT NULL,
        opponent_id INTEGER NOT NULL,
        home_away TEXT NOT NULL,
        innings_pitched REAL NOT NULL DEFAULT 0,
        hits INTEGER NOT NULL DEFAULT 0,
        runs INTEGER NOT NULL DEFAULT 0,
        earned_runs INTEGER NOT NULL DEFAULT 0,
        home_runs INTEGER NOT NULL DEFAULT 0,
        strike_outs INTEGER NOT NULL DEFAULT 0,
        base_on_balls INTEGER NOT NULL DEFAULT 0,
        win INTEGER NOT NULL DEFAULT 0,
        loss INTEGER NOT NULL DEFAULT 0,
        save INTEGER NOT NULL DEFAULT 0,
        hold INTEGER NOT NULL DEFAULT 0,
        blown_save INTEGER NOT NULL DEFAULT 0,
        pitches_thrown INTEGER NOT NULL DEFAULT 0,
        strikes INTEGER NOT NULL DEFAULT 0,
        batters_faced INTEGER NOT NULL DEFAULT 0,
        game_score INTEGER,
        season INTEGER NOT NULL DEFAULT 0,
        game_type TEXT NOT NULL DEFAULT 'R',
        source TEXT NOT NULL DEFAULT 'mlb_api',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (player_id, game_id),
        FOREIGN KEY (player_id) REFERENCES players (id),
        FOREIGN KEY (team_id) REFERENCES teams (id),
        FOREIGN KEY (opponent_id) REFERENCES teams (id)
    )
    """,
    
    "pitches": """
    CREATE TABLE IF NOT EXISTS pitches (
        id TEXT PRIMARY KEY,
        player_id INTEGER NOT NULL,
        game_id INTEGER NOT NULL,
        at_bat_id INTEGER NOT NULL,
        pitch_number INTEGER NOT NULL,
        pitch_type TEXT NOT NULL,
        start_speed REAL,
        end_speed REAL,
        spin_rate REAL,
        spin_direction REAL,
        break_angle REAL,
        break_length REAL,
        break_y REAL,
        zone INTEGER,
        plate_x REAL,
        plate_z REAL,
        px REAL,
        pz REAL,
        x0 REAL,
        y0 REAL,
        z0 REAL,
        vx0 REAL,
        vy0 REAL,
        vz0 REAL,
        description TEXT,
        type TEXT,
        strike_type TEXT,
        ball_type TEXT,
        event TEXT,
        inning INTEGER NOT NULL DEFAULT 0,
        top_bottom TEXT NOT NULL DEFAULT '',
        outs INTEGER NOT NULL DEFAULT 0,
        balls INTEGER NOT NULL DEFAULT 0,
        strikes INTEGER NOT NULL DEFAULT 0,
        batter_id INTEGER NOT NULL DEFAULT 0,
        batter_side TEXT NOT NULL DEFAULT '',
        game_date TEXT NOT NULL,
        pitcher_hand TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT 'statcast',
        updated_at TEXT NOT NULL,
        FOREIGN KEY (player_id) REFERENCES players (id),
        FOREIGN KEY (game_id) REFERENCES games (id),
        FOREIGN KEY (batter_id) REFERENCES players (id)
    )
    """,
    
    "pitching_mix_stats": """
    CREATE TABLE IF NOT EXISTS pitching_mix_stats (
        player_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        pitch_type TEXT NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        percentage REAL NOT NULL DEFAULT 0,
        avg_speed REAL,
        max_speed REAL,
        avg_spin_rate REAL,
        avg_break_angle REAL,
        avg_break_length REAL,
        avg_break_y REAL,
        whiff_rate REAL,
        put_away_rate REAL,
        batting_avg REAL,
        slugging_pct REAL,
        woba REAL,
        launch_angle REAL,
        exit_velocity REAL,
        barrel_rate REAL,
        hard_hit_rate REAL,
        source TEXT NOT NULL DEFAULT 'statcast',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (player_id, season, pitch_type),
        FOREIGN KEY (player_id) REFERENCES players (id)
    )
    """,
    
    "split_stats": """
    CREATE TABLE IF NOT EXISTS split_stats (
        player_id INTEGER NOT NULL,
        season INTEGER NOT NULL,
        split_type TEXT NOT NULL,
        split_value TEXT NOT NULL,
        innings_pitched REAL NOT NULL DEFAULT 0,
        hits INTEGER NOT NULL DEFAULT 0,
        runs INTEGER NOT NULL DEFAULT 0,
        earned_runs INTEGER NOT NULL DEFAULT 0,
        home_runs INTEGER NOT NULL DEFAULT 0,
        strike_outs INTEGER NOT NULL DEFAULT 0,
        base_on_balls INTEGER NOT NULL DEFAULT 0,
        batters_faced INTEGER NOT NULL DEFAULT 0,
        era REAL NOT NULL DEFAULT 0,
        whip REAL NOT NULL DEFAULT 0,
        avg REAL NOT NULL DEFAULT 0,
        obp REAL NOT NULL DEFAULT 0,
        slg REAL NOT NULL DEFAULT 0,
        ops REAL NOT NULL DEFAULT 0,
        source TEXT NOT NULL DEFAULT 'mlb_api',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (player_id, season, split_type, split_value),
        FOREIGN KEY (player_id) REFERENCES players (id)
    )
    """
}