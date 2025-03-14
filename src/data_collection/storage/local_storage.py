"""
データのローカルストレージを管理するモジュール

このモジュールは、MLB投手データをSQLiteデータベースに保存し、
取得するための機能を提供します。
"""

import logging
import sqlite3
import json
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from pathlib import Path
import pandas as pd

from ..config import config
from ...utils.logging import log_execution_time
from ..storage.schema import SCHEMA_DEFINITIONS

# ロガーの設定
logger = logging.getLogger(__name__)


class LocalStorage:
    """
    SQLiteデータベースを使用したローカルストレージクラス
    
    このクラスは、データベースの初期化、CRUD操作、
    クエリ実行などの機能を提供します。
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        LocalStorageを初期化する
        
        Args:
            db_path: データベースファイルのパス（Noneの場合は設定から取得）
        """
        self.db_path = Path(db_path) if db_path else config.database_path
        self.connection = None
        
        # データベースディレクトリが存在することを確認
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"LocalStorage initialized with database path: {self.db_path}")
    
    def connect(self) -> sqlite3.Connection:
        """
        データベースに接続する
        
        Returns:
            データベース接続オブジェクト
        """
        if self.connection is None:
            logger.debug(f"Connecting to database: {self.db_path}")
            
            # 接続を作成（存在しない場合は新規作成）
            self.connection = sqlite3.connect(
                self.db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            
            # カラム名を取得するための設定
            self.connection.row_factory = sqlite3.Row
            
            # 外部キー制約を有効化
            self.connection.execute("PRAGMA foreign_keys = ON")
            
            # データベースバージョンを確認
            sqlite_version = sqlite3.sqlite_version
            logger.info(f"Connected to SQLite database (version {sqlite_version})")
        
        return self.connection
    
    def close(self):
        """データベース接続を閉じる"""
        if self.connection:
            logger.debug("Closing database connection")
            self.connection.close()
            self.connection = None
    
    def initialize_database(self) -> bool:
        """
        データベースを初期化し、必要なテーブルを作成する
        
        Returns:
            初期化が成功した場合はTrue
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            logger.info("Initializing database tables")
            
            # スキーマ定義からテーブルを作成
            for table_name, create_statement in SCHEMA_DEFINITIONS.items():
                logger.debug(f"Creating table: {table_name}")
                cursor.execute(create_statement)
            
            # インデックスの作成
            # players テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players (last_name, first_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_team ON players (team_id)")
            
            # season_pitching_stats テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_season_stats_player ON season_pitching_stats (player_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_season_stats_season ON season_pitching_stats (season)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_season_stats_team ON season_pitching_stats (team_id)")
            
            # game_pitching_stats テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_stats_player ON game_pitching_stats (player_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_stats_game ON game_pitching_stats (game_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_stats_date ON game_pitching_stats (game_date)")
            
            # pitches テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitches_player ON pitches (player_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitches_game ON pitches (game_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitches_type ON pitches (pitch_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitches_date ON pitches (game_date)")
            
            # pitching_mix_stats テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_mix_player ON pitching_mix_stats (player_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_mix_season ON pitching_mix_stats (season)")
            
            # split_stats テーブル
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_split_player ON split_stats (player_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_split_season ON split_stats (season)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_split_type ON split_stats (split_type)")
            
            conn.commit()
            logger.info("Database initialization completed successfully")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            if conn:
                conn.rollback()
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        SQLクエリを実行し、結果を取得する
        
        Args:
            query: 実行するSQLクエリ
            params: クエリパラメータ
            
        Returns:
            クエリ結果（ディクショナリのリスト）
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # 結果をディクショナリのリストに変換
            results = [dict(row) for row in cursor.fetchall()]
            return results
            
        except sqlite3.Error as e:
            logger.error(f"Query execution error: {e}\nQuery: {query}\nParams: {params}")
            raise
    
    def execute_query_df(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        SQLクエリを実行し、結果をDataFrameとして取得する
        
        Args:
            query: 実行するSQLクエリ
            params: クエリパラメータ
            
        Returns:
            クエリ結果のDataFrame
        """
        conn = self.connect()
        
        try:
            if params:
                return pd.read_sql_query(query, conn, params=params)
            else:
                return pd.read_sql_query(query, conn)
                
        except Exception as e:
            logger.error(f"Query execution error (DataFrame): {e}\nQuery: {query}\nParams: {params}")
            raise
    
    def execute_transaction(self, statements: List[Tuple[str, Optional[Tuple]]]) -> bool:
        """
        複数のSQLステートメントをトランザクションとして実行する
        
        Args:
            statements: (query, params)のタプルのリスト
            
        Returns:
            トランザクションが成功した場合はTrue
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # トランザクション開始
            cursor.execute("BEGIN TRANSACTION")
            
            for query, params in statements:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
            
            # トランザクションをコミット
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Transaction execution error: {e}")
            # エラー発生時はロールバック
            conn.rollback()
            return False
    
    def insert_player(self, player_data: Dict[str, Any]) -> int:
        """
        選手データを挿入/更新する
        
        Args:
            player_data: 選手データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['id', 'full_name', 'first_name', 'last_name']
            for field in required_fields:
                if field not in player_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in player_data:
                player_data['updated_at'] = datetime.now()
            
            # UPSERT操作（存在する場合は更新、なければ挿入）
            query = """
            INSERT INTO players (
                id, full_name, first_name, last_name, primary_number, birth_date,
                throws, height_feet, height_inches, weight, mlb_debut_date,
                active, primary_position, team_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                full_name = excluded.full_name,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                primary_number = excluded.primary_number,
                birth_date = excluded.birth_date,
                throws = excluded.throws,
                height_feet = excluded.height_feet,
                height_inches = excluded.height_inches,
                weight = excluded.weight,
                mlb_debut_date = excluded.mlb_debut_date,
                active = excluded.active,
                primary_position = excluded.primary_position,
                team_id = excluded.team_id,
                updated_at = excluded.updated_at
            """
            
            params = (
                player_data['id'],
                player_data['full_name'],
                player_data['first_name'],
                player_data['last_name'],
                player_data.get('primary_number'),
                player_data.get('birth_date'),
                player_data.get('throws'),
                player_data.get('height_feet'),
                player_data.get('height_inches'),
                player_data.get('weight'),
                player_data.get('mlb_debut_date'),
                player_data.get('active', True),
                player_data.get('primary_position'),
                player_data.get('team_id'),
                player_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Player data inserted/updated for ID: {player_data['id']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting player data: {e}")
            conn.rollback()
            raise
    
    def insert_team(self, team_data: Dict[str, Any]) -> int:
        """
        チームデータを挿入/更新する
        
        Args:
            team_data: チームデータのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['id', 'name', 'abbreviation', 'team_code', 'league_id', 'division_id']
            for field in required_fields:
                if field not in team_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in team_data:
                team_data['updated_at'] = datetime.now()
            
            # UPSERT操作
            query = """
            INSERT INTO teams (
                id, name, abbreviation, team_code, league_id, division_id,
                venue_id, city, active, first_year_of_play, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                abbreviation = excluded.abbreviation,
                team_code = excluded.team_code,
                league_id = excluded.league_id,
                division_id = excluded.division_id,
                venue_id = excluded.venue_id,
                city = excluded.city,
                active = excluded.active,
                first_year_of_play = excluded.first_year_of_play,
                updated_at = excluded.updated_at
            """
            
            params = (
                team_data['id'],
                team_data['name'],
                team_data['abbreviation'],
                team_data['team_code'],
                team_data['league_id'],
                team_data['division_id'],
                team_data.get('venue_id'),
                team_data.get('city'),
                team_data.get('active', True),
                team_data.get('first_year_of_play'),
                team_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Team data inserted/updated for ID: {team_data['id']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting team data: {e}")
            conn.rollback()
            raise
    
    def insert_season_pitching_stats(self, stats_data: Dict[str, Any]) -> int:
        """
        シーズン投手成績データを挿入/更新する
        
        Args:
            stats_data: 投手成績データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['player_id', 'season', 'team_id']
            for field in required_fields:
                if field not in stats_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in stats_data:
                stats_data['updated_at'] = datetime.now()
            
            # ゲームタイプの設定
            if 'game_type' not in stats_data:
                stats_data['game_type'] = 'R'  # デフォルトはレギュラーシーズン
            
            # UPSERT操作
            query = """
            INSERT INTO season_pitching_stats (
                player_id, season, team_id, games_played, games_started,
                wins, losses, saves, save_opportunities, holds, blown_saves,
                innings_pitched, hits, runs, earned_runs, home_runs, strike_outs,
                base_on_balls, intentional_walks, hit_batsmen, wild_pitches, balks,
                ground_outs, fly_outs, complete_games, shutouts, batters_faced,
                pitches_thrown, strikes, era, whip, avg, game_type,
                fip, xfip, babip, lob_percentage, k_per_9, bb_per_9, hr_per_9,
                k_percentage, bb_percentage, k_minus_bb, ground_ball_percentage,
                fly_ball_percentage, line_drive_percentage, soft_hit_percentage,
                medium_hit_percentage, hard_hit_percentage, war,
                source, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(player_id, season, team_id, game_type) DO UPDATE SET
                games_played = excluded.games_played,
                games_started = excluded.games_started,
                wins = excluded.wins,
                losses = excluded.losses,
                saves = excluded.saves,
                save_opportunities = excluded.save_opportunities,
                holds = excluded.holds,
                blown_saves = excluded.blown_saves,
                innings_pitched = excluded.innings_pitched,
                hits = excluded.hits,
                runs = excluded.runs,
                earned_runs = excluded.earned_runs,
                home_runs = excluded.home_runs,
                strike_outs = excluded.strike_outs,
                base_on_balls = excluded.base_on_balls,
                intentional_walks = excluded.intentional_walks,
                hit_batsmen = excluded.hit_batsmen,
                wild_pitches = excluded.wild_pitches,
                balks = excluded.balks,
                ground_outs = excluded.ground_outs,
                fly_outs = excluded.fly_outs,
                complete_games = excluded.complete_games,
                shutouts = excluded.shutouts,
                batters_faced = excluded.batters_faced,
                pitches_thrown = excluded.pitches_thrown,
                strikes = excluded.strikes,
                era = excluded.era,
                whip = excluded.whip,
                avg = excluded.avg,
                fip = excluded.fip,
                xfip = excluded.xfip,
                babip = excluded.babip,
                lob_percentage = excluded.lob_percentage,
                k_per_9 = excluded.k_per_9,
                bb_per_9 = excluded.bb_per_9,
                hr_per_9 = excluded.hr_per_9,
                k_percentage = excluded.k_percentage,
                bb_percentage = excluded.bb_percentage,
                k_minus_bb = excluded.k_minus_bb,
                ground_ball_percentage = excluded.ground_ball_percentage,
                fly_ball_percentage = excluded.fly_ball_percentage,
                line_drive_percentage = excluded.line_drive_percentage,
                soft_hit_percentage = excluded.soft_hit_percentage,
                medium_hit_percentage = excluded.medium_hit_percentage,
                hard_hit_percentage = excluded.hard_hit_percentage,
                war = excluded.war,
                source = excluded.source,
                updated_at = excluded.updated_at
            """
            
            params = (
                stats_data['player_id'],
                stats_data['season'],
                stats_data['team_id'],
                stats_data.get('games_played', 0),
                stats_data.get('games_started', 0),
                stats_data.get('wins', 0),
                stats_data.get('losses', 0),
                stats_data.get('saves', 0),
                stats_data.get('save_opportunities', 0),
                stats_data.get('holds', 0),
                stats_data.get('blown_saves', 0),
                stats_data.get('innings_pitched', 0.0),
                stats_data.get('hits', 0),
                stats_data.get('runs', 0),
                stats_data.get('earned_runs', 0),
                stats_data.get('home_runs', 0),
                stats_data.get('strike_outs', 0),
                stats_data.get('base_on_balls', 0),
                stats_data.get('intentional_walks', 0),
                stats_data.get('hit_batsmen', 0),
                stats_data.get('wild_pitches', 0),
                stats_data.get('balks', 0),
                stats_data.get('ground_outs', 0),
                stats_data.get('fly_outs', 0),
                stats_data.get('complete_games', 0),
                stats_data.get('shutouts', 0),
                stats_data.get('batters_faced', 0),
                stats_data.get('pitches_thrown', 0),
                stats_data.get('strikes', 0),
                stats_data.get('era', 0.0),
                stats_data.get('whip', 0.0),
                stats_data.get('avg', 0.0),
                stats_data.get('game_type', 'R'),
                stats_data.get('fip'),
                stats_data.get('xfip'),
                stats_data.get('babip'),
                stats_data.get('lob_percentage'),
                stats_data.get('k_per_9'),
                stats_data.get('bb_per_9'),
                stats_data.get('hr_per_9'),
                stats_data.get('k_percentage'),
                stats_data.get('bb_percentage'),
                stats_data.get('k_minus_bb'),
                stats_data.get('ground_ball_percentage'),
                stats_data.get('fly_ball_percentage'),
                stats_data.get('line_drive_percentage'),
                stats_data.get('soft_hit_percentage'),
                stats_data.get('medium_hit_percentage'),
                stats_data.get('hard_hit_percentage'),
                stats_data.get('war'),
                stats_data.get('source', 'mlb_api'),
                stats_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Season pitching stats inserted/updated for player: {stats_data['player_id']}, season: {stats_data['season']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting season pitching stats: {e}")
            conn.rollback()
            raise
    
    def insert_game_pitching_stats(self, stats_data: Dict[str, Any]) -> int:
        """
        試合ごとの投手成績データを挿入/更新する
        
        Args:
            stats_data: 投手成績データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['player_id', 'game_id', 'game_date', 'team_id', 'opponent_id', 'home_away']
            for field in required_fields:
                if field not in stats_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in stats_data:
                stats_data['updated_at'] = datetime.now()
            
            # ゲームタイプの設定
            if 'game_type' not in stats_data:
                stats_data['game_type'] = 'R'  # デフォルトはレギュラーシーズン
            
            # UPSERT操作
            query = """
            INSERT INTO game_pitching_stats (
                player_id, game_id, game_date, team_id, opponent_id, home_away,
                innings_pitched, hits, runs, earned_runs, home_runs, strike_outs,
                base_on_balls, win, loss, save, hold, blown_save, pitches_thrown,
                strikes, batters_faced, game_score, season, game_type, source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, game_id) DO UPDATE SET
                game_date = excluded.game_date,
                team_id = excluded.team_id,
                opponent_id = excluded.opponent_id,
                home_away = excluded.home_away,
                innings_pitched = excluded.innings_pitched,
                hits = excluded.hits,
                runs = excluded.runs,
                earned_runs = excluded.earned_runs,
                home_runs = excluded.home_runs,
                strike_outs = excluded.strike_outs,
                base_on_balls = excluded.base_on_balls,
                win = excluded.win,
                loss = excluded.loss,
                save = excluded.save,
                hold = excluded.hold,
                blown_save = excluded.blown_save,
                pitches_thrown = excluded.pitches_thrown,
                strikes = excluded.strikes,
                batters_faced = excluded.batters_faced,
                game_score = excluded.game_score,
                season = excluded.season,
                game_type = excluded.game_type,
                source = excluded.source,
                updated_at = excluded.updated_at
            """
            
            # ブール値を整数に変換
            win = 1 if stats_data.get('win', False) else 0
            loss = 1 if stats_data.get('loss', False) else 0
            save = 1 if stats_data.get('save', False) else 0
            hold = 1 if stats_data.get('hold', False) else 0
            blown_save = 1 if stats_data.get('blown_save', False) else 0
            
            params = (
                stats_data['player_id'],
                stats_data['game_id'],
                stats_data['game_date'],
                stats_data['team_id'],
                stats_data['opponent_id'],
                stats_data['home_away'],
                stats_data.get('innings_pitched', 0.0),
                stats_data.get('hits', 0),
                stats_data.get('runs', 0),
                stats_data.get('earned_runs', 0),
                stats_data.get('home_runs', 0),
                stats_data.get('strike_outs', 0),
                stats_data.get('base_on_balls', 0),
                win,
                loss,
                save,
                hold,
                blown_save,
                stats_data.get('pitches_thrown', 0),
                stats_data.get('strikes', 0),
                stats_data.get('batters_faced', 0),
                stats_data.get('game_score'),
                stats_data.get('season', 0),
                stats_data.get('game_type', 'R'),
                stats_data.get('source', 'mlb_api'),
                stats_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Game pitching stats inserted/updated for player: {stats_data['player_id']}, game: {stats_data['game_id']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting game pitching stats: {e}")
            conn.rollback()
            raise
    
    def insert_pitch(self, pitch_data: Dict[str, Any]) -> int:
        """
        投球データを挿入/更新する
        
        Args:
            pitch_data: 投球データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['id', 'player_id', 'game_id', 'at_bat_id', 'pitch_number', 'pitch_type']
            for field in required_fields:
                if field not in pitch_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in pitch_data:
                pitch_data['updated_at'] = datetime.now()
            
            # UPSERT操作
            query = """
            INSERT INTO pitches (
                id, player_id, game_id, at_bat_id, pitch_number, pitch_type,
                start_speed, end_speed, spin_rate, spin_direction, break_angle,
                break_length, break_y, zone, plate_x, plate_z, px, pz, x0, y0, z0,
                vx0, vy0, vz0, description, type, strike_type, ball_type, event,
                inning, top_bottom, outs, balls, strikes, batter_id, batter_side,
                game_date, pitcher_hand, source, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(id) DO UPDATE SET
                player_id = excluded.player_id,
                game_id = excluded.game_id,
                at_bat_id = excluded.at_bat_id,
                pitch_number = excluded.pitch_number,
                pitch_type = excluded.pitch_type,
                start_speed = excluded.start_speed,
                end_speed = excluded.end_speed,
                spin_rate = excluded.spin_rate,
                spin_direction = excluded.spin_direction,
                break_angle = excluded.break_angle,
                break_length = excluded.break_length,
                break_y = excluded.break_y,
                zone = excluded.zone,
                plate_x = excluded.plate_x,
                plate_z = excluded.plate_z,
                px = excluded.px,
                pz = excluded.pz,
                x0 = excluded.x0,
                y0 = excluded.y0,
                z0 = excluded.z0,
                vx0 = excluded.vx0,
                vy0 = excluded.vy0,
                vz0 = excluded.vz0,
                description = excluded.description,
                type = excluded.type,
                strike_type = excluded.strike_type,
                ball_type = excluded.ball_type,
                event = excluded.event,
                inning = excluded.inning,
                top_bottom = excluded.top_bottom,
                outs = excluded.outs,
                balls = excluded.balls,
                strikes = excluded.strikes,
                batter_id = excluded.batter_id,
                batter_side = excluded.batter_side,
                game_date = excluded.game_date,
                pitcher_hand = excluded.pitcher_hand,
                source = excluded.source,
                updated_at = excluded.updated_at
            """
            
            params = (
                pitch_data['id'],
                pitch_data['player_id'],
                pitch_data['game_id'],
                pitch_data['at_bat_id'],
                pitch_data['pitch_number'],
                pitch_data['pitch_type'],
                pitch_data.get('start_speed'),
                pitch_data.get('end_speed'),
                pitch_data.get('spin_rate'),
                pitch_data.get('spin_direction'),
                pitch_data.get('break_angle'),
                pitch_data.get('break_length'),
                pitch_data.get('break_y'),
                pitch_data.get('zone'),
                pitch_data.get('plate_x'),
                pitch_data.get('plate_z'),
                pitch_data.get('px'),
                pitch_data.get('pz'),
                pitch_data.get('x0'),
                pitch_data.get('y0'),
                pitch_data.get('z0'),
                pitch_data.get('vx0'),
                pitch_data.get('vy0'),
                pitch_data.get('vz0'),
                pitch_data.get('description'),
                pitch_data.get('type'),
                pitch_data.get('strike_type'),
                pitch_data.get('ball_type'),
                pitch_data.get('event'),
                pitch_data.get('inning', 0),
                pitch_data.get('top_bottom', ''),
                pitch_data.get('outs', 0),
                pitch_data.get('balls', 0),
                pitch_data.get('strikes', 0),
                pitch_data.get('batter_id', 0),
                pitch_data.get('batter_side', ''),
                pitch_data.get('game_date', datetime.now()),
                pitch_data.get('pitcher_hand', ''),
                pitch_data.get('source', 'statcast'),
                pitch_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Pitch data inserted/updated for ID: {pitch_data['id']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting pitch data: {e}")
            conn.rollback()
            raise
    
    def insert_pitching_mix_stats(self, mix_data: Dict[str, Any]) -> int:
        """
        投手の球種構成データを挿入/更新する
        
        Args:
            mix_data: 球種構成データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['player_id', 'season', 'pitch_type']
            for field in required_fields:
                if field not in mix_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in mix_data:
                mix_data['updated_at'] = datetime.now()
            
            # UPSERT操作
            query = """
            INSERT INTO pitching_mix_stats (
                player_id, season, pitch_type, count, percentage, avg_speed,
                max_speed, avg_spin_rate, avg_break_angle, avg_break_length,
                avg_break_y, whiff_rate, put_away_rate, batting_avg, slugging_pct,
                woba, launch_angle, exit_velocity, barrel_rate, hard_hit_rate,
                source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, season, pitch_type) DO UPDATE SET
                count = excluded.count,
                percentage = excluded.percentage,
                avg_speed = excluded.avg_speed,
                max_speed = excluded.max_speed,
                avg_spin_rate = excluded.avg_spin_rate,
                avg_break_angle = excluded.avg_break_angle,
                avg_break_length = excluded.avg_break_length,
                avg_break_y = excluded.avg_break_y,
                whiff_rate = excluded.whiff_rate,
                put_away_rate = excluded.put_away_rate,
                batting_avg = excluded.batting_avg,
                slugging_pct = excluded.slugging_pct,
                woba = excluded.woba,
                launch_angle = excluded.launch_angle,
                exit_velocity = excluded.exit_velocity,
                barrel_rate = excluded.barrel_rate,
                hard_hit_rate = excluded.hard_hit_rate,
                source = excluded.source,
                updated_at = excluded.updated_at
            """
            
            params = (
                mix_data['player_id'],
                mix_data['season'],
                mix_data['pitch_type'],
                mix_data.get('count', 0),
                mix_data.get('percentage', 0.0),
                mix_data.get('avg_speed'),
                mix_data.get('max_speed'),
                mix_data.get('avg_spin_rate'),
                mix_data.get('avg_break_angle'),
                mix_data.get('avg_break_length'),
                mix_data.get('avg_break_y'),
                mix_data.get('whiff_rate'),
                mix_data.get('put_away_rate'),
                mix_data.get('batting_avg'),
                mix_data.get('slugging_pct'),
                mix_data.get('woba'),
                mix_data.get('launch_angle'),
                mix_data.get('exit_velocity'),
                mix_data.get('barrel_rate'),
                mix_data.get('hard_hit_rate'),
                mix_data.get('source', 'statcast'),
                mix_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Pitching mix stats inserted/updated for player: {mix_data['player_id']}, season: {mix_data['season']}, pitch: {mix_data['pitch_type']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting pitching mix stats: {e}")
            conn.rollback()
            raise
    
    def insert_split_stats(self, split_data: Dict[str, Any]) -> int:
        """
        状況別投手成績データを挿入/更新する
        
        Args:
            split_data: 状況別成績データのディクショナリ
            
        Returns:
            影響を受けた行数
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # 必須フィールドの確認
            required_fields = ['player_id', 'season', 'split_type', 'split_value']
            for field in required_fields:
                if field not in split_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # 更新日時の設定
            if 'updated_at' not in split_data:
                split_data['updated_at'] = datetime.now()
            
            # UPSERT操作
            query = """
            INSERT INTO split_stats (
                player_id, season, split_type, split_value, innings_pitched,
                hits, runs, earned_runs, home_runs, strike_outs, base_on_balls,
                batters_faced, era, whip, avg, obp, slg, ops, source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, season, split_type, split_value) DO UPDATE SET
                innings_pitched = excluded.innings_pitched,
                hits = excluded.hits,
                runs = excluded.runs,
                earned_runs = excluded.earned_runs,
                home_runs = excluded.home_runs,
                strike_outs = excluded.strike_outs,
                base_on_balls = excluded.base_on_balls,
                batters_faced = excluded.batters_faced,
                era = excluded.era,
                whip = excluded.whip,
                avg = excluded.avg,
                obp = excluded.obp,
                slg = excluded.slg,
                ops = excluded.ops,
                source = excluded.source,
                updated_at = excluded.updated_at
            """
            
            params = (
                split_data['player_id'],
                split_data['season'],
                split_data['split_type'],
                split_data['split_value'],
                split_data.get('innings_pitched', 0.0),
                split_data.get('hits', 0),
                split_data.get('runs', 0),
                split_data.get('earned_runs', 0),
                split_data.get('home_runs', 0),
                split_data.get('strike_outs', 0),
                split_data.get('base_on_balls', 0),
                split_data.get('batters_faced', 0),
                split_data.get('era', 0.0),
                split_data.get('whip', 0.0),
                split_data.get('avg', 0.0),
                split_data.get('obp', 0.0),
                split_data.get('slg', 0.0),
                split_data.get('ops', 0.0),
                split_data.get('source', 'mlb_api'),
                split_data['updated_at']
            )
            
            cursor.execute(query, params)
            conn.commit()
            
            logger.debug(f"Split stats inserted/updated for player: {split_data['player_id']}, season: {split_data['season']}, split: {split_data['split_type']}/{split_data['split_value']}")
            return cursor.rowcount
            
        except Exception as e:
            logger.error(f"Error inserting split stats: {e}")
            conn.rollback()
            raise
    
    def get_player(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        選手情報を取得する
        
        Args:
            player_id: 選手ID
            
        Returns:
            選手データのディクショナリ、または存在しない場合はNone
        """
        try:
            query = "SELECT * FROM players WHERE id = ?"
            results = self.execute_query(query, (player_id,))
            
            if results:
                return results[0]
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting player data: {e}")
            raise
    
    def search_players(self, name: str) -> List[Dict[str, Any]]:
        """
        選手を名前で検索する
        
        Args:
            name: 検索する選手名
            
        Returns:
            検索結果の選手リスト
        """
        try:
            # 名前の前後に%を追加してLIKE検索
            search_pattern = f"%{name}%"
            
            query = """
            SELECT * FROM players 
            WHERE full_name LIKE ? OR first_name LIKE ? OR last_name LIKE ?
            ORDER BY last_name, first_name
            """
            
            return self.execute_query(query, (search_pattern, search_pattern, search_pattern))
                
        except Exception as e:
            logger.error(f"Error searching players: {e}")
            raise
    
    def get_player_season_stats(self, player_id: int, 
                               season: Optional[int] = None,
                               game_type: str = 'R') -> List[Dict[str, Any]]:
        """
        選手のシーズン成績を取得する
        
        Args:
            player_id: 選手ID
            season: シーズン年度（Noneの場合は全シーズン）
            game_type: 試合タイプ（'R'=レギュラーシーズン）
            
        Returns:
            シーズン成績のリスト
        """
        try:
            params = [player_id, game_type]
            
            if season:
                query = """
                SELECT * FROM season_pitching_stats 
                WHERE player_id = ? AND game_type = ? AND season = ?
                ORDER BY season DESC
                """
                params.append(season)
            else:
                query = """
                SELECT * FROM season_pitching_stats 
                WHERE player_id = ? AND game_type = ?
                ORDER BY season DESC
                """
            
            return self.execute_query(query, tuple(params))
                
        except Exception as e:
            logger.error(f"Error getting player season stats: {e}")
            raise
    
    def get_player_game_stats(self, player_id: int, 
                             season: Optional[int] = None,
                             limit: int = 10) -> List[Dict[str, Any]]:
        """
        選手の試合成績を取得する
        
        Args:
            player_id: 選手ID
            season: シーズン年度（Noneの場合は全シーズン）
            limit: 取得する最大試合数
            
        Returns:
            試合成績のリスト
        """
        try:
            params = [player_id]
            
            if season:
                query = """
                SELECT g.*, t.name as team_name, o.name as opponent_name
                FROM game_pitching_stats g
                LEFT JOIN teams t ON g.team_id = t.id
                LEFT JOIN teams o ON g.opponent_id = o.id
                WHERE g.player_id = ? AND g.season = ?
                ORDER BY g.game_date DESC
                LIMIT ?
                """
                params.extend([season, limit])
            else:
                query = """
                SELECT g.*, t.name as team_name, o.name as opponent_name
                FROM game_pitching_stats g
                LEFT JOIN teams t ON g.team_id = t.id
                LEFT JOIN teams o ON g.opponent_id = o.id
                WHERE g.player_id = ?
                ORDER BY g.game_date DESC
                LIMIT ?
                """
                params.append(limit)
            
            return self.execute_query(query, tuple(params))
                
        except Exception as e:
            logger.error(f"Error getting player game stats: {e}")
            raise
    
    def get_player_pitch_mix(self, player_id: int, 
                            season: int) -> List[Dict[str, Any]]:
        """
        選手の球種構成を取得する
        
        Args:
            player_id: 選手ID
            season: シーズン年度
            
        Returns:
            球種構成データのリスト
        """
        try:
            query = """
            SELECT * FROM pitching_mix_stats
            WHERE player_id = ? AND season = ?
            ORDER BY percentage DESC
            """
            
            return self.execute_query(query, (player_id, season))
                
        except Exception as e:
            logger.error(f"Error getting player pitch mix: {e}")
            raise
    
    def get_player_split_stats(self, player_id: int, 
                              season: int,
                              split_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        選手の状況別成績を取得する
        
        Args:
            player_id: 選手ID
            season: シーズン年度
            split_type: 状況タイプ（Noneの場合は全タイプ）
            
        Returns:
            状況別成績のリスト
        """
        try:
            if split_type:
                query = """
                SELECT * FROM split_stats
                WHERE player_id = ? AND season = ? AND split_type = ?
                ORDER BY split_type, split_value
                """
                return self.execute_query(query, (player_id, season, split_type))
            else:
                query = """
                SELECT * FROM split_stats
                WHERE player_id = ? AND season = ?
                ORDER BY split_type, split_value
                """
                return self.execute_query(query, (player_id, season))
                
        except Exception as e:
            logger.error(f"Error getting player split stats: {e}")
            raise
    
    def get_top_pitchers(self, season: int, 
                        stat: str = 'era', 
                        min_innings: float = 50.0,
                        limit: int = 10,
                        order: str = 'ASC') -> List[Dict[str, Any]]:
        """
        指定された統計値でトップの投手を取得する
        
        Args:
            season: シーズン年度
            stat: 並べ替える統計（era, whip, strike_outs など）
            min_innings: 最低投球回数
            limit: 取得する最大選手数
            order: 並べ替え順序（'ASC'または'DESC'）
            
        Returns:
            投手リスト
        """
        try:
            # 安全のため、入力値を検証
            valid_stats = [
                'era', 'whip', 'wins', 'losses', 'saves', 'strike_outs',
                'base_on_balls', 'innings_pitched', 'home_runs', 'hits'
            ]
            
            if stat not in valid_stats:
                raise ValueError(f"Invalid stat: {stat}")
            
            valid_orders = ['ASC', 'DESC']
            if order not in valid_orders:
                order = 'ASC'
            
            query = f"""
            SELECT s.*, p.full_name, p.throws, p.primary_position, t.name as team_name
            FROM season_pitching_stats s
            JOIN players p ON s.player_id = p.id
            JOIN teams t ON s.team_id = t.id
            WHERE s.season = ? AND s.innings_pitched >= ? AND s.game_type = 'R'
            ORDER BY s.{stat} {order}, s.innings_pitched DESC
            LIMIT ?
            """
            
            return self.execute_query(query, (season, min_innings, limit))
                
        except Exception as e:
            logger.error(f"Error getting top pitchers: {e}")
            raise
    
    @log_execution_time
    def get_pitch_data(self, player_id: int, 
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None,
                      pitch_type: Optional[str] = None,
                      limit: int = 1000) -> List[Dict[str, Any]]:
        """
        選手の投球データを取得する
        
        Args:
            player_id: 選手ID
            start_date: 開始日
            end_date: 終了日
            pitch_type: 球種
            limit: 取得する最大投球数
            
        Returns:
            投球データのリスト
        """
        try:
            params = [player_id]
            conditions = []
            
            if start_date:
                conditions.append("game_date >= ?")
                params.append(start_date)
            
            if end_date:
                conditions.append("game_date <= ?")
                params.append(end_date)
            
            if pitch_type:
                conditions.append("pitch_type = ?")
                params.append(pitch_type)
            
            where_clause = " AND ".join(conditions) if conditions else ""
            if where_clause:
                where_clause = f" AND {where_clause}"
            
            query = f"""
            SELECT * FROM pitches
            WHERE player_id = ?{where_clause}
            ORDER BY game_date DESC, at_bat_id DESC, pitch_number
            LIMIT ?
            """
            
            params.append(limit)
            
            return self.execute_query(query, tuple(params))
                
        except Exception as e:
            logger.error(f"Error getting pitch data: {e}")
            raise
    
    def get_team(self, team_id: int) -> Optional[Dict[str, Any]]:
        """
        チーム情報を取得する
        
        Args:
            team_id: チームID
            
        Returns:
            チームデータのディクショナリ、または存在しない場合はNone
        """
        try:
            query = "SELECT * FROM teams WHERE id = ?"
            results = self.execute_query(query, (team_id,))
            
            if results:
                return results[0]
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting team data: {e}")
            raise
    
    def get_all_teams(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        全チームを取得する
        
        Args:
            active_only: アクティブなチームのみ取得するかどうか
            
        Returns:
            チームリスト
        """
        try:
            if active_only:
                query = "SELECT * FROM teams WHERE active = 1 ORDER BY name"
                return self.execute_query(query)
            else:
                query = "SELECT * FROM teams ORDER BY name"
                return self.execute_query(query)
                
        except Exception as e:
            logger.error(f"Error getting all teams: {e}")
            raise


# ローカルストレージのシングルトンインスタンス
local_storage = LocalStorage()