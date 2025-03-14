"""
JSONデータの読み込みと処理を行うモジュール

このモジュールは、MLB APIやその他のソースからのJSONデータを処理し、
アプリケーションで使用可能なデータ形式に変換する機能を提供します。
"""

import json
import logging
import os
from typing import Dict, List, Any, Optional, Union, Callable, TypeVar
import pandas as pd
from datetime import datetime
from pathlib import Path
import re

from ..config import config

# ロガーの設定
logger = logging.getLogger(__name__)

# ジェネリック型の定義
T = TypeVar('T')


class JSONParser:
    """
    JSONデータのパースと変換を行うクラス
    
    このクラスは、JSONファイルの読み込み、データの抽出、
    データ構造の変換などの機能を提供します。
    """
    
    def __init__(self):
        """JSONパーサーの初期化"""
        logger.debug("JSONParser initialized")
    
    def read_json_file(self, file_path: Union[str, Path]) -> Any:
        """
        JSONファイルを読み込む
        
        Args:
            file_path: JSONファイルのパス
            
        Returns:
            JSONデータ（通常はディクショナリまたはリスト）
            
        Raises:
            FileNotFoundError: ファイルが見つからない場合
            json.JSONDecodeError: JSONの解析に失敗した場合
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"JSON file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Reading JSON file: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Successfully read JSON from {file_path}")
            return data
            
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise
        
        except Exception as e:
            error_msg = f"Error reading JSON file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise
    
    def parse_json_string(self, json_string: str) -> Any:
        """
        JSON文字列をパースする
        
        Args:
            json_string: パースするJSON文字列
            
        Returns:
            パースされたJSONデータ
            
        Raises:
            json.JSONDecodeError: JSONの解析に失敗した場合
        """
        try:
            return json.loads(json_string)
            
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON string: {str(e)}"
            logger.error(error_msg)
            raise
    
    def save_to_json(self, data: Any, 
                    output_path: Union[str, Path], 
                    indent: int = 4) -> str:
        """
        データをJSONファイルに保存する
        
        Args:
            data: 保存するデータ
            output_path: 出力ファイルパス
            indent: JSONのインデント
            
        Returns:
            保存されたファイルのパス
        """
        output_path = Path(output_path)
        
        # 親ディレクトリが存在しない場合は作成
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent)
            
            logger.info(f"Successfully saved data to {output_path}")
            return str(output_path)
            
        except Exception as e:
            error_msg = f"Failed to save data to JSON file {output_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def extract_nested_data(self, data: Dict[str, Any], 
                           path: List[str], 
                           default: Any = None) -> Any:
        """
        ネストされたディクショナリから指定されたパスのデータを抽出する
        
        Args:
            data: 元のディクショナリデータ
            path: 抽出するデータのパス（キーのリスト）
            default: キーが見つからない場合のデフォルト値
            
        Returns:
            抽出されたデータ、またはデフォルト値
        """
        current = data
        
        for key in path:
            # リストの場合はインデックスとして処理
            if isinstance(current, list) and key.isdigit():
                index = int(key)
                if 0 <= index < len(current):
                    current = current[index]
                else:
                    return default
            # ディクショナリの場合はキーとして処理
            elif isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current
    
    def flatten_json(self, nested_json: Dict[str, Any], 
                    separator: str = '_') -> Dict[str, Any]:
        """
        ネストされたJSONを平坦なディクショナリに変換する
        
        Args:
            nested_json: ネストされたディクショナリ
            separator: 階層を示す区切り文字
            
        Returns:
            平坦化されたディクショナリ
        """
        flattened = {}
        
        def _flatten(curr_obj, prefix=''):
            if isinstance(curr_obj, dict):
                for key, val in curr_obj.items():
                    new_prefix = f"{prefix}{separator}{key}" if prefix else key
                    _flatten(val, new_prefix)
            elif isinstance(curr_obj, list):
                for i, item in enumerate(curr_obj):
                    new_prefix = f"{prefix}{separator}{i}" if prefix else str(i)
                    _flatten(item, new_prefix)
            else:
                flattened[prefix] = curr_obj
        
        _flatten(nested_json)
        return flattened
    
    def json_to_dataframe(self, data: Union[List[Dict[str, Any]], Dict[str, Any]], 
                         normalize_path: Optional[str] = None) -> pd.DataFrame:
        """
        JSONデータをDataFrameに変換する
        
        Args:
            data: 変換するJSONデータ
            normalize_path: 正規化するネストされたデータのパス
            
        Returns:
            変換されたDataFrame
        """
        try:
            if normalize_path and isinstance(data, dict):
                # ネストされたパスからデータフレームを作成
                nested_data = self.extract_nested_data(data, normalize_path.split('.'), [])
                if nested_data:
                    return pd.json_normalize(nested_data)
                else:
                    return pd.DataFrame()
            elif isinstance(data, list):
                # リストから直接データフレームを作成
                return pd.json_normalize(data)
            elif isinstance(data, dict):
                # ディクショナリをシングルロウのデータフレームに変換
                return pd.DataFrame([data])
            else:
                logger.warning(f"Unsupported data type for DataFrame conversion: {type(data)}")
                return pd.DataFrame()
                
        except Exception as e:
            error_msg = f"Failed to convert JSON to DataFrame: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def convert_mlb_api_player(self, player_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        MLB API の選手データを標準形式に変換する
        
        Args:
            player_data: MLB APIからの選手データ
            
        Returns:
            標準形式に変換された選手データ
        """
        try:
            # 選手データがpeople配列の中にある場合の処理
            if 'people' in player_data and isinstance(player_data['people'], list) and player_data['people']:
                person = player_data['people'][0]
            else:
                person = player_data
            
            # 基本データの抽出
            player = {
                'id': person.get('id'),
                'full_name': person.get('fullName', ''),
                'first_name': person.get('firstName', ''),
                'last_name': person.get('lastName', ''),
                'primary_number': person.get('primaryNumber'),
                'throws': person.get('pitchHand', {}).get('code') if 'pitchHand' in person else None,
                'primary_position': person.get('primaryPosition', {}).get('abbreviation') if 'primaryPosition' in person else None,
                'active': person.get('active', False)
            }
            
            # 生年月日の処理
            if 'birthDate' in person:
                try:
                    player['birth_date'] = datetime.strptime(person['birthDate'], '%Y-%m-%d')
                except:
                    logger.warning(f"Failed to parse birth date for player {player['id']}: {person.get('birthDate')}")
            
            # 身長の処理
            if 'height' in person:
                height = person['height']
                if isinstance(height, str) and "'" in height:
                    # 5'11" 形式の身長を処理
                    feet, inches = height.split("'")
                    player['height_feet'] = int(feet.strip())
                    player['height_inches'] = int(inches.strip('"'))
            
            # 体重の処理
            if 'weight' in person:
                player['weight'] = person['weight']
            
            # デビュー日の処理
            if 'mlbDebutDate' in person:
                try:
                    player['mlb_debut_date'] = datetime.strptime(person['mlbDebutDate'], '%Y-%m-%d')
                except:
                    logger.warning(f"Failed to parse MLB debut date for player {player['id']}: {person.get('mlbDebutDate')}")
            
            # チームの処理
            if 'currentTeam' in person:
                player['team_id'] = person['currentTeam'].get('id')
            
            return player
            
        except Exception as e:
            logger.error(f"Error converting MLB API player data: {str(e)}")
            # 基本的なIDと名前だけでも返す
            return {
                'id': player_data.get('id') or player_data.get('player_id'),
                'full_name': player_data.get('fullName') or player_data.get('name') or '',
            }
    
    def convert_mlb_api_pitching_stats(self, stats_data: Dict[str, Any], 
                                      player_id: int,
                                      season: Optional[int] = None) -> Dict[str, Any]:
        """
        MLB API の投手成績データを標準形式に変換する
        
        Args:
            stats_data: MLB APIからの投手成績データ
            player_id: 選手ID
            season: シーズン年度
            
        Returns:
            標準形式に変換された投手成績データ
        """
        try:
            # 投手成績データの抽出
            if 'stats' in stats_data and isinstance(stats_data['stats'], list):
                stats_list = stats_data['stats']
                # 最初の統計グループを使用
                if stats_list and 'splits' in stats_list[0] and stats_list[0]['splits']:
                    split_data = stats_list[0]['splits'][0]
                    stats = split_data.get('stat', {})
                    
                    # シーズン情報の取得
                    if season is None and 'season' in split_data:
                        season = int(split_data['season'])
                    
                    # チーム情報の取得
                    team_id = None
                    if 'team' in split_data:
                        team_id = split_data['team'].get('id')
                    
                    # 投手成績の標準形式への変換
                    pitching_stats = {
                        'player_id': player_id,
                        'season': season,
                        'team_id': team_id,
                        'games_played': stats.get('gamesPlayed', 0),
                        'games_started': stats.get('gamesStarted', 0),
                        'wins': stats.get('wins', 0),
                        'losses': stats.get('losses', 0),
                        'saves': stats.get('saves', 0),
                        'save_opportunities': stats.get('saveOpportunities', 0),
                        'holds': stats.get('holds', 0),
                        'blown_saves': stats.get('blownSaves', 0),
                        'innings_pitched': float(stats.get('inningsPitched', 0)),
                        'hits': stats.get('hits', 0),
                        'runs': stats.get('runs', 0),
                        'earned_runs': stats.get('earnedRuns', 0),
                        'home_runs': stats.get('homeRuns', 0),
                        'strike_outs': stats.get('strikeOuts', 0),
                        'base_on_balls': stats.get('baseOnBalls', 0),
                        'intentional_walks': stats.get('intentionalWalks', 0),
                        'hit_batsmen': stats.get('hitBatsmen', 0),
                        'wild_pitches': stats.get('wildPitches', 0),
                        'balks': stats.get('balks', 0),
                        'ground_outs': stats.get('groundOuts', 0),
                        'fly_outs': stats.get('airOuts', 0),
                        'complete_games': stats.get('completeGames', 0),
                        'shutouts': stats.get('shutouts', 0),
                        'batters_faced': stats.get('battersFaced', 0),
                        'era': stats.get('era', 0.0),
                        'whip': stats.get('whip', 0.0),
                        'avg': stats.get('avg', 0.0),
                        'game_type': split_data.get('gameType', 'R'),
                        'source': 'mlb_api',
                        'updated_at': datetime.now()
                    }
                    
                    return pitching_stats
            
            # データが見つからない場合は空のディクショナリと警告
            logger.warning(f"No pitching stats found for player {player_id}, season {season}")
            return {}
            
        except Exception as e:
            logger.error(f"Error converting MLB API pitching stats: {str(e)}")
            return {}
    
    def parse_statcast_pitch_data(self, pitch_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Statcastの投球データを解析し標準形式に変換する
        
        Args:
            pitch_data: Statcastからの投球データ
            
        Returns:
            標準形式に変換された投球データ
        """
        try:
            pitch = {
                'id': pitch_data.get('pitch_id') or f"{pitch_data.get('game_pk')}_{pitch_data.get('at_bat_number')}_{pitch_data.get('pitch_number')}",
                'player_id': pitch_data.get('pitcher'),
                'game_id': pitch_data.get('game_pk'),
                'at_bat_id': pitch_data.get('at_bat_number'),
                'pitch_number': pitch_data.get('pitch_number'),
                'pitch_type': pitch_data.get('pitch_type', 'UN'),
                'start_speed': pitch_data.get('release_speed'),
                'end_speed': pitch_data.get('effective_speed'),
                'spin_rate': pitch_data.get('release_spin_rate'),
                'spin_direction': pitch_data.get('spin_dir'),
                'break_angle': pitch_data.get('break_angle'),
                'break_length': pitch_data.get('break_length'),
                'break_y': pitch_data.get('break_y'),
                'zone': pitch_data.get('zone'),
                'plate_x': pitch_data.get('plate_x'),
                'plate_z': pitch_data.get('plate_z'),
                'px': pitch_data.get('px'),
                'pz': pitch_data.get('pz'),
                'x0': pitch_data.get('x0'),
                'y0': pitch_data.get('y0'),
                'z0': pitch_data.get('z0'),
                'vx0': pitch_data.get('vx0'),
                'vy0': pitch_data.get('vy0'),
                'vz0': pitch_data.get('vz0'),
                'description': pitch_data.get('des'),
                'type': pitch_data.get('type'),
                'event': pitch_data.get('events'),
                'inning': pitch_data.get('inning'),
                'top_bottom': pitch_data.get('inning_topbot'),
                'outs': pitch_data.get('outs_when_up'),
                'balls': pitch_data.get('balls'),
                'strikes': pitch_data.get('strikes'),
                'batter_id': pitch_data.get('batter'),
                'batter_side': pitch_data.get('stand'),
                'pitcher_hand': pitch_data.get('p_throws'),
                'source': 'statcast'
            }
            
            # 日付の処理
            if 'game_date' in pitch_data:
                try:
                    game_date = pitch_data['game_date']
                    if isinstance(game_date, str):
                        pitch['game_date'] = datetime.strptime(game_date, '%Y-%m-%d')
                    elif isinstance(game_date, pd.Timestamp):
                        pitch['game_date'] = game_date.to_pydatetime()
                    else:
                        pitch['game_date'] = game_date
                except Exception as e:
                    logger.warning(f"Failed to parse game date: {e}")
            
            # 更新日時の設定
            pitch['updated_at'] = datetime.now()
            
            return pitch
            
        except Exception as e:
            logger.error(f"Error parsing Statcast pitch data: {str(e)}")
            # 最低限のデータだけでも返す
            return {
                'id': pitch_data.get('pitch_id') or f"{pitch_data.get('game_pk')}_{pitch_data.get('at_bat_number')}_{pitch_data.get('pitch_number')}",
                'player_id': pitch_data.get('pitcher'),
                'game_id': pitch_data.get('game_pk'),
                'updated_at': datetime.now()
            }


# JSONパーサーのシングルトンインスタンス
json_parser = JSONParser()