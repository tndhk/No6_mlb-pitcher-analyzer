"""
CSVファイルの読み込みと処理を行うモジュール

このモジュールは、MLBデータを含むCSVファイルを読み込み、
アプリケーションで使用可能なデータ形式に変換する機能を提供します。
"""

import csv
import logging
import os
from typing import Dict, List, Any, Optional, Union, Callable, TypeVar, Iterator
import pandas as pd
from datetime import datetime
from pathlib import Path
import re

from ..config import config

# ロガーの設定
logger = logging.getLogger(__name__)

# ジェネリック型の定義
T = TypeVar('T')


class CSVParser:
    """
    CSVファイルのパースと変換を行うクラス
    
    このクラスは、CSVファイルの読み込み、データ型変換、
    カラム名マッピングなどの機能を提供します。
    """
    
    def __init__(self):
        """CSVパーサーの初期化"""
        logger.debug("CSVParser initialized")
    
    def read_csv(self, file_path: Union[str, Path], 
                 delimiter: str = ',', 
                 encoding: str = 'utf-8',
                 skip_rows: int = 0,
                 **kwargs) -> List[Dict[str, Any]]:
        """
        CSVファイルを読み込み、ディクショナリのリストとして返す
        
        Args:
            file_path: CSVファイルのパス
            delimiter: 区切り文字
            encoding: ファイルのエンコーディング
            skip_rows: スキップする先頭行数
            **kwargs: csv.DictReaderに渡す追加パラメータ
            
        Returns:
            CSVの各行をディクショナリとしたリスト
            
        Raises:
            FileNotFoundError: ファイルが見つからない場合
            ValueError: CSVの解析に失敗した場合
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"CSV file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Reading CSV file: {file_path}")
            
            rows = []
            with open(file_path, 'r', encoding=encoding, newline='') as csvfile:
                # 指定された行数をスキップ
                for _ in range(skip_rows):
                    next(csvfile)
                
                reader = csv.DictReader(csvfile, delimiter=delimiter, **kwargs)
                for row in reader:
                    # 空白キーを除去
                    cleaned_row = {k.strip(): v for k, v in row.items() if k is not None}
                    rows.append(cleaned_row)
            
            logger.info(f"Successfully read {len(rows)} rows from {file_path}")
            return rows
            
        except Exception as e:
            error_msg = f"Failed to parse CSV file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def read_csv_with_pandas(self, file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
        """
        pandasを使用してCSVファイルを読み込む
        
        より高度なデータ処理が必要な場合に使用します。
        
        Args:
            file_path: CSVファイルのパス
            **kwargs: pd.read_csvに渡す追加パラメータ
            
        Returns:
            CSVデータを含むDataFrame
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"CSV file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Reading CSV file with pandas: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            
            # 空白のカラム名を修正
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            
            logger.info(f"Successfully read {len(df)} rows from {file_path} with pandas")
            return df
            
        except Exception as e:
            error_msg = f"Failed to parse CSV file {file_path} with pandas: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def convert_types(self, data: List[Dict[str, Any]], 
                     type_mapping: Dict[str, Callable[[str], Any]]) -> List[Dict[str, Any]]:
        """
        ディクショナリリスト内の値を指定された型に変換する
        
        Args:
            data: 変換するデータ（ディクショナリのリスト）
            type_mapping: カラム名と変換関数のマッピング
            
        Returns:
            型変換後のデータ
        """
        converted_data = []
        
        for row in data:
            converted_row = {}
            
            for key, value in row.items():
                if key in type_mapping and value is not None and value != '':
                    try:
                        converted_row[key] = type_mapping[key](value)
                    except Exception as e:
                        logger.warning(f"Failed to convert value '{value}' for column '{key}': {e}")
                        converted_row[key] = None
                else:
                    converted_row[key] = value
            
            converted_data.append(converted_row)
        
        return converted_data
    
    def map_columns(self, data: List[Dict[str, Any]], 
                   column_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        ディクショナリリスト内のカラム名をマッピングに従って変換する
        
        Args:
            data: 変換するデータ（ディクショナリのリスト）
            column_mapping: 元のカラム名と新しいカラム名のマッピング
            
        Returns:
            カラム名変換後のデータ
        """
        mapped_data = []
        
        for row in data:
            mapped_row = {}
            
            for key, value in row.items():
                if key in column_mapping:
                    mapped_row[column_mapping[key]] = value
                else:
                    mapped_row[key] = value
            
            mapped_data.append(mapped_row)
        
        return mapped_data
    
    def filter_columns(self, data: List[Dict[str, Any]], 
                       columns: List[str]) -> List[Dict[str, Any]]:
        """
        ディクショナリリストから指定されたカラムのみを抽出する
        
        Args:
            data: フィルタリングするデータ（ディクショナリのリスト）
            columns: 残すカラム名のリスト
            
        Returns:
            フィルタリング後のデータ
        """
        filtered_data = []
        
        for row in data:
            filtered_row = {key: value for key, value in row.items() if key in columns}
            filtered_data.append(filtered_row)
        
        return filtered_data
    
    def process_statcast_csv(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Baseball Savant（Statcast）からのCSVファイルを処理する
        
        Statcast形式のCSVファイルを読み込み、適切なデータ型に変換します。
        
        Args:
            file_path: CSVファイルのパス
            
        Returns:
            処理後のDataFrame
        """
        try:
            logger.info(f"Processing Statcast CSV file: {file_path}")
            
            # CSVファイルを読み込む
            df = pd.read_csv(file_path, low_memory=False)
            
            # カラム名のクリーニング
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            # 一般的なStatcastの日付フォーマット変換
            if 'game_date' in df.columns:
                df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')
            
            # 数値データ型の変換
            numeric_columns = [
                'release_speed', 'release_pos_x', 'release_pos_z', 
                'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 
                'launch_speed', 'launch_angle', 'spin_rate', 
                'spin_dir', 'break_angle', 'break_length', 
                'zone', 'strikes', 'balls', 'outs_when_up'
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 選手IDを整数に変換
            player_id_columns = ['pitcher', 'batter']
            for col in player_id_columns:
                if col in df.columns:
                    df[col] = df[col].astype('Int64')  # pandas 0.24.0以降のnullableな整数型
            
            # 球種コードを統一
            if 'pitch_type' in df.columns:
                # NAやNone値を除外
                df['pitch_type'] = df['pitch_type'].fillna('UN')  # 不明球種
                # 全て大文字に変換
                df['pitch_type'] = df['pitch_type'].str.upper()
            
            # 欠損値の処理
            df = df.fillna({
                'events': 'unknown',
                'description': 'unknown',
                'pitch_name': 'unknown'
            })
            
            logger.info(f"Successfully processed Statcast CSV with {len(df)} rows")
            return df
            
        except Exception as e:
            error_msg = f"Failed to process Statcast CSV file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def process_fangraphs_csv(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        FanGraphsからのCSVファイルを処理する
        
        FanGraphs形式のCSVファイルを読み込み、適切なデータ型に変換します。
        
        Args:
            file_path: CSVファイルのパス
            
        Returns:
            処理後のDataFrame
        """
        try:
            logger.info(f"Processing FanGraphs CSV file: {file_path}")
            
            # CSVファイルを読み込む
            df = pd.read_csv(file_path, encoding='utf-8-sig')  # FanGraphsはBOMを使用することがある
            
            # カラム名のクリーニング
            df.columns = [col.strip().replace('%', '_pct').replace('/', '_per_').lower() for col in df.columns]
            
            # 選手IDの抽出（FanGraphsのURLから）
            if 'playerid' not in df.columns and 'playername' in df.columns:
                # IDカラムがない場合は名前から選手を識別
                df['player_key'] = df['playername'].str.replace(' ', '').str.lower()
            
            # 数値データの変換
            for col in df.columns:
                # パーセント値の変換（例: 23.5% -> 0.235）
                if re.search(r'_pct$', col) and df[col].dtype == 'object':
                    df[col] = df[col].str.rstrip('%').astype('float') / 100.0
                
                # $ や , を含む金額データの変換
                elif df[col].dtype == 'object' and df[col].str.contains(r'[$,]', regex=True).any():
                    df[col] = df[col].str.replace('$', '').str.replace(',', '').astype('float')
                
                # その他の数値データの変換
                elif df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except:
                        pass
            
            logger.info(f"Successfully processed FanGraphs CSV with {len(df)} rows")
            return df
            
        except Exception as e:
            error_msg = f"Failed to process FanGraphs CSV file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def save_to_csv(self, data: Union[List[Dict[str, Any]], pd.DataFrame], 
                   output_path: Union[str, Path],
                   index: bool = False) -> str:
        """
        データをCSVファイルに保存する
        
        Args:
            data: 保存するデータ（ディクショナリのリストまたはDataFrame）
            output_path: 出力ファイルパス
            index: DataFrameのインデックスを含めるかどうか
            
        Returns:
            保存されたファイルのパス
        """
        output_path = Path(output_path)
        
        # 親ディレクトリが存在しない場合は作成
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if isinstance(data, pd.DataFrame):
                data.to_csv(output_path, index=index)
            else:
                # リストが空の場合は空のCSVを作成
                if not data:
                    with open(output_path, 'w', newline='') as f:
                        f.write('')
                    return str(output_path)
                
                # ディクショナリのリストをCSVに変換
                with open(output_path, 'w', newline='') as f:
                    fieldnames = list(data[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in data:
                        writer.writerow(row)
            
            logger.info(f"Successfully saved data to {output_path}")
            return str(output_path)
            
        except Exception as e:
            error_msg = f"Failed to save data to CSV file {output_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def read_csv_in_chunks(self, file_path: Union[str, Path], 
                          chunk_size: int = 1000, 
                          **kwargs) -> Iterator[pd.DataFrame]:
        """
        大きなCSVファイルをチャンクで読み込むイテレータを返す
        
        Args:
            file_path: CSVファイルのパス
            chunk_size: 一度に読み込む行数
            **kwargs: pd.read_csvに渡す追加パラメータ
            
        Returns:
            DataFrameのイテレータ
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            error_msg = f"CSV file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            logger.info(f"Reading CSV file in chunks: {file_path} (chunk size: {chunk_size})")
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, **kwargs):
                # 空白のカラム名を修正
                chunk.columns = [col.strip() if isinstance(col, str) else col for col in chunk.columns]
                yield chunk
                
        except Exception as e:
            error_msg = f"Failed to read CSV file in chunks {file_path}: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e


# CSVパーサーのシングルトンインスタンス
csv_parser = CSVParser()