"""
データの同期とバックアップを管理するモジュール

このモジュールは、MLBデータの差分更新、同期、
バックアップなどの機能を提供します。
"""

import logging
import os
import json
import shutil
import zipfile
import sqlite3
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import tempfile

from ..config import config
from .local_storage import local_storage
from ...utils.logging import log_execution_time

# ロガーの設定
logger = logging.getLogger(__name__)


class DataSynchronizer:
    """
    データの同期を管理するクラス
    
    このクラスは、データの増分更新、マージ、
    競合解決などの機能を提供します。
    """
    
    def __init__(self, sync_log_path: Optional[Union[str, Path]] = None):
        """
        DataSynchronizerを初期化する
        
        Args:
            sync_log_path: 同期ログファイルのパス（Noneの場合は設定から生成）
        """
        if sync_log_path:
            self.sync_log_path = Path(sync_log_path)
        else:
            self.sync_log_path = config.data_dir / "sync_log.json"
        
        # 同期ログの初期化
        self._initialize_sync_log()
        
        logger.debug(f"DataSynchronizer initialized with log path: {self.sync_log_path}")
    
    def _initialize_sync_log(self):
        """同期ログファイルを初期化する"""
        if not self.sync_log_path.exists():
            # 親ディレクトリが存在することを確認
            self.sync_log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # デフォルトの同期ログを作成
            default_log = {
                "last_sync": {},
                "sync_history": []
            }
            
            with open(self.sync_log_path, 'w') as f:
                json.dump(default_log, f, indent=4)
            
            logger.info(f"Created new sync log at {self.sync_log_path}")
    
    def _load_sync_log(self) -> Dict[str, Any]:
        """
        同期ログを読み込む
        
        Returns:
            同期ログのディクショナリ
        """
        try:
            with open(self.sync_log_path, 'r') as f:
                sync_log = json.load(f)
            
            return sync_log
            
        except Exception as e:
            logger.error(f"Failed to load sync log: {e}")
            # エラーが発生した場合は新しいログを作成
            default_log = {
                "last_sync": {},
                "sync_history": []
            }
            return default_log
    
    def _save_sync_log(self, sync_log: Dict[str, Any]):
        """
        同期ログを保存する
        
        Args:
            sync_log: 保存する同期ログ
        """
        try:
            with open(self.sync_log_path, 'w') as f:
                json.dump(sync_log, f, indent=4)
            
            logger.debug("Sync log updated")
            
        except Exception as e:
            logger.error(f"Failed to save sync log: {e}")
    
    def update_last_sync(self, data_type: str, timestamp: Optional[datetime] = None):
        """
        最終同期情報を更新する
        
        Args:
            data_type: データタイプ（players, teams, stats, pitches など）
            timestamp: 同期タイムスタンプ（Noneの場合は現在時刻）
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # 同期ログを読み込む
        sync_log = self._load_sync_log()
        
        # 最終同期情報を更新
        sync_log["last_sync"][data_type] = timestamp.isoformat()
        
        # 同期履歴に追加
        sync_history_entry = {
            "type": data_type,
            "timestamp": timestamp.isoformat(),
            "status": "success"
        }
        sync_log["sync_history"].append(sync_history_entry)
        
        # 履歴が長すぎる場合は古いエントリを削除（最新100件を保持）
        if len(sync_log["sync_history"]) > 100:
            sync_log["sync_history"] = sync_log["sync_history"][-100:]
        
        # 更新したログを保存
        self._save_sync_log(sync_log)
        
        logger.info(f"Updated last sync for {data_type}: {timestamp.isoformat()}")
    
    def get_last_sync(self, data_type: str) -> Optional[datetime]:
        """
        特定のデータタイプの最終同期日時を取得する
        
        Args:
            data_type: データタイプ
            
        Returns:
            最終同期日時、または未同期の場合はNone
        """
        sync_log = self._load_sync_log()
        
        last_sync_str = sync_log["last_sync"].get(data_type)
        if last_sync_str:
            try:
                return datetime.fromisoformat(last_sync_str)
            except ValueError:
                logger.warning(f"Invalid timestamp format in sync log for {data_type}: {last_sync_str}")
                return None
        
        return None
    
    def record_sync_error(self, data_type: str, error_message: str):
        """
        同期エラーを記録する
        
        Args:
            data_type: データタイプ
            error_message: エラーメッセージ
        """
        sync_log = self._load_sync_log()
        
        # エラーエントリを作成
        error_entry = {
            "type": data_type,
            "timestamp": datetime.now().isoformat(),
            "status": "error",
            "message": error_message
        }
        
        # 同期履歴に追加
        sync_log["sync_history"].append(error_entry)
        
        # 更新したログを保存
        self._save_sync_log(sync_log)
        
        logger.error(f"Recorded sync error for {data_type}: {error_message}")
    
    def get_sync_history(self, data_type: Optional[str] = None, 
                        limit: int = 10) -> List[Dict[str, Any]]:
        """
        同期履歴を取得する
        
        Args:
            data_type: フィルタするデータタイプ（Noneの場合は全て）
            limit: 取得する最大エントリ数
            
        Returns:
            同期履歴のリスト
        """
        sync_log = self._load_sync_log()
        
        # 新しい順に並べ替え
        history = sorted(
            sync_log["sync_history"],
            key=lambda x: x.get("timestamp", ""),
            reverse=True
        )
        
        # データタイプでフィルタリング
        if data_type:
            history = [entry for entry in history if entry.get("type") == data_type]
        
        # 指定された数に制限
        return history[:limit]
    
    def merge_data(self, source_data: List[Dict[str, Any]], 
                  target_data: List[Dict[str, Any]], 
                  key_field: str) -> List[Dict[str, Any]]:
        """
        2つのデータセットをマージする
        
        Args:
            source_data: ソースデータのリスト
            target_data: ターゲットデータのリスト
            key_field: マージに使用するキーフィールド
            
        Returns:
            マージされたデータのリスト
        """
        # インデックスの作成
        target_index = {item[key_field]: item for item in target_data if key_field in item}
        
        # 結果のリスト
        merged_data = list(target_data)
        
        # ソースデータを処理
        for source_item in source_data:
            # キーフィールドが存在することを確認
            if key_field not in source_item:
                logger.warning(f"Item in source data missing key field '{key_field}': {source_item}")
                continue
            
            key = source_item[key_field]
            
            if key in target_index:
                # 既存アイテムを更新
                target_item = target_index[key]
                
                # timestamp フィールドをチェックして新しい方を採用
                if ('updated_at' in source_item and 'updated_at' in target_item and
                    source_item['updated_at'] > target_item['updated_at']):
                    # ソースアイテムの方が新しい場合は更新
                    idx = merged_data.index(target_item)
                    merged_data[idx] = source_item
                    target_index[key] = source_item
                    
                elif 'updated_at' not in target_item:
                    # ターゲットにtimestampがない場合は更新
                    idx = merged_data.index(target_item)
                    merged_data[idx] = source_item
                    target_index[key] = source_item
            else:
                # 新しいアイテムを追加
                merged_data.append(source_item)
                target_index[key] = source_item
        
        return merged_data
    
    def detect_changes(self, old_data: List[Dict[str, Any]], 
                      new_data: List[Dict[str, Any]], 
                      key_field: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        2つのデータセット間の変更を検出する
        
        Args:
            old_data: 古いデータのリスト
            new_data: 新しいデータのリスト
            key_field: 比較に使用するキーフィールド
            
        Returns:
            (追加されたアイテム, 更新されたアイテム, 削除されたアイテム) のタプル
        """
        old_keys = {item[key_field] for item in old_data if key_field in item}
        new_keys = {item[key_field] for item in new_data if key_field in item}
        
        # 追加されたキー
        added_keys = new_keys - old_keys
        # 更新されたキー
        updated_keys = old_keys.intersection(new_keys)
        # 削除されたキー
        removed_keys = old_keys - new_keys
        
        # 古いデータのインデックス
        old_index = {item[key_field]: item for item in old_data if key_field in item}
        # 新しいデータのインデックス
        new_index = {item[key_field]: item for item in new_data if key_field in item}
        
        # 追加されたアイテム
        added_items = [new_index[key] for key in added_keys]
        
        # 更新されたアイテム（中身が変更されたもののみ）
        updated_items = []
        for key in updated_keys:
            old_item = old_index[key]
            new_item = new_index[key]
            
            # 更新日時をチェック
            if ('updated_at' in new_item and 'updated_at' in old_item and
                new_item['updated_at'] > old_item['updated_at']):
                updated_items.append(new_item)
            # 内容の比較（更新日時がない場合）
            elif json.dumps(old_item, sort_keys=True) != json.dumps(new_item, sort_keys=True):
                updated_items.append(new_item)
        
        # 削除されたアイテム
        removed_items = [old_index[key] for key in removed_keys]
        
        return added_items, updated_items, removed_items


class DataBackup:
    """
    データのバックアップを管理するクラス
    
    このクラスは、データベースとファイルのバックアップ、
    リストア、ローテーションなどの機能を提供します。
    """
    
    def __init__(self, backup_dir: Optional[Union[str, Path]] = None):
        """
        DataBackupを初期化する
        
        Args:
            backup_dir: バックアップディレクトリのパス（Noneの場合は設定から取得）
        """
        self.backup_dir = Path(backup_dir) if backup_dir else config.backup_dir
        self.db_path = config.database_path
        
        # バックアップディレクトリが存在することを確認
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # バックアップ間隔（日数）
        self.backup_interval_days = config.data_collection.get(
            "backup_interval_days", 7
        )
        
        # 保持するバックアップの最大数
        self.max_backups = 10
        
        logger.debug(f"DataBackup initialized with backup directory: {self.backup_dir}")
    
    @log_execution_time
    def create_database_backup(self, tag: Optional[str] = None) -> Optional[Path]:
        """
        データベースのバックアップを作成する
        
        Args:
            tag: バックアップに付けるタグ（Noneの場合は日付のみ）
            
        Returns:
            バックアップファイルのパス、または失敗した場合はNone
        """
        try:
            # 最新のバックアップ時刻を確認
            last_backup = self._get_last_backup_time()
            
            # バックアップ間隔内に既にバックアップがある場合はスキップ
            if last_backup:
                elapsed_days = (datetime.now() - last_backup).days
                if elapsed_days < self.backup_interval_days:
                    logger.info(f"Skipping backup, last backup was {elapsed_days} days ago")
                    return None
            
            # バックアップファイル名の生成
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"mlb_pitcher_data_{timestamp}"
            
            if tag:
                backup_filename += f"_{tag}"
            
            backup_filename += ".sqlite"
            backup_path = self.backup_dir / backup_filename
            
            # データベースが存在しない場合はエラー
            if not self.db_path.exists():
                logger.error(f"Database file not found: {self.db_path}")
                return None
            
            # データベース接続を閉じる
            local_storage.close()
            
            # バックアップの作成
            logger.info(f"Creating database backup: {backup_path}")
            shutil.copy2(self.db_path, backup_path)
            
            # 古いバックアップを削除
            self._rotate_backups()
            
            logger.info(f"Database backup created successfully: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to create database backup: {e}")
            return None
    
    def create_zip_backup(self, include_raw_data: bool = True, 
                         tag: Optional[str] = None) -> Optional[Path]:
        """
        データディレクトリの圧縮バックアップを作成する
        
        Args:
            include_raw_data: 生データディレクトリを含めるかどうか
            tag: バックアップに付けるタグ（Noneの場合は日付のみ）
            
        Returns:
            バックアップファイルのパス、または失敗した場合はNone
        """
        try:
            # バックアップファイル名の生成
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"mlb_pitcher_data_{timestamp}"
            
            if tag:
                backup_filename += f"_{tag}"
            
            backup_filename += ".zip"
            backup_path = self.backup_dir / backup_filename
            
            # データベース接続を閉じる
            local_storage.close()
            
            # ZIPファイルの作成
            logger.info(f"Creating ZIP backup: {backup_path}")
            
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # データベースファイルの追加
                if self.db_path.exists():
                    zipf.write(self.db_path, arcname=self.db_path.name)
                
                # 処理済みデータディレクトリの追加
                processed_dir = config.processed_data_dir
                if processed_dir.exists():
                    for file_path in processed_dir.glob('**/*'):
                        if file_path.is_file():
                            rel_path = file_path.relative_to(config.data_dir)
                            zipf.write(file_path, arcname=str(rel_path))
                
                # 生データディレクトリの追加（オプション）
                if include_raw_data:
                    raw_dir = config.raw_data_dir
                    if raw_dir.exists():
                        for file_path in raw_dir.glob('**/*'):
                            if file_path.is_file():
                                rel_path = file_path.relative_to(config.data_dir)
                                zipf.write(file_path, arcname=str(rel_path))
            
            # 古いバックアップを削除
            self._rotate_backups(pattern="*.zip")
            
            logger.info(f"ZIP backup created successfully: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to create ZIP backup: {e}")
            return None
    
    def restore_database(self, backup_path: Union[str, Path]) -> bool:
        """
        データベースバックアップからリストアする
        
        Args:
            backup_path: リストアするバックアップファイルのパス
            
        Returns:
            リストアが成功した場合はTrue
        """
        backup_path = Path(backup_path)
        
        try:
            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False
            
            # データベース接続を閉じる
            local_storage.close()
            
            # 既存のデータベースのバックアップを作成
            if self.db_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                pre_restore_backup = self.db_path.with_name(f"{self.db_path.name}.pre_restore_{timestamp}")
                shutil.copy2(self.db_path, pre_restore_backup)
                logger.info(f"Created pre-restore backup: {pre_restore_backup}")
            
            # バックアップからリストア
            logger.info(f"Restoring database from backup: {backup_path}")
            shutil.copy2(backup_path, self.db_path)
            
            logger.info("Database restored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore database: {e}")
            return False
    
    def restore_from_zip(self, backup_path: Union[str, Path], 
                        restore_raw_data: bool = False) -> bool:
        """
        ZIPバックアップからリストアする
        
        Args:
            backup_path: リストアするZIPバックアップファイルのパス
            restore_raw_data: 生データも復元するかどうか
            
        Returns:
            リストアが成功した場合はTrue
        """
        backup_path = Path(backup_path)
        
        try:
            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False
            
            # データベース接続を閉じる
            local_storage.close()
            
            # 既存のデータベースのバックアップを作成
            if self.db_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                pre_restore_backup = self.db_path.with_name(f"{self.db_path.name}.pre_restore_{timestamp}")
                shutil.copy2(self.db_path, pre_restore_backup)
                logger.info(f"Created pre-restore backup: {pre_restore_backup}")
            
            # 一時ディレクトリを作成
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # ZIPファイルを展開
                logger.info(f"Extracting ZIP backup: {backup_path}")
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    zipf.extractall(temp_path)
                
                # データベースファイルの復元
                db_name = self.db_path.name
                temp_db_path = temp_path / db_name
                if temp_db_path.exists():
                    shutil.copy2(temp_db_path, self.db_path)
                    logger.info(f"Restored database file: {self.db_path}")
                
                # 処理済みデータの復元
                processed_dir = config.processed_data_dir
                temp_processed_dir = temp_path / "processed"
                if temp_processed_dir.exists():
                    if processed_dir.exists():
                        shutil.rmtree(processed_dir)
                    shutil.copytree(temp_processed_dir, processed_dir)
                    logger.info(f"Restored processed data directory: {processed_dir}")
                
                # 生データの復元（オプション）
                if restore_raw_data:
                    raw_dir = config.raw_data_dir
                    temp_raw_dir = temp_path / "raw"
                    if temp_raw_dir.exists():
                        if raw_dir.exists():
                            shutil.rmtree(raw_dir)
                        shutil.copytree(temp_raw_dir, raw_dir)
                        logger.info(f"Restored raw data directory: {raw_dir}")
            
            logger.info("Restore from ZIP completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore from ZIP: {e}")
            return False
    
    def list_backups(self, backup_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        利用可能なバックアップのリストを取得する
        
        Args:
            backup_type: バックアップタイプ（"db", "zip", Noneの場合は全て）
            
        Returns:
            バックアップ情報のリスト
        """
        try:
            backups = []
            
            if backup_type is None or backup_type == "db":
                # データベースバックアップを検索
                for file_path in self.backup_dir.glob("*.sqlite"):
                    stats = file_path.stat()
                    backups.append({
                        "path": str(file_path),
                        "filename": file_path.name,
                        "type": "database",
                        "size": stats.st_size,
                        "created": datetime.fromtimestamp(stats.st_mtime),
                        "created_str": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            if backup_type is None or backup_type == "zip":
                # ZIPバックアップを検索
                for file_path in self.backup_dir.glob("*.zip"):
                    stats = file_path.stat()
                    backups.append({
                        "path": str(file_path),
                        "filename": file_path.name,
                        "type": "zip",
                        "size": stats.st_size,
                        "created": datetime.fromtimestamp(stats.st_mtime),
                        "created_str": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            # 作成日時でソート（新しい順）
            backups.sort(key=lambda x: x["created"], reverse=True)
            
            return backups
            
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
            return []
    
    def _get_last_backup_time(self) -> Optional[datetime]:
        """
        最新のデータベースバックアップの日時を取得する
        
        Returns:
            最新バックアップの日時、またはバックアップがない場合はNone
        """
        try:
            backups = list(self.backup_dir.glob("*.sqlite"))
            if not backups:
                return None
            
            # 最新のバックアップを検索
            newest_backup = max(backups, key=lambda p: p.stat().st_mtime)
            
            # 作成日時を返す
            return datetime.fromtimestamp(newest_backup.stat().st_mtime)
            
        except Exception as e:
            logger.error(f"Failed to get last backup time: {e}")
            return None
    
    def _rotate_backups(self, pattern: str = "*.sqlite"):
        """
        古いバックアップを削除する
        
        Args:
            pattern: 対象ファイルのパターン
        """
        try:
            # バックアップファイルのリストを取得
            backups = list(self.backup_dir.glob(pattern))
            
            # バックアップの数が最大数を超えている場合
            if len(backups) > self.max_backups:
                # 作成日時でソート（古い順）
                backups.sort(key=lambda p: p.stat().st_mtime)
                
                # 古いバックアップを削除
                for backup in backups[:-self.max_backups]:
                    logger.info(f"Removing old backup: {backup}")
                    backup.unlink()
            
        except Exception as e:
            logger.error(f"Failed to rotate backups: {e}")


# データ同期マネージャのシングルトンインスタンス
data_synchronizer = DataSynchronizer()

# データバックアップマネージャのシングルトンインスタンス
data_backup = DataBackup()