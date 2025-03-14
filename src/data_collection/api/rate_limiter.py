"""
APIリクエストのレート制限を管理するモジュール

このモジュールは、APIへのリクエスト頻度を制御し、
レート制限エラーの発生を防止するための機能を提供します。
"""

import logging
import time
from typing import Callable, TypeVar, Any
import random
from datetime import datetime

# ロガーの設定
logger = logging.getLogger(__name__)

# ジェネリック型の定義
T = TypeVar('T')


class RateLimiter:
    """
    APIリクエストのレート制限を管理するクラス
    
    このクラスは、APIリクエストの間隔を制御し、
    レート制限エラーが発生した場合のバックオフとリトライを処理します。
    """
    
    def __init__(self, rate_limit_ms: int = 500, max_retries: int = 3, retry_backoff_base: float = 2.0):
        """
        RateLimiterを初期化する
        
        Args:
            rate_limit_ms: リクエスト間の最小間隔（ミリ秒）
            max_retries: 最大リトライ回数
            retry_backoff_base: リトライ時のバックオフ計算の基準値（指数バックオフに使用）
        """
        self.rate_limit_ms = rate_limit_ms
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.last_request_time = 0
        
        logger.debug(f"RateLimiter initialized with rate_limit_ms={rate_limit_ms}, max_retries={max_retries}")
    
    def wait_if_needed(self):
        """リクエスト間隔を確保するために必要に応じて待機する"""
        current_time_ms = int(time.time() * 1000)
        elapsed_ms = current_time_ms - self.last_request_time
        
        if elapsed_ms < self.rate_limit_ms and self.last_request_time > 0:
            wait_time_ms = self.rate_limit_ms - elapsed_ms
            wait_time_s = wait_time_ms / 1000
            
            logger.debug(f"Rate limiting: waiting {wait_time_s:.2f} seconds")
            time.sleep(wait_time_s)
        
        self.last_request_time = int(time.time() * 1000)
    
    def execute(self, request_func: Callable[[], T]) -> T:
        """
        レート制限を適用してリクエストを実行する
        
        Args:
            request_func: 実行するリクエスト関数（引数なし、任意の戻り値）
            
        Returns:
            リクエスト関数の戻り値
            
        Raises:
            RuntimeError: 最大リトライ回数を超えてもリクエストが成功しない場合
        """
        retries = 0
        last_error = None
        
        while retries <= self.max_retries:
            try:
                # リクエスト間隔を確保
                self.wait_if_needed()
                
                # リクエストを実行
                return request_func()
                
            except Exception as e:
                last_error = e
                retries += 1
                
                # 最大リトライ回数に達した場合はエラーを発生
                if retries > self.max_retries:
                    logger.error(f"Max retries ({self.max_retries}) exceeded")
                    raise RuntimeError(f"Request failed after {self.max_retries} retries") from last_error
                
                # バックオフ時間の計算（指数バックオフ+ジッター）
                backoff_seconds = self._calculate_backoff_time(retries)
                
                logger.warning(f"Request failed (retry {retries}/{self.max_retries}): {str(e)}")
                logger.warning(f"Backing off for {backoff_seconds:.2f} seconds")
                
                # バックオフ
                time.sleep(backoff_seconds)
        
        # ここには到達しないはずだが、念のため
        raise RuntimeError("Unexpected error in rate limiter execution")
    
    def _calculate_backoff_time(self, retry_count: int) -> float:
        """
        バックオフ時間を計算する（指数バックオフ+ジッター）
        
        Args:
            retry_count: 現在のリトライ回数
            
        Returns:
            バックオフ時間（秒）
        """
        # 基本的な指数バックオフ
        base_backoff = self.retry_backoff_base ** retry_count
        
        # ジッターを追加（基本バックオフの0〜20%）
        jitter = random.uniform(0, 0.2 * base_backoff)
        
        # 合計バックオフ時間
        total_backoff = base_backoff + jitter
        
        return total_backoff


class TimeWindowRateLimiter(RateLimiter):
    """
    時間枠ベースのレート制限を管理するクラス
    
    このクラスは、指定された時間枠内でのリクエスト数を制限します。
    例えば、「1分間に最大60リクエスト」といった制限を実装できます。
    """
    
    def __init__(self, max_requests: int, time_window_seconds: int, max_retries: int = 3, retry_backoff_base: float = 2.0):
        """
        TimeWindowRateLimiterを初期化する
        
        Args:
            max_requests: 時間枠内の最大リクエスト数
            time_window_seconds: 時間枠の長さ（秒）
            max_retries: 最大リトライ回数
            retry_backoff_base: リトライ時のバックオフ計算の基準値
        """
        super().__init__(0, max_retries, retry_backoff_base)
        self.max_requests = max_requests
        self.time_window_seconds = time_window_seconds
        self.request_timestamps = []
        
        logger.debug(f"TimeWindowRateLimiter initialized with max_requests={max_requests}, "
                   f"time_window_seconds={time_window_seconds}")
    
    def wait_if_needed(self):
        """
        時間枠内のリクエスト数を制限するために必要に応じて待機する
        """
        current_time = time.time()
        
        # 時間枠外のタイムスタンプを削除
        window_start_time = current_time - self.time_window_seconds
        self.request_timestamps = [ts for ts in self.request_timestamps if ts >= window_start_time]
        
        # 時間枠内のリクエスト数をチェック
        if len(self.request_timestamps) >= self.max_requests:
            # 最も古いリクエストのタイムスタンプ
            oldest_timestamp = min(self.request_timestamps)
            
            # 次のリクエストまでの待機時間を計算
            wait_time = (oldest_timestamp + self.time_window_seconds) - current_time
            
            if wait_time > 0:
                logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds to stay within {self.max_requests} "
                           f"requests per {self.time_window_seconds} seconds")
                time.sleep(wait_time)
        
        # 現在のリクエストのタイムスタンプを記録
        self.request_timestamps.append(time.time())