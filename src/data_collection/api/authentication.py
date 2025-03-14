"""
API認証関連の機能を提供するモジュール

このモジュールは、MLBデータAPIへの認証機能を提供します。
APIキーの管理やトークンベースの認証フローを実装します。
"""

import logging
import time
import os
from typing import Dict, Optional
import json
from datetime import datetime, timedelta
from pathlib import Path

# ロガーの設定
logger = logging.getLogger(__name__)

# トークンキャッシュファイルのパス
TOKEN_CACHE_FILE = Path(__file__).parent.parent.parent.parent / "data" / ".token_cache.json"


def get_auth_headers(api_key: str) -> Dict[str, str]:
    """
    APIキーを使用した認証ヘッダーを生成する
    
    Args:
        api_key: APIキー
        
    Returns:
        認証ヘッダーのディクショナリ
    """
    return {
        "X-API-Key": api_key
    }


class TokenManager:
    """
    認証トークンを管理するクラス
    
    このクラスは、認証トークンの取得、キャッシュ、リフレッシュを処理します。
    OAuth2などのトークンベースの認証が必要な場合に使用します。
    """
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None):
        """
        TokenManagerを初期化する
        
        Args:
            client_id: OAuth2クライアントID（環境変数から取得、未指定の場合）
            client_secret: OAuth2クライアントシークレット（環境変数から取得、未指定の場合）
        """
        self.client_id = client_id or os.environ.get("MLB_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("MLB_CLIENT_SECRET")
        self.token_data = None
        self.token_expiry = None
        
        # トークンキャッシュを読み込む
        self._load_token_cache()
        
        logger.debug("TokenManager initialized")
    
    def _load_token_cache(self):
        """トークンキャッシュファイルからトークンデータを読み込む"""
        if TOKEN_CACHE_FILE.exists():
            try:
                with open(TOKEN_CACHE_FILE, "r") as f:
                    cache_data = json.load(f)
                
                self.token_data = cache_data.get("token_data")
                
                # 有効期限の復元
                expiry_str = cache_data.get("token_expiry")
                if expiry_str:
                    self.token_expiry = datetime.fromisoformat(expiry_str)
                    
                logger.debug("Token cache loaded successfully")
                
            except Exception as e:
                logger.warning(f"Failed to load token cache: {e}")
                # キャッシュ読み込みエラーの場合はリセット
                self.token_data = None
                self.token_expiry = None
    
    def _save_token_cache(self):
        """トークンデータをキャッシュファイルに保存する"""
        if self.token_data and self.token_expiry:
            try:
                # キャッシュディレクトリが存在することを確認
                TOKEN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
                
                cache_data = {
                    "token_data": self.token_data,
                    "token_expiry": self.token_expiry.isoformat()
                }
                
                with open(TOKEN_CACHE_FILE, "w") as f:
                    json.dump(cache_data, f)
                
                # ファイルのパーミッションを制限（所有者のみ読み書き可能）
                os.chmod(TOKEN_CACHE_FILE, 0o600)
                
                logger.debug("Token cache saved successfully")
                
            except Exception as e:
                logger.warning(f"Failed to save token cache: {e}")
    
    def get_token(self) -> Optional[str]:
        """
        有効な認証トークンを取得する
        
        必要に応じてトークンを更新します。
        
        Returns:
            認証トークン（取得できない場合はNone）
        """
        # 有効なトークンがあるか確認
        if self._is_token_valid():
            logger.debug("Using cached token")
            return self.token_data.get("access_token")
        
        # トークンの更新が必要な場合
        if self._can_refresh_token():
            logger.debug("Refreshing token")
            success = self._refresh_token()
        else:
            logger.debug("Requesting new token")
            success = self._request_new_token()
        
        if success:
            return self.token_data.get("access_token")
        else:
            logger.error("Failed to obtain valid token")
            return None
    
    def get_auth_headers(self) -> Dict[str, str]:
        """
        認証トークンを含むリクエストヘッダーを取得する
        
        Returns:
            認証ヘッダーのディクショナリ
        """
        token = self.get_token()
        if token:
            return {
                "Authorization": f"Bearer {token}"
            }
        return {}
    
    def _is_token_valid(self) -> bool:
        """
        現在のトークンが有効かどうかを確認する
        
        Returns:
            トークンが有効な場合はTrue
        """
        if not self.token_data or not self.token_expiry:
            return False
        
        # 有効期限の10分前に更新するためのマージン
        margin = timedelta(minutes=10)
        
        return datetime.now() < (self.token_expiry - margin)
    
    def _can_refresh_token(self) -> bool:
        """
        トークンを更新できるかどうかを確認する
        
        Returns:
            更新可能な場合はTrue
        """
        return (
            self.token_data is not None and
            self.token_data.get("refresh_token") is not None
        )
    
    def _request_new_token(self) -> bool:
        """
        新しいトークンをリクエストする
        
        実際のAPIでの実装は、このメソッドを適切に実装する必要があります。
        
        Returns:
            リクエストが成功した場合はTrue
        """
        # MLB APIにはOAuth2認証が公開されていないため、
        # 実際の実装はAPIの仕様に応じて行う必要があります
        # 以下はダミー実装です
        
        logger.warning("OAuth2 token request is not implemented for MLB API")
        return False
        
        # 実際の実装例（OAuth2の場合）:
        """
        import requests
        
        try:
            response = requests.post(
                "https://example.com/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            self.token_data = token_data
            
            # 有効期限の設定
            expires_in = token_data.get("expires_in", 3600)  # デフォルト: 1時間
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # トークンをキャッシュに保存
            self._save_token_cache()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to request new token: {e}")
            return False
        """
    
    def _refresh_token(self) -> bool:
        """
        既存のリフレッシュトークンを使用してトークンを更新する
        
        実際のAPIでの実装は、このメソッドを適切に実装する必要があります。
        
        Returns:
            更新が成功した場合はTrue
        """
        # MLB APIにはOAuth2認証が公開されていないため、
        # 実際の実装はAPIの仕様に応じて行う必要があります
        # 以下はダミー実装です
        
        logger.warning("OAuth2 token refresh is not implemented for MLB API")
        return False
        
        # 実際の実装例（OAuth2の場合）:
        """
        import requests
        
        try:
            response = requests.post(
                "https://example.com/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.token_data.get("refresh_token"),
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            self.token_data = token_data
            
            # 有効期限の設定
            expires_in = token_data.get("expires_in", 3600)  # デフォルト: 1時間
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # トークンをキャッシュに保存
            self._save_token_cache()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            # リフレッシュに失敗した場合は新しいトークンを取得
            return self._request_new_token()
        """


# トークンマネージャーのシングルトンインスタンス
token_manager = TokenManager()


def get_oauth_headers() -> Dict[str, str]:
    """
    OAuth認証ヘッダーを取得する
    
    Returns:
        認証ヘッダーのディクショナリ
    """
    return token_manager.get_auth_headers()