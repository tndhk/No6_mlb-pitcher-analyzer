"""
MLB Stats APIとの通信を行うクライアントモジュール

このモジュールは、MLB Stats APIとの通信を抽象化し、
選手、チーム、試合データなどの取得を容易にする機能を提供します。
"""

import logging
import time
from typing import Dict, List, Optional, Any, Union, Tuple
import requests
import json
from datetime import datetime, timedelta

from ..config import config
from .rate_limiter import RateLimiter
from .authentication import get_auth_headers

# ロガーの設定
logger = logging.getLogger(__name__)


class MLBApiClient:
    """
    MLB Stats APIとの通信を行うクライアントクラス
    
    このクラスは、MLB Stats APIへのリクエスト送信、レスポンス処理、
    エラーハンドリングなどの機能を提供します。
    """
    
    def __init__(self):
        """MLB API クライアントの初期化"""
        self.base_url = config.mlb_api["base_url"]
        self.endpoints = config.mlb_api["endpoints"]
        self.api_key = config.mlb_api["api_key"]
        self.user_agent = config.mlb_api["user_agent"]
        self.timeout = config.mlb_api["timeout_seconds"]
        
        # レート制限の設定
        self.rate_limiter = RateLimiter(
            rate_limit_ms=config.mlb_api["rate_limit_ms"],
            max_retries=config.mlb_api["max_retries"],
            retry_backoff_base=config.mlb_api["retry_backoff_base"]
        )
        
        logger.info(f"MLBApiClient initialized with base URL: {self.base_url}")
    
    def _get_headers(self) -> Dict[str, str]:
        """リクエストヘッダーを生成する"""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json"
        }
        
        # 認証ヘッダーの追加（APIキーがある場合）
        if self.api_key:
            auth_headers = get_auth_headers(self.api_key)
            headers.update(auth_headers)
        
        return headers
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        APIにリクエストを送信し、レスポンスを処理する
        
        Args:
            endpoint: APIエンドポイントのパス
            params: クエリパラメータ
            
        Returns:
            レスポンスのJSONデータ
            
        Raises:
            requests.RequestException: リクエストに失敗した場合
            ValueError: レスポンスのパースに失敗した場合
            RuntimeError: リトライ回数を超えてもリクエストが成功しない場合
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        logger.debug(f"Making request to {url} with params: {params}")
        
        # レート制限を考慮してリクエスト実行
        return self.rate_limiter.execute(
            lambda: self._send_request(url, headers, params)
        )
    
    def _send_request(self, url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        HTTPリクエストを送信し、レスポンスを処理する内部メソッド
        
        Args:
            url: リクエスト先のURL
            headers: リクエストヘッダー
            params: クエリパラメータ
            
        Returns:
            レスポンスのJSONデータ
            
        Raises:
            requests.RequestException: リクエストに失敗した場合
            ValueError: レスポンスのパースに失敗した場合
        """
        try:
            response = requests.get(
                url, 
                headers=headers, 
                params=params, 
                timeout=self.timeout
            )
            
            # エラーレスポンスの処理
            response.raise_for_status()
            
            # レスポンスのJSONパース
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            # APIエラーレスポンスの詳細を取得（可能な場合）
            try:
                error_data = response.json()
                logger.error(f"API error details: {error_data}")
            except Exception:
                logger.error(f"Raw response: {response.text[:1000]}")
            raise
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            raise
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}, Raw response: {response.text[:1000]}")
            raise ValueError(f"Failed to parse API response: {e}")
    
    # ----- MLB API エンドポイント別メソッド -----
    
    def get_player(self, player_id: int) -> Dict[str, Any]:
        """
        選手情報を取得する
        
        Args:
            player_id: 選手ID
            
        Returns:
            選手情報のデータ
        """
        endpoint = self.endpoints["player"].format(player_id=player_id)
        return self._make_request(endpoint)
    
    def get_player_stats(self, player_id: int, 
                        stats_type: str = "season", 
                        group: str = "pitching",
                        season: Optional[int] = None) -> Dict[str, Any]:
        """
        選手の統計データを取得する
        
        Args:
            player_id: 選手ID
            stats_type: 統計タイプ ("season", "career", "yearByYear" など)
            group: 統計グループ ("pitching", "hitting" など)
            season: シーズン年度（指定しない場合は最新）
            
        Returns:
            選手の統計データ
        """
        endpoint = self.endpoints["player_stats"].format(player_id=player_id)
        
        params = {
            "stats": stats_type,
            "group": group
        }
        
        if season:
            params["season"] = season
        
        return self._make_request(endpoint, params)
    
    def get_team(self, team_id: int) -> Dict[str, Any]:
        """
        チーム情報を取得する
        
        Args:
            team_id: チームID
            
        Returns:
            チーム情報のデータ
        """
        endpoint = self.endpoints["team"].format(team_id=team_id)
        return self._make_request(endpoint)
    
    def get_teams(self, season: Optional[int] = None, active_only: bool = True) -> Dict[str, Any]:
        """
        全チームのリストを取得する
        
        Args:
            season: シーズン年度（指定しない場合は現在）
            active_only: アクティブなチームのみ取得するかどうか
            
        Returns:
            チームリストのデータ
        """
        params = {}
        
        if season:
            params["season"] = season
        
        if active_only:
            params["activeStatus"] = "ACTIVE"
        
        return self._make_request(self.endpoints["teams"], params)
    
    def get_game(self, game_pk: int) -> Dict[str, Any]:
        """
        試合情報を取得する
        
        Args:
            game_pk: 試合ID
            
        Returns:
            試合情報のデータ
        """
        endpoint = self.endpoints["game"].format(game_pk=game_pk)
        return self._make_request(endpoint)
    
    def get_schedule(self, date: Optional[str] = None, 
                    start_date: Optional[str] = None, 
                    end_date: Optional[str] = None,
                    team_id: Optional[int] = None,
                    season: Optional[int] = None,
                    game_type: Optional[str] = None) -> Dict[str, Any]:
        """
        試合スケジュールを取得する
        
        Args:
            date: 特定日（YYYY-MM-DD形式）
            start_date: 開始日（YYYY-MM-DD形式）
            end_date: 終了日（YYYY-MM-DD形式）
            team_id: チームID
            season: シーズン年度
            game_type: 試合タイプ ("R" for regular season, "P" for postseason, etc.)
            
        Returns:
            スケジュールデータ
        """
        params = {}
        
        # 日付パラメータの設定
        if date:
            params["date"] = date
        elif start_date and end_date:
            params["startDate"] = start_date
            params["endDate"] = end_date
        
        # その他のパラメータ
        if team_id:
            params["teamId"] = team_id
        
        if season:
            params["season"] = season
        
        if game_type:
            params["gameType"] = game_type
        
        # 常にhydrateパラメータを追加して詳細データを取得
        params["hydrate"] = "team,linescore,stats,probablePitcher"
        
        return self._make_request(self.endpoints["schedule"], params)
    
    # ----- 便利なヘルパーメソッド -----
    
    def search_players(self, name: str) -> List[Dict[str, Any]]:
        """
        選手を名前で検索する
        
        Args:
            name: 検索する選手名
            
        Returns:
            検索結果の選手リスト
        """
        # MLB APIには直接の検索エンドポイントがないため、
        # ワークアラウンドとしてscheduleエンドポイントを使用
        params = {
            "sportId": 1,  # MLB
            "personNames": name
        }
        
        response = self._make_request(self.endpoints["schedule"], params)
        
        # レスポンスから選手情報を抽出
        players = []
        if "dates" in response and response["dates"]:
            for date in response["dates"]:
                if "games" in date:
                    for game in date["games"]:
                        for team_type in ["away", "home"]:
                            if team_type in game and "players" in game[team_type]:
                                players.extend(game[team_type]["players"])
        
        # 重複を除去
        unique_players = {}
        for player in players:
            if player["id"] not in unique_players:
                unique_players[player["id"]] = player
        
        return list(unique_players.values())
    
    def get_pitcher_list(self, season: int, team_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        投手のリストを取得する
        
        Args:
            season: シーズン年度
            team_id: チームID（指定されている場合は特定チームの投手のみ）
            
        Returns:
            投手リスト
        """
        # チームのロスターを取得
        params = {
            "season": season,
            "gameType": "R"  # レギュラーシーズン
        }
        
        if team_id:
            # 特定チームの投手を取得
            endpoint = f"/teams/{team_id}/roster"
            params["rosterType"] = "active"
        else:
            # 全チームの投手を取得するため、まずチームリストを取得
            teams_response = self.get_teams(season=season)
            pitchers = []
            
            for team in teams_response.get("teams", []):
                team_id = team["id"]
                team_pitchers = self.get_pitcher_list(season, team_id)
                pitchers.extend(team_pitchers)
            
            return pitchers
        
        response = self._make_request(endpoint, params)
        
        # レスポンスから投手のみをフィルタリング
        pitchers = []
        for player in response.get("roster", []):
            position = player.get("position", {}).get("abbreviation", "")
            if position == "P":  # 投手（Pitcher）
                # 詳細情報を取得
                player_id = player["person"]["id"]
                player_info = self.get_player(player_id)
                
                if "people" in player_info and player_info["people"]:
                    pitchers.append(player_info["people"][0])
        
        return pitchers
    
    def get_recent_pitching_performances(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        最近の投手成績を取得する
        
        Args:
            days: 過去何日分のデータを取得するか
            
        Returns:
            最近の投手成績リスト
        """
        # 日付範囲の計算
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # 日付文字列のフォーマット（YYYY-MM-DD）
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # スケジュールの取得
        schedule_response = self.get_schedule(
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        # 投手成績の抽出
        pitching_performances = []
        
        for date in schedule_response.get("dates", []):
            for game in date.get("games", []):
                game_pk = game["gamePk"]
                
                # 試合データの取得
                game_data = self.get_game(game_pk)
                
                # 各チームの投手データを抽出
                for team_type in ["away", "home"]:
                    if "boxscore" in game_data and "teams" in game_data["boxscore"]:
                        team_boxscore = game_data["boxscore"]["teams"].get(team_type, {})
                        
                        if "pitchers" in team_boxscore:
                            for pitcher_id in team_boxscore["pitchers"]:
                                # 投手の成績データ
                                pitcher_stats = team_boxscore["players"].get(f"ID{pitcher_id}", {})
                                
                                if "stats" in pitcher_stats and "pitching" in pitcher_stats["stats"]:
                                    performance = {
                                        "player_id": pitcher_id,
                                        "player_name": pitcher_stats.get("person", {}).get("fullName", ""),
                                        "game_id": game_pk,
                                        "game_date": date["date"],
                                        "team_id": team_boxscore.get("team", {}).get("id", 0),
                                        "team_name": team_boxscore.get("team", {}).get("name", ""),
                                        **pitcher_stats["stats"]["pitching"]
                                    }
                                    
                                    pitching_performances.append(performance)
        
        return pitching_performances


# クライアントのシングルトンインスタンス
mlb_api_client = MLBApiClient()