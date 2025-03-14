# コーディング規約

## 基本方針
- PEP 8に準拠する
- Blackによるコードフォーマット
- isortによるインポート順序整理
- 関数・クラスにはdocstringを必ず記述
- 型ヒントを積極的に使用

## 命名規則
- クラス名: UpperCamelCase
- 関数・メソッド名: snake_case
- 変数名: snake_case
- 定数: UPPER_SNAKE_CASE
- モジュール名: snake_case

## ドキュメント
- すべての関数・クラスにはdocstringを記述
- docstringはGoogle styleを採用

## テスト
- 新しい機能には必ずテストを書く
- テストカバレッジは80%以上を目標とする

## コードレビュー
- PRにはコードレビューを必須とする
- レビュアーは最低1名以上
- CIテストが通過していることを確認