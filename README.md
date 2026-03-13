# Hormuz Transit Index

AIS データを使ってホルムズ海峡の通過隻数を24時間監視し、`Hormuz Transit Index` を生成する Python プロジェクトです。

初期版の設計方針:

- `MarineTraffic` を最優先ソース
- `MarineTraffic` 未設定時は `AISStream`、さらに未設定時は `replay`
- 24時間収集 / SQLite 保存 / FastAPI 提供
- 東向き・西向きの両方向を通過判定
- Discord/Webhook 通知対応
- 商用 AIS API へ差し替えやすい構成

## 実装済みの範囲

- `AISStream` 互換の WebSocket 収集器
- サンプル JSONL の replay 収集器
- ホルムズ海峡用の2ゲート通過判定
- SQLite への raw event / transit / index / alert 保存
- 1時間通過数、24時間通過数、24時間指数の算出
- FastAPI エンドポイント
- Discord アラートの送信フック
- pytest ベースの単体テスト

## ディレクトリ

- `src/hormuz_index/`: アプリ本体
- `sample_data/`: replay 用サンプル AIS データ
- `tests/`: テスト

## セットアップ

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pip install -e .
copy .env.example .env
```

`.env` の主要項目:

- `AISSTREAM_API_KEY`: AISStream の API キー
- `DISCORD_WEBHOOK_URL`: Discord 通知先
- `DATABASE_URL`: SQLite 保存先
- `WEST_GATE_LON`, `EAST_GATE_LON`: 通過判定ゲート
- `VESSEL_SCOPE`: 初期値は `all_merchant`

`C:\Users\hp\Documents\Investment\.env` には 2026-03-13 時点で `MARINETRAFFIC_API_KEY` は存在しなかったため、live で `MarineTraffic` を使うには追加設定が必要です。

現状のままでも `sample_data/ais_sample.jsonl` を使った replay 動作は可能です。

## 使い方

### 1. サンプルデータで収集

```bash
python -m hormuz_index.cli run-collector --provider replay
```

### 2. 指数を1回計算

```bash
python -m hormuz_index.cli run-indexer-once
```

### 3. API を起動

```bash
python -m hormuz_index.cli run-api
```

### 4. API 例

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/index/latest
curl http://127.0.0.1:8010/index/history?hours=168
curl http://127.0.0.1:8010/transits/recent?limit=20
```

## 収集モード

- `AIS_PROVIDER=auto`: `MarineTraffic` -> `AISStream` -> `replay` の順で選択
- `--provider marinetraffic`: MarineTraffic API を利用
- `--provider aisstream`: AISStream WebSocket を利用
- `--provider replay`: ローカル JSONL を再生

`MarineTraffic` は公式 docs の `Vessel Positions in an Area of Interest` 前提で、`jsono`、`timespan`、`limit`、`cursor` を利用する実装です。
`MARINETRAFFIC_API_KEY` が未設定でもアプリは停止せず、従来ロジックとして `AISStream`、さらに未設定なら `replay` に自動フォールバックします。

## 指数の定義

- `count_1h`: 直近1時間の確定通過隻数
- `count_24h`: 直近24時間の確定通過隻数
- `baseline_24h_median`: 過去 index point の `count_24h` 中央値
- `index_24h`: `count_24h / baseline_24h_median * 100`

履歴が不足している間は `index_24h` は `null` になります。

## アラート

以下を評価します。

- AIS 入力停止
- 24時間指数の閾値割れ
- 1時間通過数のベースライン割れ

通知を使う場合は `.env` で以下を設定してください。

```env
DISCORD_ALERT_ENABLED=true
DISCORD_WEBHOOK_URL=...
DISCORD_ALERT_COOLDOWN_MIN=30
```

疎通確認:

```bash
curl -X POST http://127.0.0.1:8010/alerts/test
```

## Windows 24時間運用の想定

- `run-collector` を常駐
- `run-indexer-loop --interval-sec 300` を常駐
- `run-api` を常駐

実運用では Windows Task Scheduler またはサービス化ツールで再起動復旧を設定してください。

## 注意点

- 公開 AIS は欠損、遅延、spoofing の影響を受けます。
- 投資判断の実運用で精度を上げる場合は、将来的に商用 AIS API を追加する前提です。
- public repository には秘密情報を含めないでください。

## 追加で必要になる可能性が高い API

優先ソースは `MarineTraffic AIS API` です。加えて精度比較や冗長化が必要なら以下を候補にしてください。

- Spire Maritime
- ORBCOMM
- AISStream
