# dbx_handson
このハンズオンでは、DatabricksのFree Editionを使用してデータ分析とAI機能を体験します。以下の手順に従って進めてください。

## 前提条件
- インターネット接続環境
- Webブラウザ（Chrome、Firefox、Safari推奨）
- GitHubアカウント（リポジトリアクセス用）

## 手順

### 1. Databricks Free Editionへのサインアップ

1. ブラウザで以下のURLにアクセスします
   ```
   https://www.databricks.com/learn/free-edition
   ```

2. 「Get Started for Free」ボタンをクリック

3. サインアップフォームに必要事項を入力：
   - **Email Address**: 有効なメールアドレス
   - **First Name**: 名前
   - **Last Name**: 姓
   - **Company**: 会社名（個人の場合は「Personal」等）
   - **Job Title**: 職種

4. 「Continue」ボタンをクリック

5. メール認証を完了し、パスワードを設定

### 2. Databricks Workspaceへのアクセス

1. サインアップ完了後、Databricks Workspaceが自動的に作成されます（数分かかる場合があります）

2. Workspace作成完了の通知メールが届いたら、メール内のリンクをクリックしてログイン

3. Databricksのメイン画面が表示されることを確認

### 3. 教材リポジトリのクローン

1. **左サイドバー**の「**Workspace**」アイコンをクリック

2. ツリー構造で「**Users**」→「**あなたのメールアドレス**」フォルダを展開

3. あなたのフォルダを右クリックし、コンテキストメニューから「**Create**」を選択

4. Create画面で以下を設定：
   - 「**Git folder**」を選択
   - **Git repository URL**: 
     ```
     https://github.com/ureshiii-yu/dbx_handson
     ```
   - **Git provider**: 「**GitHub**」を選択
   - **Git folder name**: `dbx_handson`（自動入力）

5. 「**Clone**」ボタンをクリック

6. クローン完了後、左サイドバーのWorkspaceツリーに`dbx_handson`フォルダが表示されることを確認

### 4. データ準備の実行

1. **Workspace**から`dbx_handson`フォルダを展開

2. 「**0. データ準備**」ノートブックをクリックして開く

3. ノートブック画面右上の「**Connect**」ボタンをクリック

4. コンピュートオプションから「**Serverless**」を選択し、「**Connect**」をクリック

5. コンピュートの接続完了を待機（ステータスが緑色の「Connected」になるまで）

6. ノートブック上部の「**Run all**」ボタンをクリック

7. すべてのセルの実行完了まで待機（各セルの左側に緑のチェックマークが表示）

### 5. データ作成の確認

1. **左サイドバー**の「**Catalog**」アイコンをクリック

2. カタログブラウザで以下の構造が作成されていることを確認：
   - **main** → **default** → テーブル一覧
   - 作成されたテーブルが表示されていることを確認

### 6. 特徴量テーブルの作成

1. **Workspace**に戻り、「**1. AI分析**」ノートブックを開く

2. ノートブック右上の「**Connect**」ボタンから**Serverless**コンピュートをアタッチ

3. ノートブックの**最初のセル**（特徴量テーブル作成用）を実行
   - セル左上の「▶」ボタンをクリック、または`Shift + Enter`

4. セル実行完了を確認（緑のチェックマーク表示）

### 7. AutoMLの実行

1. **左サイドバー**の「**Experiments**」アイコンをクリック

2. Experiments画面上部の「**Predictions**」タブを選択

3. 「**Forecast**」ボタンをクリック

4. AutoML設定画面で以下を入力：

   **Data Configuration:**
   - **Training dataset**: 手順6で作成した特徴量テーブルを選択 例: workspace.default.feature_sales_by_product
   - **Time column**: `FISCAL_YEAR_MONTH` 
   - **Forecast frequency**: `Monthly` 
   - **Forecast horizon**: `3` を入力
   - **Prediction Target column**: 売上データの対象列を選択 例:TOTAL_NETAMOUNT
   - **Prediction data path**: `workspace.default`（任意）
   - **Prediction Table name**: `forecast_predictions_ureshino`（任意）
   - **Model registration Register to location**: `workspace.default`（任意）
   - **Model name**: `forecast_model_ureshino`（任意）

   **Advanced options** を展開
   - **Experiment name**: `forecast_experiment_ureshino`（任意）
   - **Time series identifier columns**: `REGION`
   - **Holiday region**: `Japan`
   - **Timeout**: `15 minutes`
   - そのほかの項目は自動入力のまま

5. 「**Start AutoML**」ボタンをクリック

6. AutoMLの実行開始を確認し、完了まで待機（約15分）

### 8. 結果確認関数の作成

1. 「**1. AI分析**」ノートブックに戻る

2. 結果確認用の関数セルを実行（約5分）
   - 「地域別の売上予測を返す関数」のセルの中のコメントアウト部分に予測結果のテーブル名を入力し、コメントアウトを外す
   -  例) FROM workspace.default.forecast_predictions_ureshino
   -  「地域別の売上予測を返す関数」のセルの「▶」ボタンをクリック
   -  「指定された期間の月別・プロダクト別の売上のテーブルを返す関数」のセルの「▶」ボタンをクリック
   - 残りのセルの「▶」ボタンを順番にクリック

3. 関数作成完了を確認

### 9. Playgroundでの関数設定

1. **左サイドバー**の「**Playground**」アイコンをクリック

2. Playground画面右側の「**Tools**」セクションを確認

3. 「**Add Tool**」または「**+**」ボタンをクリック

4. ツール選択画面で、手順8で作成した関数を選択

5. 「**Add**」ボタンをクリックして関数を追加

### 10. 初回分析の実行

1. Playgroundのチャット入力欄に以下の質問を入力：

```
2019年4,5,6月の地域別・製品別の売上についてレポートを作成しなさい
```

2. 「**Send**」ボタンまたは`Enter`キーで送信

3. AIが関数を使用してデータ分析を実行し、レポートを生成することを確認

### 11. 予測分析の実行

1. AutoMLの実行が完了したことを確認（手順7）

2. Playgroundで以下の質問を入力：

```
2024年4,5,6月の地域別・製品別の売上についてレポートを作成し、著しい増減があれば指摘しなさい。また2024年7月の売上予測を加味して、特に注意が必要な地域を指摘してください。
```

3. 「**Send**」ボタンで送信

4. AIが売上データの分析と予測結果を組み合わせた包括的なレポートを生成することを確認

## トラブルシューティング

### よくある問題と解決方法

- **コンピュートが接続できない場合**: ページを再読み込みし、再度接続を試行
- **ノートブックの実行が失敗する場合**: セルを個別に実行し、エラーメッセージを確認
- **AutoMLが開始できない場合**: データの準備が完了していることを確認
- **関数が見つからない場合**: ノートブックでの関数作成が正常に完了していることを確認

## 完了確認

以下すべてが完了していることを確認してください：

- ✅ Databricks Workspaceへのアクセス
- ✅ 教材リポジトリのクローン
- ✅ データ準備の完了
- ✅ 特徴量テーブルの作成
- ✅ AutoMLの正常実行
- ✅ Playgroundでの分析レポート生成
- ✅ 予測を含む包括的レポートの生成

これでDatabricksを使用したデータ分析とAI機能のハンズオンが完了です。
