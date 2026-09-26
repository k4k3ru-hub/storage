# OMS — 注文snapshotと追記型履歴のMySQL Store

2026-09-26。TradeHubの新001と同じ4テーブルへ更新した破壊的変更。
`schema.proposed.sql`は既存ファイル名を維持しているが、内容は新001のOMS部分に一致する。
このモジュールだけではTradeHubのPrepare／Submit／照合処理を切り替えない。

## 構成

| テーブル | 役割 |
| --- | --- |
| oms_orders | 原注文の条件、現在状態、累積約定量、反映済みsequence |
| oms_order_executions | 受付・送信・結果・約定・訂正・終了を表す追記型履歴 |
| oms_order_execution_onchain_details | 履歴ごとのTx・台帳・イベント根拠。受付だけが復旧payloadを保持 |
| oms_order_execution_fees | 履歴ごとの複数費用。訂正は原費用を参照する符号付き差分 |

Prepare・Quote・approveの履歴型はない。初回Swap Submitの受付から保存する。
executionの可変status、purpose、attempt_number、expires_at、updated_atは廃止した。
`OnchainAMMPoolSwap`、`ExecutionState`、`UpdateExecutionState`など旧APIも廃止した。
旧APIを新テーブルへ見かけ上マッピングする互換レイヤーは提供しない。

## 作成・追記とトランザクション

`NewDefaultStore()`、または
`NewStore(orderTable, executionTable, onchainDetailTable, feeTable)`で構成する。
TradeHubでは各デフォルト名へ`trade_hub_`を付ける。
StoreはDB接続・commitを所有せず、書込みは呼出元の`*sql.Tx`を受け取る。

初回Submitでは、同じtransactionで次を実施する。

1. `InsertOrder`でpending／filled_quantity=0／sequence=0の注文を作る。
2. `AppendExecution`で`submission_accepted`と検証済み送信材料を追記する。
3. commit後にRPC送信する。commit失敗時は送信しない。

`InsertOrder`は任意のasset_classに利用でき、venueはNULLにできる。
注文の`specification`は原注文条件・単位のJSON objectと正のversionを必要とする。
公開アドレス以外の資格情報や秘密鍵を注文仕様・protocol_dataへ入れない。
原注文の数量、親注文、条件を履歴追記の途中で書き換えるAPIは設けない。

`AppendExecution(ctx, tx, accountID, ExecutionRecord)`は以下を行う。

1. 所有者を確認し、親注文を`SELECT ... FOR UPDATE`でロックする。
2. 履歴をcurrent readで取得し、再生結果と保存snapshotの整合を検査する。
3. 安定record_keyが既存なら、事実・詳細・全費用の一致を確認して同じIDを返す。
4. 新規なら注文内sequenceを採番し、参照・単位・根拠・費用を検査する。
5. 履歴・onchain詳細・feesをINSERTし、再集計した注文snapshotをUPDATEする。

ID=0は自動採番。execution.Sequence=0はStore採番を要求する。
子の親ID・exec_typeは省略でき、親から補完する。指定した場合は一致が必要。
渡した構造体・sliceは変更しない。発生時刻は呼出元、保存時刻は省略時にStoreが設定する。
時刻はUTC DATETIME(6)、数量は正規化した10進文字列。`math/big`で計算し、floatを使わない。

**書込みエラー時は、呼出元がtransaction全体をrollbackする。**
SQLの子INSERT失敗などでは、そのtransaction内に先行書込みが残り得るため、エラーを無視してcommitしない。
ロック中にRPCを呼ばない。複数注文を扱う場合は親注文を固定ID順でロックする。

同じkey・同じ内容の再通知は`AppendResult.Duplicate=true`を返し、sequenceを増やさない。
自動採番ID・保存時刻を指定し直す必要はない。JSON objectのキー順・費用sliceの順は比較に影響しない。
同じkeyで数量・Tx材料・費用などが変われば`ErrConflict`。
別注文の公開execution_id／同じchain・networkのTx受付重複は`ErrDuplicate`。
`errors.Is`で`ErrInvalidParameter`、`ErrConflict`、`ErrDuplicate`、`sql.ErrNoRows`を識別できる。

## 履歴と集約規則

`ReplayOrder(order, completeHistory)`はDB非依存の再生関数。
sequence=1から連続する全履歴を渡し、snapshotと有効約定のリストを得る。
`AppendExecution`も同じ関数を使用する。個別ページや時刻順での再生は行わない。

| 事実 | exec_type | 注文への効果 |
| --- | --- | --- |
| 送信受付 | submission_accepted | 送信単位の起点。未約定はpending |
| RPCが受理／明確に拒否 | submitted / submission_rejected | 単独では注文全体を終了しない |
| 取引結果 | execution_succeeded / execution_failed | 成功receiptだけでfilledにはしない |
| 個別約定 | filled | order_quantityを累積する |
| 約定訂正 | fill_corrected | 参照した有効約定を、訂正後の絶対数量へ置換 |
| 約定取消 | fill_reversed | 参照した有効約定を累積対象から外す |
| 結果撤回 | execution_reversed | 現在の結果と同じ送信の有効約定を無効化 |
| 費用遅着／訂正 | fees_recorded / fees_adjusted | 数量を変えずに費用と確認状態を追記 |
| 根拠補足 | evidence_recorded | 対象履歴を参照して根拠を追加 |
| 注文終了 | order_canceled / order_expired / order_rejected / order_failed | 残量の処理終了。既約定量を保持 |

1公開execution_idは1送信を表し、受付・結果・複数約定で共有する。
別送信は別の受付・公開ID。submission_record_idは同じ注文の受付だけを指す。
訂正・取消・費用の参照は同じ注文・送信・execution_systemに限定する。
参照は既存履歴だけを許可し、循環参照や無効化済み約定の再訂正を拒否する。
約定訂正は同じ資産・decimalsを保持する。両側にsource_versionがある訂正・撤回は新しいrevisionを要求する。
費用のrevisionは約定のrevisionと混同せず、費用ごとの元額・既存訂正と比較する。
提供元にrevisionがない場合、提供元照会による因果関係の検証はadapterが担う。

数量100に対して30・70のfilledを追記すると、partially_filled/30 → filled/100になる。
30を20へ訂正すればpartially_filled/90へ戻り、completed_atも解除する。
残量取消済みの注文に遅着約定を追記しても、未約定残量の取消は維持する。
複数送信の一部失敗だけではfailedにしない。終了判断は明示的なorder_*履歴として渡す。
明示的な注文終了は保持する。約定の訂正・撤回から注文の再発注を自動決定しない。

要求量を満たした場合はfilled、それ未満の正の累積はpartially_filled、0ならpending。
order_quantityは注文の単位に統一し、Atomicの途中legは0にする。各legのQuantityは実数量を保持する。
quantity=NULLは未確定を表し、正の累積があってもfilledとは推測しない。
この版には数量を後から確定するAPIや注文別の特殊な完了判定はない。
初期Swapの呼出元はSubmit受付時点で注文数量を確定する。

結果のreversal後は新しい成功結果を記録してから再収録約定を追記する。
結果は同じ送信に対し有効なものを1件とし、訂正なしの矛盾した結果を拒否する。
費用は約定取消・reorgだけで消さない。還付等は根拠を持つ費用訂正で記録する。

## onchain詳細

EVM→block、Solana→slot、Sui→checkpointをledger_unitに設定する。
ledger_sequence・tx_positionのNULLと0を区別し、uint64最大値まで保存する。
Tx IDや資産識別子は大文字小文字を区別する。受信形式のchain固有の正規化はadapterが担う。

| event_position | 例 |
| --- | --- |
| EVMのlog位置 | v1/log/12 |
| SuiのTx内event位置 | v1/event/2 |
| Solanaのinstruction | v1/instruction/3 |
| Solanaの内部instruction | v1/instruction/3/inner/1 |
| Solanaのprogram log | v1/program-log/20 |

受付にはsigner・payload digest／encoding／payloadが必要で、台帳位置は持たない。
結果・約定にはledger情報とfinalityが必要。filledにはevent_positionも必要。
後続履歴のchain／network／Tx IDは受付と一致させる。
有効な結果・複数約定の台帳位置も整合させ、同じ根拠を別record_keyで二重約定にしない。
後から判明したledger_idの補足だけで別の約定にしない。
同じTxに複数event_positionを持つAtomicの約定は許容する。

`SelectOnchainDetail`はSQLでtx_payloadを取得しない。
復旧worker専用の`SelectSubmissionPayload`が、所有者を検査して受付のpayloadだけを取得する。
`OnchainDetail.TxPayload`はJSON serialization対象外。取得bytesをログ／外部APIへ流さない。

Storeはprotocol_dataのobject形式と正のversionを検査する。
version別の具体的な内容、署名検証、チェーン証拠の真正性、必要finalityの判断はadapterが担う。
この変更はSolanaの実送信adapterやAtomic戦略の実装ではない。

## 費用

gasはexecution_succeeded／execution_failed、取引手数料・税はfilled／fill_correctedに紐付ける。
fees_recorded／fees_adjustedではreference_record_idから該当する結果／約定を辿る。
onchain gasはそのTxと同じchain・networkの資産を要求する。

1履歴に複数の費用・通貨を保存できる。単位はasset-unit decimalで、base unitsではない。
正額は費用、負額は還付。asset_decimalsを超える小数・非正規化数値を拒否する。
accounting_treatmentはadditional／included_in_input／included_in_output／unknown。
PnL計算時に包含済みfeeを再控除しないための区分であり、Storeは費用を合算・通貨換算しない。
venue資産のasset_idにはvenueを含む正規識別子を渡す。表示symbolだけで資産を同一視しない。

訂正はfees_adjustedの子として元feeをadjustment_of_fee_idで参照し、符号付き差分を追記する。
同じ資産・精度・費用種別・包含区分・元の結果／約定への帰属を維持する。
訂正の訂正も元feeへ参照を向ける。複数の差分は加算可能だが、同じ提供元revisionを再計上しない。
record_keyとsource_referenceは安定した費用成分の識別子を用いる。
遅れて受信した元費用を別キー・新revisionで再挿入せず、訂正として表す。

fees_completeはその履歴時点の確認状態。NULL=主張なし、false=未完了、true=確認完了。
未取得と確認済みゼロを区別でき、遅着時には新しいfees_recordedで主張を追記する。
元履歴のfees_completeを更新しない。approve gasは保存対象外。

## 取得API

| 操作 | メソッド |
| --- | --- |
| 注文作成 | InsertOrder |
| 注文取得・ロック | SelectOrder / SelectOrderForUpdate |
| 注文作成キーで取得 | SelectOrderByIdempotencyKey |
| 履歴・詳細・feesとsnapshotを原子的に追記 | AppendExecution |
| 履歴再生・有効約定の取得 | ReplayOrder |
| 履歴の安定キーで取得 | SelectExecutionByKey |
| 注文履歴のsequence順ページ取得 | ListExecutions |
| onchain根拠の取得 | SelectOnchainDetail |
| 復旧材料の限定取得 | SelectSubmissionPayload |
| 各履歴の費用明細 | ListExecutionFees |

全DB読取りにaccountスコープを要求する。ページ上限は200。
複数ページと子詳細を一貫した時点で読む場合は呼出元のread transactionを使う。
PnLの価格源・原価対応、fee確認状態のprojection、未解決送信のworker検索は次のTradeHub工程。

現在のAppendは親ロック下で全履歴を読み直す。小規模ローカル検証向けの実装であり、
長大な注文履歴の性能最適化や別のprojection tableは導入していない。
UPDATE／DELETEの履歴APIはないが、DB管理者の直接SQL操作まで禁止するトリガーは設けない。

## 検証

storageリポジトリから:

```sh
python3 go/mysql/oms/verify_store.py
# DDLと一部制約だけを確認する場合
python3 go/mysql/oms/verify_schema.py
```

ランナーは標準ライブラリのみ。ランダムな資格情報・localhostポート・tmpfsのMySQL 8.4を作り、
`go test -race -count=1 -v ./...`を実行後に専用コンテナを削除する。既存DBへは接続しない。
同じworkspaceにk4k3ruがある場合、TradeHub001とのDDL一致も検証する。

```sh
cd go/mysql/oms
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

通常のgo testは`K4K3RU_OMS_TEST_DSN`未指定時にMySQLテストをskipする。
DDL比較だけは`K4K3RU_OMS_MIGRATION_PATH`に001の絶対パスを指定して実行できる。
DB接続は`parseTime=true&loc=UTC`とし、DBセッションもUTCで運用する。
`CreateTables`は明示的な初期DDL適用専用。通常の発注処理から呼ばない。

検証対象は部分約定、訂正・取消、失敗・reorg、複数送信／複数leg、FXの複数通貨費用、
遅着費用・負gas・訂正差分、所有者／参照分離、同時追記、古いread snapshot、rollback、3チェーンの位置情報。
Suiの0.1 SUI→0.421965 USDC、gas 0.002619432 SUIをfixtureで検証し、実チェーン送信は行わない。

## TradeHubへの次工程

TradeHubは旧storageモジュールversionと旧APIを参照しており、まだこのStoreへ接続していない。
本工程ではgo.work・TradeHub go.modを変更していない。接続時には新constructorと追記APIへの修正が必要。
Database composition、非永続Prepare／approve、Submit受付、結果照合、SDK／Agentの更新を揃える。
新001適用後でも、それらの完了前に現行Swap経路が動作するようになったことは意味しない。
今回の検証は一時DBのみで、ユーザーが適用したlocal DBへ追加変更は行わない。
