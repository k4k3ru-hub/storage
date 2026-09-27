# OMS — 注文・execution snapshotと方式別events

2026-09-27。従来の「executions自体を履歴とする」構造を変更した破壊的更新。
`schema.proposed.sql`はTradeHub migration 001のOMS部分と一致する。

| テーブル | 役割 |
| --- | --- |
| `oms_orders` | 注文条件・現在状態・累計約定数量のsnapshot |
| `oms_order_executions` | 1回の送信／ルーティング先のexecutionのsnapshot |
| `oms_order_execution_onchain_events` | executionに属する受付・送信・結果・約定・訂正の追記履歴 |
| `oms_order_execution_fees` | executionに属する複数の費用明細と調整差分 |

1 order : N executions、1 execution : N events / N fees。
Onchainでは1 executionに1 Txを対応させる。同一TxのAtomicな複数legは複数filled event。
再送は同一execution・同一署名済みTxを使う。異なるTxは別executionを作る。
Quote・Prepare・approveは保存しない。署名済みSwapのSubmit受付から保存する。

共通executionはasset_class固有のTx・資産ペア・個別約定量を持たない。
FX向けの`oms_order_execution_forex_events`は命名方針のみ採用し、DDL／adapterは未実装。
FXの具体的な受付・部分約定・取消仕様を決めた時点で追加する。

## Snapshotと履歴

executionの`quantity`は受付時の割当数量で、注文と同じ単位。
`filled_quantity`は有効な各約定の`order_quantity`を集計する。
Atomicの途中legは`order_quantity=0`にし、異なる資産の数量を足さない。
各legの実数量・資産・decimalsはeventの`quantity` / `counter_quantity`等に保存する。

注文とexecutionの`filled_counter_quantity`は、有効約定の`order_counter_quantity`の合計。
不変の注文`specification.counterQuantityAsset`（namespace、chain/network、assetId、symbol、decimals）
を共通の集計単位とする。StoreはSwapのkindやlegを解釈しない。
metadataがある注文は未約定・全件取消時に`"0"`、有効約定に不明な寄与があればNULL。
metadataがない／nullの注文は受付時からNULLで、Atomicもこの扱いとする。
訂正は旧寄与を置換し、取消は対象寄与を除く。費用は合算・控除しない。
単純Swapのexact-inputは受取量、exact-outputは支払量をTradeHubが寄与として渡す。
小数は文字列のまま正確に集計し、両snapshotと履歴を同じtransactionで更新する。

executionの状態はpending、partially_filled、filled、succeeded、failed、rejected。
受付・送信だけならpending。結果成功だけでは約定を推測せずsucceeded、
有効な寄与が正ならpartially_filled、割当数量を満たせばfilled。
`completed_at`は確定結果の時刻。約定通知が先に到着した場合はfilledでも結果確定まではNULL。
確定結果が先の場合は、その後の約定・費用を引き続き反映する。

注文100、Aの割当60が約定、Bの割当40が失敗なら、A=filled、B=failed、注文=partially_filled/60。
全executionの完了だけで注文を終了しない。明示的な終了判断は、判断元executionの
`order_canceled` / `order_expired` / `order_rejected` / `order_failed` eventとして保存する。
これらはアプリの判断履歴であり、チェーン上のlogが存在するという意味ではない。
単発Swapではadapterが確定失敗とorder_failedを同じSQL transactionで保存する。
終了した注文に新しいexecutionを追加することは拒否し、遅着した結果・訂正は受け付ける。

`fill_corrected`は訂正後の絶対数量で有効約定を置換する。`fill_reversed`は対象約定を無効化する。
`execution_reversed`はその結果と約定を無効化してexecutionをpendingへ戻す。
明示的な注文終了は維持し、再発注は自動判断しない。費用はreversalだけで消さず、根拠のある調整を追記する。

## APIと原子性

`NewDefaultStore()` または `NewStore(orderTable, executionTable, onchainEventTable, feeTable)`。
StoreはDB接続やcommitを所有しない。書込みには呼出元の`*sql.Tx`を渡す。

初回Submitは`InsertOrder`と`AppendOnchainEvent(submission_accepted)`を同じtransactionで実施する。
後者はexecution snapshotの初回INSERTも行う。commit成功後にRPC送信する。
受付eventの`RequestedQuantity`がexecutionの割当数量になる。

`AppendOnchainEvent`は以下を同じtransaction内で行う。

1. accountを確認し、注文を`SELECT ... FOR UPDATE`でロックする。
2. events・fees・execution snapshotsをcurrent readし、再生結果と既存snapshotの一致を検査する。
3. record_keyの再通知なら内容を照合し、同じevent ID／execution IDを返す。
4. 注文内sequenceを採番し、参照・単位・根拠・費用を検証する。
5. event・feesをINSERTし、該当executionと注文のsnapshotを更新する。

書込み失敗時は呼出元がtransaction全体をrollbackする。エラー後のcommitは不可。
ロック中にRPCを呼ばない。入力構造体やsliceは変更しない。
ID=0は自動採番、Sequence=0は採番を要求する。子の親IDは省略可能で、指定した場合は一致が必要。
`AppendResult.EventID`と`ExecutionRecordID`は別のIDであり、`Execution`は更新後のsnapshot。
同じkey・同じ内容は`Duplicate=true`。内容変更は`ErrConflict`。
公開ID／同じchain・networkのTx受付の重複は`ErrDuplicate`。
`errors.Is`で上記と`ErrInvalidParameter`、`sql.ErrNoRows`を識別できる。

| 読取り | メソッド |
| --- | --- |
| 注文snapshot | `SelectOrder` / `SelectOrderForUpdate` / `SelectOrderByIdempotencyKey` |
| execution snapshot | `SelectExecution` / `ListExecutions` |
| 注文内のonchain履歴 | `ListEvents` / `SelectEventByKey` |
| eventのonchain根拠 | `SelectOnchainEvidence` |
| execution全体の費用 | `ListExecutionFees` |
| 特定eventを根拠とする費用 | `ListEventFees` |
| 復旧worker専用payload | `SelectSubmissionPayload` |

全読取りにaccountスコープを要求する。ListExecutionsはID順、ListEventsは注文内sequence順、上限200。
複数ページ・子明細の一貫した読取りにはread transactionを使う。
通常の根拠取得ではtx_payloadをSELECTしない。TxPayloadはJSON serializationからも除外する。

`ReplayOrder(order, []Event)`は連続する全履歴から注文状態と有効約定を再生する純粋関数。
Storeは有効約定をexecutionごとにも集約する。両snapshotのlast_event_sequenceは注文内sequence。
各executionでは他executionのevent番号を飛ばす。時刻順ではなく保存sequenceで処理する。

## Onchainの根拠

共通eventとOnchainEvidenceは同じonchain_eventsの1行に格納する。detailsテーブルはない。
`submission_event_id`は受付、`reference_event_id`は訂正／撤回／費用遅着の対象eventを指す。
複合FKとStore検証で同一注文・executionに限定し、既存eventへの後方参照だけを許可する。
受付の公開execution ID・execution_system・chain・network・Tx IDを後続eventでも維持する。

| chain_family | ledger_unit | event_position例 |
| --- | --- | --- |
| evm | block | `v1/log/12` |
| solana | slot | `v1/instruction/3/inner/1`、`v1/program-log/20` |
| sui | checkpoint | `v1/event/2` |

`ledger_sequence`、`ledger_id`、`tx_position`はTxの収録位置、`event_position`はTx内の根拠位置。
NULLと0を区別し、uint64最大値を保存できる。Tx ID等はcase-sensitive。
受付だけが署名済み復旧payload、signer、digestを持つ。秘密鍵・認証情報は保存しない。
結果／filledにはledgerとfinality、filledにはevent_positionが必要。
同一Tx・ledger・event_positionの約定を別キーで二重計上しない。
チェーン署名、資産識別、実receiptの検証とfinality判断はadapterが担う。
EVM/Suiの実処理はTradeHub側。Solana送信adapterはこの変更の対象外。

## 費用

feeの親`execution_record_id`はexecution snapshotへのFK。
`event_id`は費用を確定したevent。gasなら結果event、取引手数料・税ならfilled event、
遅着／訂正ならfees_recorded / fees_adjustedからreference_event_idで元の結果／約定を辿る。

**event_idは方式別テーブルへの論理参照で、SQLのFKは張らない。**
将来のforex_eventsにも同じ費用構造を使うため、`execution.event_family`で参照先を決める。
現在のAppendOnchainEventはeventとfeeを同時挿入し、同じexecution・対象種別・帰属を検証する。
直接SQLで作った不正なevent参照をDDLだけでは防げない。通常の書込みはStore経由とする。
feeの調整先は複合FKでも同じ注文・executionに限定する。

1 executionに複数の費用・通貨を保存する。数量はasset-unitの正規化decimal文字列。
`accounting_treatment`はadditional／included_in_input／included_in_output／unknown。
包含済み費用をPnLで再控除しない。Storeで通貨換算は行わない。
負のgas／rebateを許容する。訂正は元feeに対する符号付き差分を追記し、元行は更新しない。
同じ資産・精度・費用種別・包含区分・元の結果／約定への帰属を保持する。
費用revisionは約定revisionとは別に検証し、重複通知を再計上しない。

eventのfees_completeはNULL=主張なし、false=調査未完、true=確認済み。
executionのfees_completeは有効結果と全有効約定の確認状況を集約したbool。
後続のfees_recorded／fees_adjustedの主張を反映し、未知費用と確認済みゼロを区別する。
approveのgasは保存対象外。

## 検証と適用

参照API向けに`ListOrders`（所有者別、ID降順、before cursor）、
`ListOnchainEvents`（sequence昇順、after cursor）、`ListOrderFees`（ID昇順、after cursor）を提供する。
各limitは1〜200。snapshotと合わせる場合は呼出側でread-onlyのREPEATABLE READ transactionを使う。
`ListOnchainEvents`は復旧用Tx payloadとopaque protocol dataをSQLで読み込まない。
APIレスポンスはアプリ側の明示的なDTOへ変換し、Storeの型を直接JSON化しない。
所有者分離・ページ境界・payload除外は`TestMySQLReadPages`で検証する。

```sh
# storage repository
python3 go/mysql/oms/verify_store.py
cd go/mysql/oms
GOWORK=off go test ./...
GOWORK=off go vet ./...
GOWORK=off go build ./...
```

ランナーはランダム資格情報・localhostポート・tmpfsのMySQL 8.4でrace付き結合テストを実行し、終了時に削除する。
同じworkspaceのTradeHub 001とDDLを比較する。通常のgo testはDSN未指定ならDBテストをskipする。
DB時刻はUTC、DSNはparseTime=true&loc=UTCを使う。実チェーンへの送信は行わない。

検証は複数execution・部分失敗・明示終了、訂正／reversal、Atomicの中間leg、遅着／複数通貨費用、
同時追記・古いread snapshot・重複・所有者分離・rollback・snapshot改変検出・3チェーンの位置情報を含む。

現在は親ロック下で注文の全eventsとfeesを読み直す。長大な履歴への性能最適化は未実施。
通常APIでevents／feesのUPDATE・DELETEは提供しないが、DB管理者操作を防ぐトリガーは追加しない。

新001は新規DB用。既存の001適用済みDBを自動変換しない。
ローカルのTradeHubはgo.workでこのモジュールを参照する。公開依存versionの更新にはstorageの公開が必要。
TradeHub local composeのビルドは追加contextからこのStoreを取り込む。
