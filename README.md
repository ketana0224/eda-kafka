# EDA アーキテクチャ検討

## 1. 目的

マイクロサービス + EDA（Event-Driven Architecture）によるECサイト業務アプリケーションを Kafka を利用して検討する。
本検討では以下の観点が必要。

| 観点 | 内容 |
|---|---|
| マイクロサービス | アプリケーション間を疎結合にできること |
| 非同期通信 | サービス間をイベントで疎結合にできること |
| サービス独立性 | 各サービスが互いの内部実装を知らずに協調できること |
| イベント順序 | Kafka のパーティション単位でイベント順序が保証されること |
| 冪等性 | 同一イベントの重複受信でも副作用が発生しないこと |
| 補償フロー | 在庫不足などのエラー時に補償イベントで整合性を回復できること |

---

## 2. EDA パターン候補

本検証で採用するパターンの位置付けを整理する。

パターンは 異なるレイヤー の選択肢で、互いに排他ではなく組み合わせて使うものとする。

レイヤー別マッピング

| レイヤー | 問い | 該当パターン |
|---|---|---|
| ① 制御フロー | フロー全体を誰が駆動するか | Choreography：各サービスがイベントに反応して自律的にフローを進める（中央制御なし）<br>Orchestration Saga：中央の Orchestrator がフロー全体のステップ・補償・タイムアウトを一元管理する（**Process Manager** とも呼ばれる。Saga は補償フローに着目した用語、Process Manager は状態遷移・ルーティング制御に着目した用語だが、実装上は同一コンポーネントになることが多い） |
| ② イベント設計 | イベントに何を載せるか | Event Notification：ID のみ通知し、受信者が別途クエリで詳細取得する（発行元サービスへの問い合わせが**必要**）<br>Event-Carried State Transfer（ECST）：受信者が処理に必要な情報をイベントに含め、追加クエリなしで自律処理できる（発行元サービスへの問い合わせが**不要**）<br>Fat Event：全状態を含む大型イベント。受信者の自律性は最高だがスキーマ密結合・PII 拡散リスクあり（発行元サービスへの問い合わせが**不要**）<br>⚠️ ペイロードが大きい場合の実装選択（② の下位判断）：Kafka デフォルト上限（1MB）を超える場合は **Claim Check** を適用する。本体を Blob Storage に格納し、Kafka には参照 URL のみ載せる |
| ③ 状態管理 | 業務エンティティの最新値（注文ステータス・在庫数・配送ステータス等）をどう保持・再構築するか | Event Sourcing：状態変化をすべてイベントとして記録し、イベント列から現在状態を再構築する（例：在庫数 = 初期値 + 全引当イベントの差分合算）<br>CQRS：書き込みモデル（引当処理）と読み取りモデル（在庫照会）を分離し、参照系を最適化する<br>State-in-DB：通常の RDB に最新値のみ保持する（例：在庫数を直接 UPDATE する）（有限状態機械 + ACID 保証） |
| ④ 信頼性保証 | DB と Kafka の整合性をどう担保するか | Transactional Outbox：DB 更新とイベント発行を同一トランザクションで行い、Dual-Write 問題を解消する<br>Inbox / Idempotent Consumer：受信済みイベントを DB に記録し、重複処理を防ぐ<br>Dual-Write のまま（Outbox なし）：DB 更新後に直接 `Kafka.send()`。サービスクラッシュ時にイベントがロストする（at-most-once）<br>非冪等のまま（Inbox なし）：at-least-once をそのまま受け入れ重複処理を許容。在庫の二重引当・重複配送が発生しうる |

つまり、たとえば実装は 「Choreography ＋ Event Notification ＋ Outbox」 のように 複数レイヤーの選択を重ねた構成 になります。

---

## 3. 検証システム要件

EC サイト業務アプリケーションを対象とする。業務は大きく 3 つに分かれる。

1. 注文受付
2. 在庫引当
3. 配送手配

### 3.1 注文受付（OrderService）

| 要件 | 内容 |
|---|---|
| 注文受付 | 顧客からの注文（商品・数量・配送先）を受け付け、注文 ID を採番する |
| ステータス管理 | 注文ステータスを管理する（`PENDING` → `CONFIRMED` / `CANCELLED`） |
| 冪等性 | 同一注文の重複送信でも注文が二重登録されない |
| 補償処理 | 在庫不足通知を受けた場合、注文をキャンセルし補償イベントを発行する |
| 注文履歴参照 | 顧客は自分の注文一覧・詳細を参照できる |

### 3.2 在庫引当（InventoryService）

| 要件 | 内容 |
|---|---|
| 在庫管理 | 商品ごとの在庫数量を保持・更新する |
| 在庫引当 | 注文イベント受信時に必要数量を引き当て、在庫を減算する |
| 在庫不足通知 | 必要数量が確保できない場合は在庫不足イベントを発行する |
| 引当ロールバック | 注文キャンセルイベントを受信した場合、引当済み在庫を戻す |
| 冪等性 | 同一注文 ID の引当要求を二重処理しない |
| 在庫参照 | 商品ごとの現在在庫数を参照できる |

### 3.3 配送手配（ShippingService）

| 要件 | 内容 |
|---|---|
| 配送スケジュール作成 | 在庫引当完了イベントを受けて配送スケジュール（配送業者・配送日）を作成する |
| 配送日算出 | 引当完了日から +2 営業日を配送予定日とする |
| ステータス管理 | 配送ステータスを管理する（`SCHEDULED` → `IN_TRANSIT` → `DELIVERED`） |
| 冪等性 | 同一注文 ID に対してスケジュールを二重作成しない |
| 配送情報参照 | 注文に紐づく配送 ID・配送予定日を参照できる |

### 3.4 共通非機能要件

| R# | 要件 | 内容 |
|---|---|---|
| R1 | 疎結合 | サービス間の同期 API 呼び出し（サービス A が別のサービス B の内部 REST API を直接呼ぶ）を行わない。サービス間通信は Kafka 経由の非同期イベントで行う。各サービスが外部クライアント向けに REST API を持つことは妨げない |
| R2 | サービス独立性 | 各サービスは独立してデプロイ・スケールアウト可能 |
| R3 | データ独立性 | 各サービスは自サービス専用のデータストアを持ち、他サービスのデータストアを直接参照しない |
| R4 | 順序保証 | 同一注文 ID のイベントは Kafka パーティションキーにより順序を保証する |
| R5 | 冪等性（全サービス横断） | 同一操作の重複実行で結果が変わらない。同一注文 ID・引当 ID・配送 ID に対して二重処理しない |
| R6 | 補償処理 | 在庫不足通知を受けた場合、注文をキャンセルし補償イベントを発行する（OrderService） |
| R7 | 非線形フロー対応 | 分岐（並列引当）・マージ（部分発送集約）・ループ（再引当リトライ）・タイムアウト（自動キャンセル）を含むフロー形状に対応できる |
| R8 | 耐障害性 | 一時的なサービス停止中に発行されたイベントをロストしない（Kafka の永続化を利用） |
| R9 | 再処理可能性 | Consumer 障害回復後に未処理イベントをオフセットから再処理できる |

> **大規模・安全性重視システムにおける適切なデータストア（⚠️ 一般的な実装事例から）**
>
> | サービス | 推奨データストア | 理由 |
> |---|---|---|
> | OrderService | PostgreSQL（RDBMS） | 注文ステータス遷移はトランザクション整合性が必須。`SELECT FOR UPDATE` による楽観的/悲観的ロックで二重注文を防ぐ。ACID 保証が安全性の基盤 |
> | InventoryService | PostgreSQL ＋ Redis | 在庫の加減算は競合制御（行ロック）のため RDBMS が安全。ホットな在庫数の読取りキャッシュに Redis を併用してスループットを確保 |
> | ShippingService | PostgreSQL（RDBMS） | 配送ステータス遷移・配送業者割当の整合性管理に RDBMS が適切。大規模化で構造が多様化した場合は NoSQL（MongoDB 等）を検討 |
>
> いずれのサービスも **DB インスタンスは物理的に分離**（別ホスト・別スキーマではなく別インスタンス）することでデータ独立性を完全に担保する。  

---

## 4. EDA パターン適合判定

### 4.0 評価方針

#### 採点基準

| 記号 | 意味 | 定義 |
|:---:|---|---|
| ◎ | 標準で充足 | パターンの標準機能・設計原則だけで要件を満たす |
| ○ | 軽微な追加実装で充足 | 小規模な追加設計・実装（設定変更・ユーティリティ追加程度）で要件を満たす |
| △ | 大きな追加実装が必要 | 別途アーキテクチャ追加や大規模な補助設計が必要であり、運用・保守コストも増大する |
| × | 本質的に困難 | パターンの設計思想と要件が根本的に相反し、実現しても利点を打ち消す |

#### 評価上の前提

1. 各レイヤーは独立に比較するが、実運用では **レイヤー間に依存関係** がある
   - R7（非線形フロー）は主にレイヤー①（制御フロー）の寄与が大きい
   - R8/R9（耐障害性・再処理可能性）は主にレイヤー④（信頼性保証）の寄与が大きい
   - R1/R3（疎結合・データ独立性）はレイヤー②（イベント設計）の選択で悪化しうる
2. 「実装可能か」ではなく **「破綻なく運用できるか」** を判定基準とする
3. 追加前提・運用上の工夫・責務分散が必要な場合は、○ ではなく △ とする

#### R7（非線形フロー）の分解評価

R7 は複合要件であるため、以下の 4 項目に分解して評価し、**最弱項目に引っ張られる形で総合判定** する。

| サブ要件 | 内容 | 代表的なシナリオ |
|---|---|---|
| R7a 分岐 | 条件に応じて処理パスを切り替えられる | 在庫あり→引当 / 在庫なし→代替品提案 |
| R7b マージ | 複数の並行処理結果を集約して次ステップに進められる | 複数倉庫への並列引当→部分発送集約 |
| R7c ループ / 再試行 | 条件付きで処理を繰り返せる | 在庫引当失敗→3回リトライ→バックオーダー |
| R7d タイムアウト | 一定時間内に応答がない場合に補償・代替処理を起動できる | 引当応答 30 分なし→自動キャンセル |

---

### 4.1 適合度判定表

#### レイヤー① 制御フロー

| 要件 | Choreography | Orchestration Saga | 
|---|:---:|:---:|
| **R1** 疎結合 | ◎ | △ |
| **R2** サービス独立性 | ◎ | ○ |
| **R3** データ独立性 | ◎ | ◎ |
| **R4** 順序保証 | ○ | ◎ |
| **R5** 冪等性 | △ | ○ |
| **R6** 補償処理 | △ | ◎ |
| **R7** 非線形フロー（総合） | △ | ◎ |
| **R8** 耐障害性 | ○ | ○ |
| **R9** 再処理可能性 | ○ | ○ |

<details>
<summary>R7 分解評価</summary>

| サブ要件 | Choreography | Orchestration Saga |
|---|:---:|:---:|
| R7a 分岐 | ○ | ◎ |
| R7b マージ | △ | ◎ |
| R7c ループ / 再試行 | △ | ◎ |
| R7d タイムアウト | × | ◎ |

</details>

**判定根拠**

| 要件 | 根拠 |
|---|---|
| R1 | Choreography は Kafka pub/sub のみで完結し疎結合性が最高。Orchestration は指揮者サービスへの論理的依存が生まれる |
| R5 | Choreography は各サービスが個別に冪等実装を行い統一が困難。Orchestration は指揮者で一元管理しやすい |
| R6 | Choreography の補償は各サービスに分散実装され追跡が困難。Orchestration は指揮者が補償フローを明示的に制御できる（✅ 事実: Chris Richardson『Microservices Patterns』Ch.4） |
| R7 | Choreography は分岐までは対応可能だが、マージ条件判定・再試行回数管理・タイムアウト監視の責務が分散し運用が破綻しやすい。特に R7d タイムアウトは期限監視の責務が曖昧になるため本質的に困難。Orchestration（Temporal / Camunda 等）は条件分岐・タイムアウト・リトライを標準サポートする |
| R8 | どちらも Kafka の永続性に依存する。Choreography は単一障害点なし。Orchestration は指揮者の冗長化が必要 |

---

#### レイヤー② イベント設計

| 要件 | Event Notification | ECST | Fat Event | Claim Check 併用 |
|---|:---:|:---:|:---:|:---:|
| **R1** 疎結合 | △ | ◎ | ○ | ◎ |
| **R2** サービス独立性 | △ | ◎ | ○ | ◎ |
| **R3** データ独立性 | △ | ◎ | ○ | ◎ |
| **R4** 順序保証 | ○ | ◎ | ○ | ○ |
| **R5** 冪等性 | ○ | ◎ | ○ | ○ |
| **R6** 補償処理 | ○ | ○ | ○ | ○ |
| **R7** 非線形フロー | ○ | ○ | ○ | ○ |
| **R8** 耐障害性 | △ | ◎ | ○ | ◎ |
| **R9** 再処理可能性 | △ | ◎ | ○ | ◎ |

**判定根拠**

| 要件 | 根拠 |
|---|---|
| R1 | Event Notification は受信側が発信元 API に同期問い合わせする必要があり、R1「サービス間の同期 API 呼び出しを行わない」に厳密には抵触する（✅ 事実: [microservices.io ECST](https://microservices.io/patterns/data/event-carried-state-transfer.html)）。ECST は受信側がローカルで状態を保持し追加クエリ不要 |
| R4 | ECST はバージョン番号 / タイムスタンプをペイロードに含めることで順序整合を検証できる |
| R8 | Event Notification は依存先 API ダウン時に連鎖障害リスクがある。ECST / Claim Check は自律処理可能 |
| R9 | ECST はイベント自体が状態を持つためリプレイで状態を再構築できる。Event Notification はリプレイ時に発信元の「当時の状態」が取得できない |

> **⚠️ 推測・解釈**: Fat Event と ECST の境界は文献により定義が異なります。本表では「ECST＝受信者が処理に必要な情報をイベントに含め自律処理できる設計」「Fat Event＝エンティティの全状態を含む大型イベント」として区別しています。

---

#### レイヤー③ 状態管理

> **⚠️ 評価上の注意**: 状態管理はサービス **内部** の設計選択であり、R1〜R4・R8 といったサービス **間** の要件への寄与は限定的です。これらの要件は主にレイヤー①（制御フロー）・②（イベント設計）・④（信頼性保証）で決まります。本表では **状態管理レイヤー固有の寄与のみ** を評価し、他レイヤーとの組み合わせ効果は含めません。

| 要件 | Event Sourcing | CQRS | State-in-DB |
|---|:---:|:---:|:---:|
| **R1** 疎結合 | ○ | ○ | ○ |
| **R2** サービス独立性 | ○ | ○ | ○ |
| **R3** データ独立性 | ○ | ○ | ○ |
| **R4** 順序保証 | ○ | ○ | ○ |
| **R5** 冪等性 | ○ | ○ | ○ |
| **R6** 補償処理 | ○ | ○ | ○ |
| **R7** 非線形フロー | ○ | ○ | ○ |
| **R8** 耐障害性 | ○ | ○ | ○ |
| **R9** 再処理可能性 | ◎ | △ | ○ |

**判定根拠**

| 要件 | 根拠 |
|---|---|
| R1〜R4 | 状態管理はサービス内部の設計選択であり、サービス間の疎結合・独立性・データ独立性・順序保証への寄与は本質的に同等。いずれも専用 DB を持つ前提（セクション 3.4 R3）で差がつかない。順序保証は Kafka パーティション設計（レイヤー④）に依存する |
| R5 | いずれのパターンも Inbox（レイヤー④）併用が前提。Event Sourcing はイベントバージョンでの重複検知が自然にできるが、end-to-end の冪等性は Inbox パターンが担うため、状態管理レイヤー単独での差は小さい |
| R6 | 補償処理の制御はレイヤー①（Orchestrator）が主管。Event Sourcing は補償イベントを append するだけで整合性を保てる点は優れるが、補償ロジック自体はいずれのパターンでも実装が必要 |
| R7 | 非線形フローは主にレイヤー①の寄与。状態管理レイヤーの影響は限定的 |
| R8 | 耐障害性は主に Kafka 永続化 + Outbox（レイヤー④）に依存する。Event Sourcing はイベントログから状態を再構築できるが、それは R9（再処理可能性）の特性であり R8 とは区別する |
| R9 | **状態管理レイヤーで唯一大きな差がつく要件**。Event Sourcing はイベント再生により任意時点の状態を復元可能（◎）。State-in-DB は Kafka オフセットからの再処理自体は可能だが過去状態の完全再構築はできない（○）。CQRS 単独では読み取りモデルの再構築に追加設計が必要（△）（✅ 事実: [Microsoft Learn - CQRS](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs)） |

> **⚠️ レイヤー③の本質的な選択基準**: R1〜R9 の適合度ではレイヤー③の差は小さく、**R9（再処理可能性）が唯一の明確な差別化ポイント** です。実際の選択は以下の **非機能要件外の観点** で判断すべきです。
>
> | 観点 | Event Sourcing | CQRS | State-in-DB |
> |---|---|---|---|
> | 実装複雑性 | 高（スナップショット・スキーマ進化管理必須） | 中（DB 2 系統の運用） | 低（既存技術の延長） |
> | 学習コスト | 高 | 中 | 低 |
> | 監査証跡 | 完全（全イベント記録） | 限定的 | なし（別途実装要） |
> | 将来拡張性 | 高（分析・ML・不正検知基盤） | 中（読み取り最適化） | 低（最新値のみ） |
> | 運用コスト | 高 | 中 | 低 |
>
> ✅ 事実: [Microsoft Learn - Event Sourcing](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing) にて「ほとんどのシステムでは従来型データ管理で十分」と明記されています。
>
> **⚠️ 補足**: Event Sourcing と CQRS は独立した概念であり、常にセットで適用する必要はありません。Event Sourcing なしの CQRS（読み書き DB 分離のみ）も有効な選択肢です。

---

#### レイヤー④ 信頼性保証

| 要件 | Transactional Outbox | Inbox (Idempotent Consumer) | Dual-Write | 非冪等 |
|---|:---:|:---:|:---:|:---:|
| **R1** 疎結合 | ◎ | ◎ | ○ | ○ |
| **R2** サービス独立性 | ◎ | ◎ | ○ | △ |
| **R3** データ独立性 | ◎ | ◎ | △ | △ |
| **R4** 順序保証 | ◎ | ○ | △ | × |
| **R5** 冪等性 | ○ | ◎ | × | × |
| **R6** 補償処理 | ◎ | ○ | △ | × |
| **R7** 非線形フロー | ○ | ○ | △ | × |
| **R8** 耐障害性 | ◎ | ◎ | × | △ |
| **R9** 再処理可能性 | ◎ | ◎ | × | × |

**判定根拠**

| 要件 | 根拠 |
|---|---|
| R4 | Outbox は DB トランザクション順序＝Kafka publish 順序が CDC 経由で保証される（✅ 事実: [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)） |
| R5 | Inbox パターンが冪等性の中核。Outbox 単体は配信保証であり重複排除ではない。Dual-Write は原子性がなく二重書き込みリスク（✅ 事実: [microservices.io Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)） |
| R8 | Dual-Write は DB 書き込み後・Kafka publish 前のクラッシュで不整合が発生する。EC 業務では在庫引当の二重実行等の重大障害に直結する |
| R9 | Outbox レコードが残るため再送が容易。Inbox により再処理時の重複防止も保証される |

> **⚠️ 補足**: Outbox + Inbox の組み合わせにより at-least-once × 冪等 ＝ effectively-once を実現できます。ただし、外部 HTTP API や他 DB への副作用を含む完全な exactly-once は別途設計が必要です（✅ 事実: [Confluent - Delivery Semantics](https://docs.confluent.io/kafka/design/delivery-semantics.html)）。

---

### 4.2 パターン別 長所・短所

#### レイヤー① 制御フロー

**Choreography Saga**

| 観点 | 内容 |
|---|---|
| 長所 | 完全な疎結合（各サービスが互いを知らない）。新サービス追加が容易（トピックをリッスンするだけ）。Kafka の pub/sub モデルに自然にフィット。単一障害点なし |
| 短所 | 非線形フロー（在庫なし→代替→タイムアウト）の実装が本質的に困難。補償処理の追跡が分散しデバッグが難しい（分散トレーシング必須）。ビジネスフローの全体像がコードに現れず暗黙的（"Implicit Workflow" 問題）。Aggregator や Timeout Manager を追加すると事実上 Process Manager 化する |

**Orchestration Saga**

| 観点 | 内容 |
|---|---|
| 長所 | 複雑な分岐・タイムアウト・ループを明示的に実装可能。補償処理が指揮者に集約され追跡・監査が容易。Temporal / Camunda 等の OSS が成熟（Kafka 統合あり）。フロー全体が 1 箇所で可視化・管理できる |
| 短所 | 指揮者サービスが "神クラス" 化するリスク。指揮者への論理的依存（サービス独立性が若干損なわれる）。指揮者の冗長化・スケールアウト設計が必要 |

---

#### レイヤー② イベント設計

**Event Notification**

| 観点 | 内容 |
|---|---|
| 長所 | イベントペイロードが小さく帯域効率が良い。受信側が最新状態を API 参照できる |
| 短所 | 受信側が発信元 API に同期呼び出しする必要がある（R1 疎結合を損なう）。発信元ダウン時に処理が連鎖停止する。イベントリプレイ時に発信元の過去状態が取得できない |

**ECST（Event-Carried State Transfer）**

| 観点 | 内容 |
|---|---|
| 長所 | 受信側がローカルで状態を持てるため完全な疎結合を実現。発信元ダウン時もキャッシュで継続処理可能。イベントリプレイで状態を再構築できる（R9 充足）。在庫確認・価格参照等の読み取り負荷を分散できる |
| 短所 | ペイロードサイズが大きくなる（→ Claim Check で緩和可能）。受信側の「読み取り遅延」（結果整合性）。スキーマ変更の影響範囲が広い（Schema Registry による管理が必要） |

**Claim Check（補助パターン）**

| 観点 | 内容 |
|---|---|
| 長所 | 大きなペイロード（注文明細の大量 SKU 等）を Kafka に乗せずに済む。ECST / Fat Event のペイロード肥大問題を解消 |
| 短所 | 外部ストレージ（Blob Storage 等）への依存追加。オブジェクト保管の寿命管理・整合性管理が必要 |

---

#### レイヤー③ 状態管理

**Event Sourcing**

| 観点 | 内容 |
|---|---|
| 長所 | 注文履歴の完全な監査証跡（「いつ何が起きたか」全記録）。補償イベントの追記のみで状態を巻き戻せる。任意時点の状態再構築（バグ調査・リプレイ）。将来の分析・ML・不正検知にも活用可能 |
| 短所 | 実装複雑性が高い（スナップショット戦略が必須）。スキーマ進化管理が困難（過去イベントのマイグレーション問題）。読み取り最適化に CQRS が事実上必須でアーキテクチャが増大。チームへの学習コストが高い |

**CQRS**

| 観点 | 内容 |
|---|---|
| 長所 | 読み取り最適化 DB と書き込み DB を独立してスケール可能。EC 業務の高頻度読み取り（商品検索・注文一覧）に最適 |
| 短所 | 書き込みと読み取りの結果整合性ラグを許容する必要がある。管理対象 DB が 2 系統になり運用コスト増 |

**State-in-DB（従来型）**

| 観点 | 内容 |
|---|---|
| 長所 | 実装が最もシンプルで既存技術の延長。チームの学習コストが低い。SQL でのアドホック集計・レポートが容易 |
| 短所 | 完全な再処理可能性（過去状態の復元）は弱い。複雑な補償処理では巻き戻しロジックが煩雑になりやすい |

---

#### レイヤー④ 信頼性保証

**Transactional Outbox（+ Debezium CDC）**

| 観点 | 内容 |
|---|---|
| 長所 | DB トランザクションと Kafka publish の原子性を保証。Debezium の EventRouter SMT で Kafka トピックへの自動ルーティング。本番実績多数（2024 年時点で最もスタンダードな実装）。既存 DB に outbox テーブルを追加するだけで導入可能 |
| 短所 | Debezium（Kafka Connect）の運用管理コスト。WAL 設定が必要（PostgreSQL: `wal_level=logical`）。outbox テーブルの肥大化に対してパージ戦略が必要 |

**Inbox（Idempotent Consumer）**

| 観点 | 内容 |
|---|---|
| 長所 | メッセージの冪等受信を保証（Kafka の at-least-once 配信に対応）。イベント ID を inbox テーブルで管理することで重複処理を排除 |
| 短所 | inbox テーブルの管理・パージが必要。処理済みイベント ID の保存期間設計が必要 |

**Dual-Write / 非冪等**

| 観点 | 内容 |
|---|---|
| 非推奨理由 | Dual-Write は DB 書き込み後・Kafka publish 前のクラッシュで不整合が発生し、EC 業務では在庫二重引当等の重大障害に直結する。非冪等は Kafka の at-least-once 配信下で重複処理を防止できず再処理も安全に行えない。いずれも初期開発コストは低いが、障害時の調査・修復コストが極めて高く、総所有コストでは Outbox + Inbox の方が安価になりやすい |

---

### 4.3 推奨構成

#### 棄却案と理由

推奨構成の検討に先立ち、本検討で主案から除外するパターンとその理由を明記する。

| 除外パターン | 理由 |
|---|---|
| Choreography を制御フロー主体に採用 | R7（非線形フロー）と R6（補償処理）の運用負荷が高い。EC 業務では「在庫なし→バックオーダー→タイムアウト→補償キャンセル」等の非線形フローが頻発するため、Choreography 単独では対応困難 |
| Event Notification をイベント設計の標準に採用 | R1（疎結合）・R3（データ独立性）を損なう。受信側が発信元 API に同期問い合わせする設計はセクション 3.4 R1「サービス間の同期 API 呼び出しを行わない」に抵触する |
| Dual-Write / 非冪等を信頼性保証に採用 | R5（冪等性）・R8（耐障害性）・R9（再処理可能性）が本質的に充足不可能。本検討では採用不可 |

---

#### 案 A: 実用性優先型（推奨）

```
① 制御フロー  : Orchestration Saga
② イベント設計 : ECST + Claim Check（大ペイロード時）
③ 状態管理   : State-in-DB
④ 信頼性保証  : Transactional Outbox（Debezium） + Inbox（Idempotent Consumer）
```

**選定理由**

| 観点 | 根拠 |
|---|---|
| R6・R7 の充足 | Orchestration（Temporal 等）により補償フロー・非線形フローを明示的に制御 |
| R1・R3 の充足 | ECST により各サービスがローカルキャッシュを持ち、発信元への同期依存をゼロにする |
| R5・R8 の充足 | Outbox + Inbox で at-least-once × 冪等 ＝ effectively-once を実現 |
| 実装コスト | State-in-DB により学習コストを抑え、既存チームの技術延長で構築可能 |

**トレードオフ**

- Orchestrator（Temporal / Camunda）の習熟コストが必要
- Debezium（Kafka Connect クラスタ）の運用負荷が追加される
- R9（再処理可能性）は完全ではない（過去状態の完全な復元は不可）
- ECST による結果整合性ラグが発生する（リアルタイム在庫チェックは別途設計が必要）

**推奨フロー**

```
1. OrderService: 注文確定 + outbox に OrderPlaced
2. Orchestrator: ReserveInventory コマンド発行
3. InventoryService: InventoryReserved / InventoryRejected
4. Reserved → ArrangeShipping コマンド発行
5. Rejected → CancelOrder 補償（タイムアウト制御含む）
```

---

#### 案 B: トレーサビリティ重視型

```
① 制御フロー  : Orchestration Saga
② イベント設計 : ECST + Claim Check（大ペイロード時）
③ 状態管理   : Event Sourcing + CQRS
④ 信頼性保証  : Transactional Outbox（Debezium） + Inbox（Idempotent Consumer）
```

**選定理由**

| 観点 | 根拠 |
|---|---|
| R9 の最大化 | Event Sourcing により完全な監査証跡・イベント再生・任意時点の状態復元が可能（R8 は Outbox で両案とも同等に充足） |
| 読み取り最適化 | CQRS により注文状況一覧・在庫照会・配送進捗を用途別に最適化 |
| 将来拡張性 | 分析・ML・不正検知への活用基盤となる |

**トレードオフ**

- Event Sourcing の全面適用はスキーマ進化・スナップショット戦略等の高い専門知識が必要
- CQRS による DB 2 系統の運用コスト増
- 初期構築コストが案 A の約 1.5〜2 倍と推測される（⚠️ 推測・解釈）
- 小規模チームには重い可能性がある

---

#### 判定サマリーマトリクス

| 要件 | 案 A | 案 B | 備考 |
|---|:---:|:---:|---|
| R1 疎結合 | ◎ | ◎ | 両案とも ECST + Kafka 非同期を基本 |
| R2 サービス独立性 | ○ | ○ | Orchestrator への論理的依存があるが許容範囲 |
| R3 データ独立性 | ◎ | ◎ | 各サービスが専用 DB を保持 |
| R4 順序保証 | ◎ | ◎ | パーティションキー + Outbox で充足 |
| R5 冪等性 | ◎ | ◎ | Inbox パターンで充足 |
| R6 補償処理 | ◎ | ◎ | Orchestrator で明示的に管理 |
| R7 非線形フロー | ◎ | ◎ | Orchestrator（Temporal 等）で標準サポート |
| R8 耐障害性 | ◎ | ◎ | Outbox により保証 |
| R9 再処理可能性 | ○ | ◎ | 案 A は Kafka オフセット再処理は可能だが過去状態の完全復元は不可。案 B は Event Sourcing で完全対応 |
| **総合** | **◎** | **○** | EC サイト初期構築では案 A を推奨。監査・トレーサビリティ要件が強い場合は案 B |

> **推奨**: 初期構築では **案 A** を採用し、運用実績を踏まえて必要箇所（OrderService 等）に段階的に Event Sourcing を導入する **段階的移行戦略** が最もリスクが低い。

---

### 4.4 補足検討

#### 4.4.1 Choreography で R7 を実現する場合の課題

Choreography で非線形フローを実現する場合、以下の補助パターンが必要になる。

| 補助パターン | 用途 | 実装例 |
|---|---|---|
| Correlation ID / Saga ID | フロー追跡 | 全イベントに `sagaId` を付与 |
| Aggregator / Joiner | マージ | `PaymentApproved` と `InventoryReserved` の合流判定 |
| Timeout Manager | タイムアウト | Scheduler トピックによる期限監視 |
| Compensating Event Chain | 補償 | 逆方向のイベント連鎖 |

**課題**: これらの補助パターンを導入すると、実質的に Process Manager（Orchestrator）化する。Aggregator + Timeout Manager + Compensating Chain ＝ Orchestrator と等価であり、Choreography の最大の利点（疎結合・シンプルさ）を失う結果になりやすい。

---

#### 4.4.2 Orchestrator 実装方式比較

| 方式 | 特徴 | 学習コスト | 運用コスト | Kafka 連携 |
|---|---|:---:|:---:|---|
| **Temporal** | コードファースト、Durable Execution、retry / timer / signal が標準（✅ 事実: [Temporal Docs](https://docs.temporal.io/concepts/workflow-history)） | 中〜高 | 中 | Consumer がイベント受信→`workflowId=orderId` で Signal |
| **Camunda 8** | BPMN 可視化、業務部門への説明に強い（✅ 事実: [Camunda Kafka Connector](https://docs.camunda.io/docs/components/connectors/out-of-the-box-connectors/kafka/)） | 中 | 中〜高 | Kafka Connector / Job Worker で publish/consume |
| **自前実装** | saga_state テーブル + scheduler + outbox/inbox | 低〜中 | 高 | 全て自作 |

**推奨判断基準**

- 技術チーム主導・長期運用 → **Temporal**
- 業務可視化・BPMN・人手介在 → **Camunda**
- 単純な数ステップ Saga のみ → 自前実装も可（ただし R7 が強い本検討では非推奨）

> **⚠️ 推測・解釈**: Temporal と Kafka の統合における具体的なパフォーマンス数値は公式ドキュメントに記載がなく、実装規模に依存すると考えられます。

---

#### 4.4.3 Transactional Outbox + Debezium CDC 構成

**基本構成**

```
サービス DB
  ├── 業務テーブル（orders, inventory, shipments）
  └── outbox テーブル（event_id, aggregate_type, aggregate_id, payload, created_at）
       ↓ CDC（Change Data Capture）
Debezium Connector（Kafka Connect）
  └── Outbox Event Router SMT
       ↓
Kafka トピック（order-events, inventory-events, shipping-events）
```

**運用上の注意点**

| 項目 | 内容 |
|---|---|
| パーティションキー | `aggregateId`（orderId）を Kafka key に設定 → R4 順序保証のため |
| Consumer 側 Inbox | CDC でも end-to-end exactly-once にはならないため必須 |
| Connector lag 監視 | CDC の遅延をモニタリング |
| WAL / binlog 監視 | PostgreSQL: `wal_level=logical`、replication slot の監視 |
| スキーマ管理 | Avro / Protobuf + Schema Registry でスキーマ進化を管理 |
| Outbox パージ | 処理済みレコードの定期削除戦略が必要 |
| Poison Event 対策 | デシリアライズ不可能なイベントの DLQ（Dead Letter Queue）運用 |

> **✅ 事実**: Debezium の Outbox Event Router は [公式ドキュメント](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) に詳細な設定方法が記載されています。

---

#### 4.4.4 学習コスト・運用コスト比較

| レイヤー | パターン | 学習コスト | 運用コスト | 備考 |
|---|---|:---:|:---:|---|
| ① 制御 | Choreography | 中 | 高 | 運用時の障害解析コストが高い |
| ① 制御 | Orchestration Saga | 中〜高 | 中 | Temporal / Camunda の習熟が必要 |
| ② イベント | Event Notification | 低 | 中 | |
| ② イベント | ECST | 中 | 中 | Schema Registry の管理が追加 |
| ② イベント | Claim Check | 中 | 高 | 外部ストレージ管理が追加 |
| ③ 状態 | State-in-DB | 低 | 低 | 既存技術の延長 |
| ③ 状態 | CQRS | 中 | 中 | DB 2 系統の運用 |
| ③ 状態 | Event Sourcing | 高 | 高 | スナップショット・スキーマ進化管理 |
| ④ 信頼性 | Outbox + Inbox | 中 | 中 | Debezium 運用が主コスト |
| ④ 信頼性 | Dual-Write | 低 | **障害コスト極大** | 非推奨 |
| ④ 信頼性 | 非冪等 | 低 | **障害コスト極大** | 非推奨 |

---

### 4.5 実装ロードマップ（案 A 採用時）

リスクの高い要件から段階的に固めていく方針とする。

```
Phase 1: 信頼性基盤の確立（R5 / R8 優先）
├── Transactional Outbox テーブル設計 + Debezium CDC 基盤構築
├── Inbox（Idempotent Consumer）実装
├── ECST スキーマ設計（Avro / Protobuf + Schema Registry）
└── Kafka トピック設計（パーティションキー = orderId）

Phase 2: 基本業務フローの構築（R6 / R7 対応）
├── Orchestration Saga（Temporal）導入
│   ├── 注文受付→在庫引当フロー
│   ├── 在庫引当→配送手配フロー
│   └── 在庫不足時の補償フロー（注文キャンセル）
├── 各サービスの State-in-DB 実装（PostgreSQL）
└── CQRS 読み取りモデル構築（注文一覧・在庫照会・配送進捗）

Phase 3: 高度化・拡張（R7 強化 / R9 強化）
├── 非線形フロー拡充（分岐引当・部分発送マージ・リトライループ・タイムアウト自動キャンセル）
├── Claim Check パターン導入（大ペイロード対応）
├── 必要箇所への Event Sourcing 部分適用（OrderService 等）
└── 可観測性強化（分散トレーシング・フロー監視ダッシュボード）
```

> **段階的移行のポイント**: Phase 1 で R5/R8 を固めることで、Phase 2 以降の開発中に発生する障害の影響を最小化する。Phase 3 の Event Sourcing 導入は運用実績を踏まえた判断とし、不要であれば State-in-DB のまま運用を継続する。

---

### 4.6 参照情報

| 分類 | 参照先 |
|---|---|
| Transactional Outbox パターン | https://microservices.io/patterns/data/transactional-outbox.html |
| ECST パターン | https://microservices.io/patterns/data/event-carried-state-transfer.html |
| Debezium Outbox Event Router | https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html |
| CQRS パターン | https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs |
| Event Sourcing パターン | https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing |
| Kafka Delivery Semantics | https://docs.confluent.io/kafka/design/delivery-semantics.html |
| Temporal Saga | https://docs.temporal.io/concepts/saga |
| Temporal Workflow History | https://docs.temporal.io/concepts/workflow-history |
| Camunda Kafka Connector | https://docs.camunda.io/docs/components/connectors/out-of-the-box-connectors/kafka/ |

---

## 5. 考察 — アーキテクチャ選択の再検討

> **本章の位置付け**: セクション 1〜4 では「マイクロサービス + EDA + Kafka を採用する場合の設計指針」を検討した。本章では一歩引いて、**そもそもマイクロサービス + EDA が本システムの最適解であるかを問い直す**。4 章の検討を否定するものではなく、**採用判断の前提条件を明確化する** ことを目的とする。
>
> 4 章は「採るならどう組むか」、5 章は「そもそも採るべき条件は何か」を扱う。

### 5.1 要件の再整理 — 業務要件と設計制約の分離

セクション 3.4 の共通非機能要件（R1〜R9）には、**業務から本質的に導かれる要件** と **マイクロサービス + EDA を前提とした場合の設計制約** が混在している。アーキテクチャ選択を公平に比較するためには、この 2 つを分離して考える必要がある。

| 分類 | 要件 | 説明 |
|---|---|---|
| **業務要件**（アーキテクチャ非依存） | R4 順序保証 | 同一注文の処理順序が保証されること |
| | R5 冪等性 | 重複処理が副作用を起こさないこと |
| | R6 補償処理 | 在庫不足時に注文をキャンセルできること |
| | R7 非線形フロー | 分岐・マージ・ループ・タイムアウトに対応できること |
| | R8 耐障害性 | 一時的な障害でデータをロストしないこと |
| | R9 再処理可能性 | 障害回復後に未処理を再処理できること |
| **設計制約**（マイクロサービス前提） | R1 疎結合 | サービス間の同期 API 呼び出しを行わない |
| | R2 サービス独立性 | 独立デプロイ・スケールアウト |
| | R3 データ独立性 | 各サービスが専用データストアを持つ |

R1〜R3 はマイクロサービスアーキテクチャの定義そのものであり、**モノリスやモジュラーモノリスを採用する場合はそもそも前提が異なる**。業務要件（R4〜R9）はいずれのアーキテクチャでも充足が必要だが、その実現難易度はアーキテクチャ選択によって大きく異なる。

さらに、セクション 3.4 では明示されていないが、EC 業務には以下の **暗黙の業務要件** がある。

| 要件 | 内容 |
|---|---|
| **強整合性** | 注文確定と在庫引当は同一トランザクションで完結すべき（オーバーセルの防止） |
| **データ密結合** | 注文には在庫スナップショットが必要、配送には注文明細・住所・在庫拠点が必要、キャンセルは全業務を巻き戻す |

この強整合性要件は、4 章の推奨構成（`OrderService` と `InventoryService` のサービス分離）と緊張関係にある。マイクロサービスでこの要件を満たすには Saga パターン + 補償処理 + 冪等性保証が必要であり、これが分散設計の複雑性の主因となる。

---

### 5.2 スケーラビリティの検証

本検討のシステム規模（セクション 3）を改めて定量的に検証する。

#### 処理量の位置づけ

```
100万件/日 ÷ 86,400秒 ≈ 11.6 TPS（平均）
ピーク時（平均の100倍想定） ≈ 1,200 TPS
```

#### データベースベンチマークとの比較

| 環境 | RDBMS | TPS | 出典 |
|---|---|---:|---|
| 4 vCPU / 15 GiB | PostgreSQL 18 (pgbench TPC-B-like) | 3,011 | ✅ [pgbench.github.io](https://pgbench.github.io/pg18/) |
| Xeon Gold 8コア NVMe | PostgreSQL 16 (pgbench 非チューニング) | 2,085 | ✅ [credativ.de](https://www.credativ.de/en/blog/postgresql-en/quick-benchmark-postgresql-2024q1-release-performance-improvements/) |
| 高端コモディティ 8-16コア チューニング済 | PostgreSQL 16 | 8,000〜15,000 | ✅ [PIGSTY OLTP Template](https://pigsty.io/docs/pgsql/template/oltp/) |
| 32 GB / 512 threads | MySQL 8.4 (sysbench RW) | 13,325 | ✅ [Percona Benchmark 2026](https://www.percona.com/blog/2026-mysql-ecosystem-performance-benchmark-report/) |

> **⚠️ 推測・解釈**: ピーク 1,200 TPS は、非チューニングの PostgreSQL（4 vCPU）でも処理可能なレンジです。ただし、ベンチマークは単純な TPC-B ワークロードであり、実アプリケーションでは在庫行ロック競合・外部 API 呼び出し・業務ロジックの影響があるため、そのまま適用はできません。それでも **1〜2 桁の余裕がある** ことは、スケールを理由にマイクロサービス化する根拠が薄いことを示唆しています。

#### モノリスで大規模運用している実例

| サービス | アーキテクチャ | 規模 | 出典 |
|---|---|---|---|
| **Shopify** | Ruby on Rails モジュラーモノリス | 2024 BFCM: 80M app req/min | ✅ [Shopify Engineering](https://shopify.engineering/bfcm-readiness-2025) |
| **Stack Overflow** | ASP.NET モノリス + SQL Server | 209M HTTP req/day, 505M SQL queries/day (2016) | ✅ [Nick Craver Blog](https://nickcraver.com/blog/2016/02/17/stack-overflow-the-architecture-2016-edition/) |
| **GitHub** | Rails モノリス | 200万+ LOC, 1000人超が日次変更, 最大20 deploy/day (2023) | ✅ [GitHub Blog](https://github.blog/engineering/architecture-optimization/building-github-with-ruby-and-rails/) |
| **Basecamp / HEY** | Rails モノリス | 数十万ユーザー | ✅ [DHH — The Majestic Monolith](https://world.hey.com/dhh/the-majestic-monolith-4c87bbab) |

> **⚠️ 推測・解釈**: 上記はワークロード特性が異なるため直接比較はできませんが、**「モノリスはスケールしない」という前提は事実に反する** ことを示す十分な材料です。100 万件/日は Shopify の BFCM トラフィックと比較して **3 桁以上小さい** 規模です。

---

### 5.3 アーキテクチャ比較

> **⚠️ 以下の比較表は、公式文献・実例を根拠にした設計評価です。特定の文献がこの比較表そのものを提示しているわけではありません。**

| 比較観点 | モノリス | モジュラーモノリス | マイクロサービス + EDA |
|---|---|---|---|
| **トランザクション整合性** | ◎ DB の COMMIT 一発 | ◎ 同一 DB 内 ACID | △ Saga + 補償 + 冪等性 |
| **開発速度（初期）** | ◎ 最速 | ○ モジュール設計コスト | △ 境界・契約・基盤整備 |
| **デプロイ複雑性** | ◎ 1 アーティファクト | ○ 1 アーティファクト | △ 複数サービス × CI/CD |
| **デバッグ・障害解析** | ◎ スタックトレース 1 本 | ○ モジュール境界は明確 | △ 分散トレーシング必須 |
| **スケーラビリティ** | ○ 垂直 + リードレプリカ | ○ 同左 + モジュール単位最適化 | ◎ サービス単位の独立スケール |
| **インフラコスト** | ◎ 低い | ◎ 低い | △ Kafka, K8s, 監視基盤 |
| **チーム独立性** | △ デプロイ競合リスク | ○ モジュールオーナーシップ | ◎ サービス単位の自治 |
| **技術スタック柔軟性** | △ 統一スタック | △ 同左 | ◎ サービス毎に選択可能 |
| **将来の分割容易性** | × 境界なし | ◎ モジュール境界 = 分割候補 | — 分割済み |
| **データ整合性設計の難度** | ◎ ACID で自然 | ◎ ACID で自然 | △ 分散整合性の設計が必要 |

**根拠**

- ✅ 事実: Martin Fowler は "Monolith First" 原則として「モノリスが複雑すぎて管理できなくなったときにのみ分割すべき」と提唱している（[MonolithFirst](https://martinfowler.com/bliki/MonolithFirst.html)）
- ✅ 事実: マイクロサービスの前提条件として「自動化デプロイメント・高度な監視・迅速なプロビジョニング・DevOps 成熟度」が必要と明示されている（[MicroservicePrerequisites](https://martinfowler.com/bliki/MicroservicePrerequisites.html)）
- ✅ 事実: Amazon Prime Video の監視サービスにおいて、マイクロサービス構成からモノリスに移行し **運用コストが約 90% 削減** された事例がある（[Amazon Science Blog 2023](https://www.amazon.science/blog/scaling-up-the-prime-video-audio-video-monitoring-service-and-reducing-costs-by-90-percent)）。ただし、これは映像処理パイプラインの事例であり EC 業務とはワークロードが異なる

---

### 5.4 モジュラーモノリスの適用可能性

#### モジュール分割案

```
[EC アプリ（単一プロセス・単一 DB）]
 ├─ order モジュール       （注文受付・ステータス管理）
 ├─ inventory モジュール   （在庫管理・引当・ロールバック）
 ├─ fulfillment モジュール （配送手配・配送ステータス管理）
 ├─ notification モジュール（メール・SMS 通知）※非同期
 └─ platform モジュール    （認証・監査ログ・ジョブ管理・Outbox）
```

#### トランザクション境界の設計

```
[order + inventory] ← 同一 DB トランザクション（ACID）
  ┌────────────────────────────────┐
  │  1. 在庫残数チェック（SELECT FOR UPDATE）
  │  2. 在庫減算（UPDATE inventory）
  │  3. 注文レコード作成（INSERT orders）
  │  4. Outbox レコード作成（INSERT outbox）
  └────────────────────────────────┘
         │  COMMIT 後にイベント配信
         ▼
  [fulfillment] ← 非同期（Outbox 経由 or インプロセスキュー）
```

> **⚠️ 推測・解釈**: 注文と在庫引当を同一トランザクション境界に置くことで、Saga パターン・補償処理・冪等性保証の大部分が不要になります。配送手配は外部 API 依存があり非同期処理が自然なため、ここが将来の最初の分離候補となると考えられます。

#### DB スキーマの論理分離

| 方針 | 内容 |
|---|---|
| 物理構成 | 単一 PostgreSQL クラスタ（プライマリ + リードレプリカ） |
| 論理分離 | モジュール単位のスキーマ分離（`order.*`, `inventory.*`, `fulfillment.*`） |
| FK 制約 | モジュール内のみ FK を使用。モジュール間は ID 参照 |
| Outbox | 共通または モジュール別に `outbox` テーブルを設置 |

> **✅ 事実**: Shopify は "Majestic Monolith" として、モノリス内部をコンポーネント境界で分割する設計を採用・維持しています。Shopify Packwerk によりパッケージ依存関係を明示・強制しています（[Shopify Engineering](https://shopify.engineering/deconstructing-monolith-designing-software-maximizes-developer-productivity)、[Packwerk Retrospective](https://shopify.engineering/a-packwerk-retrospective)）。

#### 将来のマイクロサービス移行パス

```
Phase 1: モジュラーモノリス構築
  └─ モジュール間インターフェース強制、DB スキーマ所有権整理

Phase 2: 非同期連携の段階的導入
  └─ Kafka を外部連携・通知・分析投影から導入
  └─ fulfillment モジュールを非同期化

Phase 3: 必要に応じたサービス抽出
  └─ 負荷差・リリース頻度差が顕在化したモジュールのみ分離
  └─ order + inventory は最後までモノリス側に残す

Phase 4: 分離サービスの可観測性整備
  └─ 分散トレーシング・API ゲートウェイ導入
```

> **✅ 事実**: Martin Fowler は「うまくモジュール化したモノリスから段階的に剥がす」アプローチを推奨しています（[MonolithFirst](https://martinfowler.com/bliki/MonolithFirst.html)）。

---

### 5.5 マイクロサービス分割のトリガー

マイクロサービスへの分割は、**スケール起因ではなく、主に組織・運用起因で検討すべき** である。

#### 分割トリガーの分類

| トリガー種別 | 判断基準 | 100万件/日で発生するか |
|---|---|---|
| **組織起因**（主因） | 開発チームが 10〜15 名を超え、デプロイ競合が日常化 | 起きうる |
| | 業務ごとにリリース頻度が大きく異なる | 起きうる |
| | オンコール責任をサービス単位で閉じたい | 起きうる |
| **技術起因**（副因） | 特定モジュールの負荷が他を圧倒し、独立スケールが必要 | まず起きない |
| | 単一 DB がボトルネック（ピーク時 P95/P99 が SLA を継続逸脱） | まず起きない |
| | 外部 API 障害が全体に波及する | 起きうる |
| **ビジネス起因** | 国・地域ごとのデータ所在地要件（GDPR 等） | 要件次第 |
| | PCI / 個人情報 / 会計データの分離監査 | 要件次第 |

#### 分割すべきでない領域

**強整合性が必要な業務境界**（本検討では注文受付 + 在庫引当）はサービス分割の対象外とすべきです。分割すると Saga + 補償 + 冪等性の複雑な分散設計が必要となり、バグ混入リスクが著しく上昇します。

どうしもサービス分割が必要な場合は、**強整合性が必要な範囲を 1 サービスに残す** 設計が定石です。

```
[Order + Inventory Service]         [Fulfillment Service]
 ├─ 注文受付          ──イベント──▶ ├─ 配送手配
 ├─ 在庫引当                        ├─ 配送業者連携
 └─（単一トランザクション）         └─ 追跡番号管理
```

この構成では Saga は「Order+Inventory → Fulfillment」の 1 ホップに抑えられ、補償も最小限となります。

---

### 5.6 本検討への示唆

#### 4 章の推奨構成との関係

4 章で検討したマイクロサービス + EDA 構成は、**以下の条件が成立する場合に正当化される**。

| 条件 | 説明 |
|---|---|
| 組織境界 | 注文・在庫・配送が異なるチーム（異なる部門）で開発・運用される |
| リリース独立性 | 各サービスが独立したリリースサイクルを持つ必要がある |
| 外部連携 | 配送業者 API 等の外部依存を障害分離したい |
| 監査・トレーサビリティ | 完全な監査証跡・イベント再生が規制上必要 |
| 既存基盤 | Kafka クラスター・K8s・分散トレーシング等のインフラが既に存在する |

これらの条件が **複数** 当てはまる場合は、4 章の設計指針（Orchestration Saga + ECST + Outbox/Inbox）が有効です。4 章の検討は、将来のサービス分割時における設計原則としても価値を持ちます。

#### モジュラーモノリスから始めるべき条件

| 条件 | 説明 |
|---|---|
| チーム規模 | 10 名以下の単一チーム |
| スケール要件 | ピーク 1,200 TPS 程度（単一 RDBMS で対応可能） |
| 整合性要件 | 注文 + 在庫引当の強整合性が最優先 |
| インフラ | Kafka / K8s 等の分散基盤が未整備 |
| 開発速度 | 市場投入速度を優先したい |

#### Kafka / EDA の適切な導入範囲

マイクロサービス全面採用を前提としなくても、Kafka / EDA は以下の領域で **モジュラーモノリスと併用** できます。

| 導入領域 | 説明 |
|---|---|
| 外部連携 | 配送業者 API・決済 API との非同期連携 |
| 通知 | メール・SMS・Webhook の非同期配信 |
| 分析・レポーティング | 注文データの分析基盤への投影 |
| 監査ログ | イベントログの永続化・監査証跡 |
| 将来の分離候補間連携 | fulfillment モジュール分離時の連携基盤 |

つまり、**Kafka は「基幹トランザクションの本流に最初から入れる」よりも、「外部連携・通知・分析・非同期後処理から段階的に導入する」** 方が合理的です。

---

### 5.7 意思決定フレームワーク

本検討の結論として、以下の意思決定ルールを提示する。

| 判断条件 | 推奨アーキテクチャ |
|---|---|
| ACID 境界が複数業務に跨る + チーム 10 名以下 + ピーク数千 TPS 以下 | **モジュラーモノリス**（5.4 参照） |
| 上記 + チームが複数に分かれリリース頻度差が大きい | **モジュラーモノリス + 一部サービス分離**（5.5 参照） |
| 上記 + 複数チームが完全独立運用 + 分散基盤が整備済み | **マイクロサービス + EDA**（4 章参照） |
| 非同期連携先（外部 API・通知・分析）が増える | **Kafka 適用拡大**（モノリス / マイクロサービスいずれでも） |

> **結論**: 本検討のシステム規模（100 万件/日、ピーク 1,200 TPS）においては、**スケールを理由にマイクロサービス化する技術的根拠は薄い**。初期形としては **モジュラーモノリスが第一候補** であり、組織的・ビジネス的なトリガーが顕在化した時点で段階的にサービス分割を検討するのが最もリスクの低いアプローチです。
>
> ただし、**組織境界（開発・運用が別部門）や監査要件が最初から存在する場合** は、マイクロサービス + EDA を初期から採用する合理性があります。その場合は 4 章の設計指針に従い、**強整合性が必要な注文 + 在庫引当は同一サービスに残す** ことを推奨します。

---

### 5.8 参照情報

| 分類 | 参照先 |
|---|---|
| Martin Fowler — Monolith First | https://martinfowler.com/bliki/MonolithFirst.html |
| Martin Fowler — Microservice Prerequisites | https://martinfowler.com/bliki/MicroservicePrerequisites.html |
| Martin Fowler — Saga Pattern | https://martinfowler.com/articles/saga.html |
| Shopify — Deconstructing the Monolith | https://shopify.engineering/deconstructing-monolith-designing-software-maximizes-developer-productivity |
| Shopify — BFCM 2024 Readiness | https://shopify.engineering/bfcm-readiness-2025 |
| Shopify — Packwerk Retrospective | https://shopify.engineering/a-packwerk-retrospective |
| Stack Overflow Architecture 2016 | https://nickcraver.com/blog/2016/02/17/stack-overflow-the-architecture-2016-edition/ |
| GitHub — Building with Ruby and Rails | https://github.blog/engineering/architecture-optimization/building-github-with-ruby-and-rails/ |
| DHH — The Majestic Monolith | https://world.hey.com/dhh/the-majestic-monolith-4c87bbab |
| Amazon Prime Video — コスト削減事例 | https://www.amazon.science/blog/scaling-up-the-prime-video-audio-video-monitoring-service-and-reducing-costs-by-90-percent |
| PostgreSQL pgbench 実測 | https://pgbench.github.io/pg18/ |
| PostgreSQL Benchmark 2024Q1 | https://www.credativ.de/en/blog/postgresql-en/quick-benchmark-postgresql-2024q1-release-performance-improvements/ |
| Percona MySQL Benchmark 2026 | https://www.percona.com/blog/2026-mysql-ecosystem-performance-benchmark-report/ |
| Kamil Grzybek — Modular Monolith with DDD | https://github.com/kgrzybek/modular-monolith-with-ddd |
| Microsoft Learn — Saga Pattern | https://learn.microsoft.com/en-us/azure/architecture/patterns/saga |
