# 狀態(現況)

本檔只記現況,不寫過程(過程見 `sgud/exp/experience.md`);各功能之規則與盤點指令見 `sgud/rule/rulebook.md`.

- 更新: 2026-10-28(lmdb-run01,第 153 項診斷輸出).
- 版號: `package.json` 為 1.0.45;本輪未改版號、未 commit、未發布(發布由使用者/主控統籌).
- 最近一次驗證(2026-10-28,lmdb-run01): 全套 `npm test` 3320 passing、210 pending、0 failing(quiet-run 日誌 `tmp/quiet-run/20261028-101150-lmdb-run01.log`;修改前基準 3224 + 新增 96 案,既有案例零變動);新增 `unit-diaglog`(56)、`unit-diaglog-run`(28)、`unit-urlerr`(12);修前紅(HEAD 副本搭最終測試): 40 案紅 25;變異 102 項殺死 101(存活 `W10` 為等價變異).`tmp/` 已於結案時清理,證據備份於 `C:\opensrcOper\dnow\20261028054319\agent-out\lmdb-run01-evidence\`.

## 功能現況

| 功能 | 現況 | 規則帳本條目 | 測試檔 |
|---|---|---|---|
| 基本讀寫(select、selectByPk、insert、insertBulk、save、del、delAll)、`useCache`、`autoGenPk` | 已套 | R-WRITE-01、R-WRITE-02 | `unit-basic`、`unit-cache`、`unit-autogenpk`、`unit-insert`、`unit-insertbulk`、`unit-save`、`unit-del`、`unit-delall`、`unit-returnlist`、`unit-writeerror` |
| 讀取可見他方提交(跨程序、同程序多實例) | 已套 | R-READ-01 | `unit-crossprocess`、`unit-multiinstance`、`unit-concurrency`、`unit-readerror` |
| 讀取失敗不回報為「查無」 | 已套 | R-READ-02 | `unit-readerror` |
| 主鍵超過 lmdb 鍵長上限之讀取側未命中 | 已套 | R-READ-03 | `unit-writeerror` |
| `close()` 後(含進行中)之操作 reject | 已套 | R-CLOSE-01 | `unit-closed` |
| `delAll` 命中判定(與 `select` 一致) | 已套 | R-DELALL-01 | `unit-delall`、`unit-multiinstance`、`unit-writeerror` |
| `change`/`error` 事件不改變回傳 | 已套 | R-EVENT-01 | `unit-event`、`unit-closed` |
| 條件(`find`)白名單 | 已套 | R-FIND-01 | `unit-find-whitelist` |
| `save(data, { merge: false })` 整筆覆寫 | 已套 | R-SAVE-01 | `unit-save-merge` |
| 字串良構(主鍵、資料、`opt`) | 已套 | R-ORMSTR-01 | `unit-wellformed` |
| 一致性備份 `backup(fpTar, { compact })` | 已套 | R-BACKUP-01 | `unit-backup`(子程序工具 `test/tools/*`) |
| **診斷輸出: 預設不含個資、`opt.logDetail`、`opt.funLog`、儲存位置遮罩、建構失敗訊息遮罩** | **已套(本輪新增)** | **R-DIAG-01** | `unit-diaglog`、`unit-diaglog-run`、`unit-urlerr`(工具 `test/tools/diagScenario.mjs`、`genDiagGolden.mjs`、`diagGolden.json`) |

## 診斷輸出(R-DIAG-01)現況

- 單一出口 `src/getDiag.mjs`;`src/WOrmLmdb.mjs` 於建構段建立 `diag`,兩個站點(change、error 訂閱函數拋錯)經它輸出,`open()` 失敗之例外經 `maskOpenErr` 遮罩後 throw.
- 預設輸出 `change handler threw { mode, errType[, errCode] }`/`error handler threw {…}`;`logDetail: true` 還原舊版輸出;`funLog(label, safe, detail)` 取代 console.
- 回傳契約以修改前 HEAD(dfa8bb7)產生之固定預期值 `test/tools/diagGolden.json` 對照,不變.
- lmdb 驅動自身之 `console.error/warn`(第三方)不攔,已逐站歸類(見帳本站點全集).

## 已知缺口與待辦

- 診斷輸出: 見 R-DIAG-01〈已知限制〉(驅動日誌不攔、`SECRET_MIN=4` 取捨、`backup` 之 reject 訊息可能回顯呼叫端自給之 `fpTar`、POSIX 未實機驗證).
- 其餘功能之已知限制見各條目(R-READ-02: 非關閉之環境層錯誤於 save/del 降為逐筆 `ok:0`;R-BACKUP-01 已知限制 ①–⑥;R-ORMSTR-01 已知限制 ①–⑤).
- 待裁示(主控): ① lmdb 原生層於紀錄無法解壓時把該筆位元組寫到 stderr(`compression.cpp:86-89`),`logDetail`/`funLog` 管不到,本輪只文件化——是否向上游通報、或在部署面把此類程序之 stderr 視為敏感;② `errCode` 之整數範圍(絕對值 ≤99999,系列一致)是否收窄,需全系列一併決定.
- 本輪外審(2026-10-28): terra 因 Codex 帳號額度用盡未能派出;Argon(`gemini-4-argon-high`)第 1 輪 17 條皆屬實或部分屬實並已處置;第 2 輪 Argon 與 agy-opus 皆額度用盡,改由 agy-flash 複驗 7 條,採納 5 條.詳見報告〈外審評分表〉.
