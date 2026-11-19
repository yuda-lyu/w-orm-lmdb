# 狀態(現況)

本檔只記現況,不寫過程(過程見 `sgud/exp/experience.md`);各功能之規則與盤點指令見 `sgud/rule/rulebook.md`.

- 更新: 2026-11-20(oro557c,排程第 557 項:主鍵「給了而非字串即拒絕」與「讀取即拋錯之輸入依原失敗通道拒絕,改用 wsemi `chkReadable`」,見 R-PKGIVEN-01、R-READABLE-01);前次 2026-11-18(wol387,排程第 387 項:`unit-backup` 同程序一致性案改為事件同步之見證判準,見 R-BACKUP-01 規則 9)、2026-11-01(orm233:wsemi 1.9.36 語料新增 wire 案 A11、C14、U22,`unit-diaglog` 處置表補登為不適用)、2026-10-29(mig-lmdb,排程第 161 項:診斷輸出與遮罩改用 wsemi 共用模組).
- 版號: `package.json` 為 1.0.75(wsemi `^1.9.70`);本輪未改版號、未 commit、未發布(發布由使用者/主控統籌;`dist/` 未重產,發布時自動產製).
- 最近一次驗證(2026-11-20,oro557c): 全套 `npm test`(`quiet-run --lock`)3771 passing、210 pending、0 failing(修改前基準 3327 passing、210 pending;新增 `unit-pkgiven` 255 案、`unit-readable` 189 案,另 `unit-diagsite` 3 處站點計數與 `unit-wellformed` 1 案隨規則更新);新增兩檔對 HEAD 之 src 副本: 444 案中紅 168(含 `after` 鉤子 1);變異 27 項全數轉紅(R-PKGIVEN-01 6 項、R-READABLE-01 21 項);修前/修後逐格行為表見報告 `C:\opensrcOper\dnow\20261120043625\agent-out\oro557c-修正報告.md`.前次(2026-11-18,wol387,`src` 未改): 全套 `npm test`(`quiet-run --lock`,閒置)3327 passing、210 pending、0 failing(與修前基準同數);`unit-backup` 同程序案於 6 個忙迴圈(below-normal)之負載下,修前 7/40 紅(皆 `writer should keep progressing, got 10`),修後 0/70(單案 70 次、整檔 25 案 10 次皆綠);阻擋變異 M1(寫入等備份)、M2(同步複製)新版 6/6 紅.前次(2026-11-01,orm233,wsemi 1.9.36): 全套 `npm test`(`quiet-run --lock`)3327 passing、210 pending、0 failing(盤查時 `R-DIAG-01-8` 紅於處置表缺 A11、C14、U22,補登後綠).前次(2026-10-29,mig-lmdb): 全套 `npm test` 3327 passing、210 pending、0 failing(修改前基準 3320;quiet-run 日誌存證據夾 `full-npm-test.log`;同輪另一次全套與單檔重跑見 `unit-backup` 既有計時不穩定: 案例「同程序持有環境且寫入進行中: 連續備份…」斷言 `nBatch > 10`,修前 HEAD 之 src 2/12、修後 2/12 次失敗〔交錯重跑,證據 `flake-unit-backup/`〕,未改該測試、與本輪無關,未修〔待主控〕);診斷相關五檔 103 案(`unit-diaglog` 26、`unit-diagerr` 22、`unit-urlerr` 16、`unit-diagsite` 10、`unit-diaglog-run` 29);修前紅(HEAD 之 src 搭新測試): 102 案紅 44;變異 42 項殺死 42(`F10`、`L3` 為行為等價變異,僅被站點盤點掃描殺死).`tmp/` 已於結案時清理,證據備份於 `C:\opensrcOper\dnow\20261029002013\agent-out\mig-lmdb-evidence\`.

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
| 主鍵之「未帶」(`undefined`、`null`、`''`)與「給了而非字串」: 未帶依 `autoGenPk` 補值或整批 reject,給了而非字串一律 `invalid id: not a string at [data[k].id]`(不補值、不隨 `autoGenPk` 改變;insert/insertBulk 整批、save/del 逐筆、selectByPk 回 null) | 已套(2026-11-20,第 557 項) | R-PKGIVEN-01 | `unit-pkgiven`(工具 `test/tools/hostile.mjs`) |
| 讀取即拋錯之輸入(已撤銷之 Proxy、`Symbol.toStringTag` 拋錯、`ownKeys` 拋錯、`[0]` 存取器拋錯之陣列)依原失敗通道以固定詞彙 `invalid <名稱>: must be a readable value` 拒絕: 建構 opt 與成員、insert/insertBulk/save/del 之 data、option 與成員(returnList、compact)、selectByPk 之 pk;改用 wsemi `chkReadable`,探針只置於判斷函數回 false 之後 | 已套(2026-11-20,第 557 項) | R-READABLE-01 | `unit-readable`(含站點盤點三案;`unit-diagsite` 之計數同步) |
| **診斷輸出與對外錯誤值之遮罩: 用 wsemi 共用模組(`getDiag`、`getDiagSecrets`),預設不含個資、`opt.logDetail`、`opt.funLog`、診斷輸出遮儲存路徑與憑證、對外錯誤值只遮憑證** | **已套(本輪自自寫版本遷移)** | **R-DIAG-01(依 dspec 與 wsemi R-DIAG-01)** | `unit-diaglog`、`unit-diagerr`、`unit-urlerr`、`unit-diagsite`、`unit-diaglog-run`(工具 `test/tools/diagScenario.mjs`、`genDiagGolden.mjs`、`diagGolden.json`、`diagStream.mjs`) |

## 診斷輸出(R-DIAG-01)現況

- 無自寫之診斷或遮罩模組(`src/getDiag.mjs` 已刪);`src/WOrmLmdb.mjs` 建構段宣告 `getDiag(opt, { secrets: getDiagSecrets({ paths }), patterns: ['fsPathTail', 'receivedTail'], codes: ['lmdb'], safeKeys: ['mode'] })`.
- 語料 wire 案處置(`unit-diaglog` 之 `WIRE`,對 wsemi 1.9.36 語料 23 案全數登錄): 經真實站點重跑 detail 9(T01、T07、K01、K09、V02、E01、E07、P01、S07)、brief 1(C01)、reject 2(R01、T07);自身等價案 4(R07、R11、O01、O15);不適用 8(R02、R04、R12、A01、A07、A11、C14、U22,理由見該表註解).不適用之依據: `src/WOrmLmdb.mjs` 只引入 `getDiag` 與 `getDiagSecrets`(兩者皆不引入 `genDiagFunLog`)、宣告 `paths`/`codes:['lmdb']`/`safeKeys:['mode']`,無 `conns`、`types`.
- 診斷輸出站點 2 個(change、error 訂閱函數拋錯),`console.log`(stdout),預設 `label + { mode, errType[, errCode] }`;`logDetail: true` 為 `console.log(label, 遮罩拷貝)`;`funLog(label, safe, detail)` 取代 console.
- 對外錯誤值(reject、建構時 throw、`'error'` 事件訊息、`save`/`del` 逐筆 `err`)之外來值經 `diag.err`/`diag.mask`(全部 catch 站點見帳本站點全集表,共 15 站,由 `unit-diagsite` 以原始碼掃描鎖定);本套件自身以固定詞彙組成之錯誤(`mkErr` 登記)原樣交出.只遮憑證,不遮儲存路徑(dspec Q3).
- 回傳契約以修改前 HEAD(dfa8bb7)產生之固定預期值 `test/tools/diagGolden.json` 對照,不變.
- lmdb 驅動自身之輸出(JS 層 `console.error/warn`、原生層直寫 stderr,含 `compression.cpp:86-89` 於解壓失敗時傾印紀錄位元組)不攔,已於 README 與帳本寫明部署面之注意,依專案原則 15 不向上游通報.

## 已知缺口與待辦

- 診斷輸出: 見 R-DIAG-01〈已知限制〉(驅動日誌不攔、對外值不遮儲存路徑〔Q3〕、`url` 之 `scheme://帳密@` 形式經路徑正規化後不再是網址形式而不被遮、預設 `_db` 現於診斷輸出被遮、POSIX 未實機驗證、測試接縫依賴 `msgpackr` 之 `Unpackr.prototype.decode` 與 `addExtension`).
- 待裁示(第 557 項,見 R-PKGIVEN-01、R-READABLE-01 已知限制與報告〈需你決定〉): ① 資料複製階段(`cloneDeep`)才拋錯之巢狀不可讀值(`id` 屬性、陣列元素本身為不可讀)之訊息為原例外文字,通道正確,是否統一為固定詞彙(需複製前探針);② `opt`/`option` 成員為存取器而拋錯者維持原錯誤文字(既有案 `unit-save-merge` 釘住),w-orm-mongodb 一律轉固定詞彙;③ `selectByPk` 不可讀 pk 為 reject(同 w-orm-lowdb、w-orm-level),w-orm-mongodb 回 null;④ `save` 之 `autoInsert` 以真假值使用、不可讀值不拒絕(w-orm-mongodb 拒絕).
- 其餘功能之已知限制見各條目(R-READ-02: 非關閉之環境層錯誤於 save/del 降為逐筆 `ok:0`;R-BACKUP-01 已知限制 ①–⑥;R-ORMSTR-01 已知限制 ①–⑤).
- 已解(2026-11-18,第 387 項): `unit-backup` 同程序案原以寫入端總批數門檻 `nBatch > 10` 判定,成因為門檻比的是寫入提交速度與約 10ms 備份長短之相對快慢(正確實作閒置時僅得 11,裕度 1 批),非產品阻擋寫入;已改事件同步之見證判準(R-BACKUP-01 規則 9、已知限制 ⑦).
- 已解(2026-11-18,主控裁示,R-BACKUP-01 已知限制 ⑧): 無間歇之持續寫入下備份大型資料庫偶有停住逾百秒(lmdb-js 層,裸 `db.backup` 同樣重現);採註明使用限制——已於 README 備份說明與 `src/WOrmLmdb.mjs` `backup` 之 JSDoc 註明,**不加逾時/停滯選項**(見 `sgud/feedback/feedback.md`).另跨程序案與 `test/tools/batchWriter.mjs` 之「約 80MB」註解已更正為實測值(`compression: true` 下約 6MB,未改填充行為).
- 待裁示(主控,細節見報告〈待裁示〉): ① Q3 後 `url` 內之網址帳密形式(`http://user:pw@host/x`)於建構錯誤不再被遮(路徑正規化成 `http:\user:pw@host\x`)——是否接受(本套件 `url` 為資料夾,無帳密欄位),或另宣告 `values`/`urls`;② 下游影響: 依賴本套件(`^1.0.45`)之 w-web-task、w-web-sso、w-web-perm、w-web-api、w-knowledge-extract、w-data-pipeline,經 Grep(各 `server`/`src` 之 `.mjs`)未見直接傳入本套件之 `logDetail`/`funLog` 或讀 `errCode`(其診斷經 w-serv-orm 之 funLog);建構錯誤 `err.path` 由 `'***'` 改為完整路徑、對外錯誤值不再遮路徑,為可見行為變化;③ `package-lock.json` 為 `npm i` 副作用(`resolved` 欄移除、頂層 version 跟到 1.0.46),是否採用由主控決定;④ quiet-run 回報 `web-tests` 槽有殘留鎖(owner=w-serv-webdata,pid 已不存在),未處理.
