# 規則帳本

本檔為套件設計與開發階段之規則帳本,為要求程式碼須長成什麼樣,或由規格產出之測試(項目/說明/流程)有哪些,包含軸、單一擁有者、已知限制、破壞性變更紀錄,以及對應是哪些測試檔案或指令等,由agent自行編列、擴充與維護.

- 系列規格: `w-orm系列套件統一規格.md`(2026-10-12 由 `資料庫函數回傳定義計算.md` 改名;他專案之引用已同步).以下「規格」即指此檔.
- 全套測試: `npm test`(即 `mocha --parallel "test/*.test.mjs" --timeout 180000`),2026-10-14 為 16 檔 206 案, 2026-10-17 增 `unit-find-whitelist.test.mjs` 為 17 檔(本行原記 458 案,與 R-FIND-01 所記 492 不一致;2026-10-18 基準實測 492 案 = 206 + 286), 2026-10-18 增 `unit-save-merge.test.mjs`(63 案)為 18 檔 555 案;2026-10-20 增 `unit-wellformed.test.mjs`(案例表逐案,2630 passing + 210 pending)與 `unit-backup.test.mjs`(25 案)為 20 檔,全套 3224 passing、210 pending、0 failing(本行 555 案之基準與實測有出入,以當次實測為準);2026-10-28 增 `unit-diaglog.test.mjs`(56)、`unit-diaglog-run.test.mjs`(28)、`unit-urlerr.test.mjs`(12)為 23 檔,全套 3320 passing、210 pending、0 failing(R-DIAG-01);2026-10-29(mig-lmdb)診斷輸出改用 wsemi 共用模組,`unit-diaglog`(26)、`unit-diaglog-run`(29)、`unit-urlerr`(16)改寫,新增 `unit-diagerr`(22)、`unit-diagsite`(10)為 24 檔,全套 3327 passing、210 pending、0 failing(以最後一次全套實測為準,見 `sgud/state/state.md`).單檔: `npx mocha --no-parallel test/<檔名> --timeout 180000`.
- 測試暫存落 `test/_tmp/<案名>/` 並於 after 刪除;備份測試之子程序工具 `test/tools/batchWriter.mjs`、`closeDuringBackup.mjs`、`killDuringBackup.mjs`、`countRows.mjs`(見 R-BACKUP-01);子程序工具 `test/tools/rawLmdbWriter.mjs`(裸 lmdb 寫入者,無讀取路徑,支援 put/remove 之提交標記檔、持續寫入、mark、reader slot 取樣)、`test/tools/closeAfterReject.mjs`(寫入遭整批性錯誤 reject 後立即 close,以離開碼與結束標記判定是否崩潰).
- **2026-10-20(第 66 項批 1a、issue #11)後 `src/WOrmLmdb.mjs` 之行號再大幅位移(建構、`procPk` 之後、`save`、`del`、`close` 各處新增,檔尾增 `backup`),以下舊規則之行號僅供參考,一律以函數名 Grep 定位**;本日新增規則(R-ORMSTR-01、R-BACKUP-01)之行號為 2026-10-20 現況.
- 行號以 2026-10-14 修正後之 `src/WOrmLmdb.mjs` 為準(2026-10-14 依 `git diff` 之 hunk 逐一對應並核對該行內容,腳本 `tmp/run03-linemap.mjs`).2026-10-18 `save` 增 `merge` 後,`save` 起(原 `:705`)之後的行號全數位移 +6~+17(外審修正 `merge` 取值移入 `try` 等又增行,分兩次對應),已依同腳本逐一對應(24 個舊行號核對內容皆 SAME)並以 `tmp/run04-remap-rulebook.mjs`、`tmp/run04-remap2-rulebook.mjs` 更新本檔(僅對應表內之舊行號,各 28 處);位移前之行號不變.

## R-READ-01 寫交易外之讀取須見呼叫前已由他方提交之資料

- 狀態: 已套(2026-10-12).
- 單一擁有者: `prepareRead`(`:210`)、`readAll`(`:109`)、`getValue`(`:318`).各操作不得於寫交易外自行呼叫 lmdb 讀取 API.
- 規則:
  1. 寫交易外之讀取,讀取前經 `prepareRead` 呼叫 `client.resetReadTxn()`,且檢查與讀取之間不可 await.
  2. 他方 = 本實例以外之寫入者: 其他程序,以及同一程序之其他實例(同路徑另一次 open).lmdb-js 之讀交易屬於每次 `open()` 所得之實例(`node_modules/lmdb/open.js:191`、`:235`、`:534`),寫入只重設寫入者自身之讀交易(`write.js:579-582`).
  3. 全表讀取一律經 `readAll`: 同步迭代(不可 for await),並於 finally 呼叫 iterator 之 `return()`(`:127`),歸還 cursor 之錯誤不得掩蓋原結果或原錯誤.for await 會使併發 select 交錯持有讀交易,重設時另開讀交易而耗盡 reader slot;`next()` 拋錯(數據無法解碼)時 for-of 不呼叫 `return()`,讀交易被孤立占用 reader slot 直到 GC.
  4. 寫交易內之讀取(insert 之 ifNoExists、insertBulk 之 childTransaction、save 之 transaction)不重設.
- 依據: `read.js:1056-1058`(延至 timers 階段重設)、`README.md:409-411`(lmdb-js 文件)、`env.cpp:233`(重設後必 renew)、`txn.cpp:56-64`(使用中且有新提交即另開)、`env.cpp:297` 與 `lmdb.h:968-975`(reader slot 預設 126、跨程序共用);ECMA-262 for-of 於 next() 拋錯時不呼叫 return().
- 盤點指令: Grep `src/` 搜 `client\.(get|getRange|getMany|getAsync|getEntry|getValues|getKeys|getCount|doesExist)\(`,逐一判定是否位於交易回調內或經 prepareRead.
- 站點全集與判準(2026-10-12 盤點):
  - `readAll` 之 `client.getRange()`: 已套(規則 1、3).
  - `getValue` 之 `client.get()`(selectByPk `:429`、save `:773`、del `:1015`): 已套.
  - insertBulk 交易內 `client.doesExist`(`:643`)、save 交易內 `client.get`(`:806`): 刻意不套(寫交易內).
- 軸: 操作 × 他方先行異動(新增/更新/刪除/改入改出條件)× 他方種類(他程序/同程序他實例)× 併發 × 數據可否解碼 × useCache.
- 已知限制: select 於 useCache 快取命中時回傳快取(只反映本實例之寫入);del、delAll 之先讀後寫非原子,多程序同時寫入時可能多報或誤刪(README 已排除同時寫入);讀前重設使 lmdb-js 每次讀取排一個計時器(實測不劣於修正前).
- 測試: `test/unit-crossprocess.test.mjs`(27 格×2,子程序提交後寫標記檔、父程序同步等待,修正前確定紅)、`test/unit-multiinstance.test.mjs`(同程序裸 lmdb putSync 確定性 5 格×2、兩實例 save×20、useCache 之 delAll)、`test/unit-concurrency.test.mjs`(併發 1000+1000,slot ≤2,突發期間他程序提交 ≥100,結果正確;定位為防「只加重設而未改同步迭代」之守門)、`test/unit-readerror.test.mjs` 之 readerror reader slot(壞數據＋他程序持續提交＋select×150,無 READERS_FULL、slot ≤2).
- 變更紀錄:
  - 2026-10-12: 讀前重設、同步迭代、finally 歸還 cursor、移除 waitOpen.修正前實測: 跨程序 14/25 格 77~96%、同程序兩實例 save 遺失 64~97%、壞數據反覆 select 約 126 次後全環境 MDB_READERS_FULL.

## R-READ-02 讀取失敗不得回報為「查無」

- 狀態: 已套(2026-10-12).
- 單一擁有者: `getValue`(`:318`,不攔錯).
- 規則:
  1. `getValue` 不攔下 `client.get` 與 `prepareRead` 之錯誤;selectByPk 由其 catch 轉 reject(規格第 261 行),del 由逐筆 catch 轉 `{n:1,nDeleted:0,ok:0,err}`(規格第 388 行).
  2. save 之快速路徑例外: 讀取失敗而實例未關閉時放棄快速路徑、續走寫交易(`:778-781`),結果由交易內之讀取決定;快速路徑為選擇性優化,不得使可由寫交易完成之儲存失敗.
  3. lmdb-js 查無時回 undefined 不拋錯(`read.js:94-96`),故會拋出者皆為真錯誤.
- 盤點指令: Grep `src/` 搜 `getValue\(|catch \(err\)`.
- 站點全集與判準: selectByPk(`:429`)、del(`:1015`): 已套;save 快速路徑(`:773`): 刻意例外(規則 2).
- 已知限制: 非關閉之環境層錯誤(MDB_READERS_FULL、環境失效、提交失敗)於 save、del 仍降為逐筆 ok:0,未依 T4 升為整批(待討論,已知觸發源已根除);無法解碼之數據無法以 del 刪除,且使整表 select、delAll 失敗.
- 測試: `test/unit-readerror.test.mjs` 1~10.
- 變更紀錄: 2026-10-12 移除吞錯;修正前無法解碼之數據或 MDB_READERS_FULL 使 selectByPk 回 null、del 回 `{n:0,nDeleted:0,ok:1}`.

## R-READ-03 主鍵超過 lmdb 鍵長上限者,讀取側一律視為未命中

- 狀態: 已套(2026-10-14,主控裁示 D5).
- 單一擁有者: `keyFits`(`:284`,與 R-WRITE-02 共用);站點 `getValue`(`:318`,判定於 `:323`;服務 selectByPk、save 快速路徑、del)與 save 交易內之讀取(`:806`).
- 規則:
  1. 鍵經 lmdb 之鍵編碼後超過 `maxKeySize`(1978)者必不存在,讀取側一律視為未命中: selectByPk 回 null;del 該筆回 `{n:0,nDeleted:0,ok:1}`;save 視為不存在——autoInsert 為 false 回 `{n:0,nInserted:0,nModified:0,ok:1}`,為 true 則其寫入失敗而該筆 ok:0,err 為 lmdb 之 `Key size is larger than the maximum key size (1978)`.
  2. 不呼叫 lmdb 之讀取 API: 其以 4096 位元組之固定緩衝寫鍵(`node_modules/lmdb/open.js:583-594`),4092 位元組以內回 undefined、4093 起拋 `String does not fit in target buffer`(2026-10-14 實測),直接呼叫則同為不存在之鍵依長度而回未命中或錯誤.
  3. 判定位於 `prepareRead` 之後: 已關閉之判斷須先於鍵長判斷,否則已關閉之實例對超長鍵回未命中而與查無同形(R-CLOSE-01).
  4. 不屬規格之「主鍵值無效(未給、型別不符)」: 鍵型別正確,僅超過本後端之上限;規格共通條文不改(他後端上限不同),只載於規格之 w-orm-lmdb 段.
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `client\.(get|doesExist|getRange)\(|getValue\(|keyFits\(`,逐一判定是否經 keyFits.
- 站點全集與判準: `getValue` 已套;save 交易內之 `client.get` 已套;insertBulk 交易內之 `client.doesExist` 刻意不套(其前 procWritable 已整批擋下超長鍵);`readAll` 之 `getRange` 不適用(無鍵).
- 已知限制(主控裁示 D9): 值非物件之紀錄(他方寫入,或修正前經陣列等元素寫入)於 insert、insertBulk 以鍵層判定為存在,於 save、del、selectByPk 以 `iseobj(值)` 判定為不存在(規格要求命中認定一致);本次不統一.
- 測試: `test/unit-writeerror.test.mjs` 之鍵超長讀取案(1979、4092、4093、70000 位元組 × selectByPk、del、save 兩種 autoInsert).
- 變更紀錄: 2026-10-14;修正前 1979–4092 位元組 del 回未命中、selectByPk 回 null,4093 位元組起 del 回 `{n:1,nDeleted:0,ok:0,err}`、selectByPk reject,save 之 err 依長度而異.

## R-CLOSE-01 close() 之後(含進行中)之操作一律 reject,不得與查無同形

- 狀態: 已套(2026-10-12).
- 單一擁有者: `isClosed`(`:186`,`client.status !== 'open'`)、`getClosedMsg`(`:191`)、`procClosed`(`:198`)、`prepareRead`(`:210`).
- 規則:
  1. 七函數入口第一步 `if (isClosed()) return procClosed(...)`,先於輸入檢查.lmdb status 僅 open/closing/closed 三值(`open.js:317`、`read.js:962`、`:999`),`'open'` 於 open() 回傳前已設,每次 open() 各有其 status;曾寫入之實例 close() 未完成前恆為 closing(`write.js:1073`,lastWritePromise 不清除).不使用 `isOperational()`(不在 `index.d.ts`).**入口順序(2026-10-17 補,見 R-FIND-01): closed → find 檢核 → 讀取**——`select`、`delAll` 以壞條件呼叫已關閉之實例,reject 訊息須為 closed 而非 `invalid find`(測試 `test/unit-find-whitelist.test.mjs` order).
  2. 讀取處同步再擋(prepareRead): 關閉中呼叫 lmdb 讀取會使 lmdb-js 於計時器或寫入提交之回呼內拋未捕捉例外(後者使 close 與寫入永不完成).
  3. 入口後才 close: 尚須存取資料庫之步驟為整批性錯誤 → save、del 之逐筆 catch(`:880`、`:1048`)往外拋統一訊息;delAll(`:1194`)、insertBulk 之 catch 改為統一訊息(insertBulk 之主鍵衝突保留衝突訊息);已於關閉前完成全部資料庫存取者(select、selectByPk、del 僅無效主鍵、delAll 無命中)照常回傳;insert 之寫入於呼叫內同步送出,不可達故不加.
  4. 重複 close 無妨(lmdb 於關閉中與已關閉時皆正常回傳).
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `isClosed\(\)|client\.status`.
- 站點全集與判準: 七入口(`:342` select、`:414` selectByPk、`:470` insert、`:598` insertBulk、`:714` save、`:963` del、`:1134` delAll)已套;prepareRead 已套;逐筆與整批 catch 如規則 3.
- 測試: `test/unit-closed.test.mjs`(closed 10 案;closing 20 案: 七函數、事件與訊息、close 前送出之寫入、入口後才 close 七函數與不存取資料庫之分支、重複 close、未捕捉例外為零、單筆小寫入未完成 ×3、曾寫入無未完成寫入、無效輸入、error 先於 reject、insertBulk 未寫入、del 整批 reject 時只發整批 error).
- 變更紀錄: 2026-10-12 涵蓋 closing、移除 waitOpen;修正前 closing 時 select/selectByPk/delAll 空轉 10.5 秒、save 降逐筆 ok:0、del 回未命中、無效輸入回正常空結果、計時器內未捕捉例外、小寫入時 close 永不完成.

## R-DELALL-01 delAll 之刪除對象只依資料庫當下之數據與正確之命中判定

- 狀態: 已套(2026-10-12).
- 規則:
  1. delAll 一律經 `readAll('delAll', true)`(`:1158`)直讀,不經 useCache 之快取(比照規格 w-orm-level 段第 761 行).
  2. 刪除對象以 `readAll('delAll', true)` 取得各筆 `{key, value}`;有條件者逐筆以 `new mingo.Query(fc).test(value)` 判定命中(`fc` 為 R-FIND-01 入口檢核所回傳之複本, 2026-10-17 起; 此前直接用呼叫端之 `find`),並**依資料庫之實際鍵刪除**(2026-10-14 起,主控裁示 D4).不得以一般物件加 `in`(含 wsemi `haskey`)判定: 主鍵為 constructor、toString、__proto__ 等會因原型鏈被誤判;亦不得以值之 `id` 欄刪除: 值不帶有效 id 者(他方寫入)使 `client.del(undefined)` 拋錯,整表無從以 delAll 清空、其前已刪者不回滾,且該錯誤若為該事件迴圈第一筆寫入,其後立即之 close 使行程崩潰(R-WRITE-02).
- 測試: `test/unit-delall.test.mjs` 7、8(原型同名主鍵);`test/unit-multiinstance.test.mjs` 7、8(useCache);`test/unit-writeerror.test.mjs` 之無 id 紀錄(無條件、帶條件)與子程序情境 delAllIdless.
- 變更紀錄: 2026-10-12;修正前快取誤刪/漏刪、原型同名主鍵被誤刪且回傳計數遮蔽之.2026-10-14 改依實際鍵刪除;修正前值不帶 id 之紀錄使 delAll reject(其前已刪者不回滾),reject 後立即 close 崩潰(3/3).

## R-EVENT-01 事件不得改變回傳結果,整批 reject 者不發逐筆事件

- 狀態: 已套(2026-10-12).
- 單一擁有者: `emitChange`(`:220`)、`emitError`.
- 規則:
  1. emitChange 於有 change 訂閱者時傳入 `cloneDeep(res)`(T10.1 第 4 條);無訂閱者不 emit.
  2. save、del 之逐筆事件(save 之 insert change、逐筆 error)緩存於 `evs`(`:906`、`:915`、`:1065`),整批定案後於整批 change 之前依序發出(`:935`、`:1085`),整批 reject 者不發出(T10.1 第 1 條、T10.2).既有順序(逐筆先於整批)不變.
  3. 寫交易失敗之筆不發 insert 事件: 逐筆 catch 內 `inserted = false`(`:885`).
  4. 每處 emit 皆 try/catch(既有).
- 測試: `test/unit-event.test.mjs` 15~18(訂閱者改寫 res);`test/unit-closed.test.mjs` closing 20(整批 reject 不發逐筆 error).規則 3 無永久測試(讀碼保證;`client` 為閉包變數,注入 commit 失敗須模組替換;sol 之故障注入證據原見 `tmp/review/sol/advanced-plan.jsonl:10-11`(暫存); **2026-10-17 該 `tmp/review/sol/` 已被本輪外審審查包建置程序(`fs.rmSync` 重建同名夾)覆寫而遺失**,規則 3 之結論保留,證據如需引用須重做).
- 變更紀錄: 2026-10-12.

## R-WRITE-01 單一程序內之寫入原子性

- 單一擁有者: insert(`ifNoExists`)、insertBulk(`childTransaction`)、save(`transaction`).
- 規則:
  1. 「檢查是否存在」與「寫入」於同一寫交易內完成.
  2. 回調須為同步函數,`client.put`/`client.get` 須直接在回調內,不得置於 await 之後.
  3. insertBulk 須用 `childTransaction`(非同步 transaction 中止時不回滾);存在判定用 `client.doesExist`(`:643`,鍵層不解碼,與 insert 一致;2026-10-12 前以 get 判定而會解碼).
  4. save 之快速路徑只用於判斷是否略過寫入;其正確性以 R-READ-01 為前提.
- 已知限制: 跨程序「同時」寫入不保證(README:24-40);使用端須自行序列化寫入.
- 測試: `test/unit-insert.test.mjs`、`test/unit-save.test.mjs`、`test/unit-insertbulk.test.mjs`、`test/unit-readerror.test.mjs` 9、10.
- 變更紀錄: 2026-10-12 insertBulk 改 doesExist.

## R-WRITE-02 寫入之整批性錯誤於任何寫入之前檢出

- 狀態: 已套(2026-10-14,主控裁示 D1(b)、D2、D3).
- 單一擁有者: `procPk`(`:256`,各列須為物件,判定於 `:258`)、`procWritable`(`:301`,鍵長與可編碼性)、`keyFits`(`:284`,鍵長判定,與 R-READ-03 共用).
- 規則:
  1. insert、insertBulk、save 之各列須為物件(wsemi `isobj`,即 `[object Object]`,類別實例亦屬之),否則整批 reject `can not <fn> by data[k] which is not an object`;於 procPk 內、補值之前檢查.陣列、Date、Map、Set 可被編碼,但掛於其上之 id 屬性會遺失,寫成無 id 之紀錄後 selectByPk、del 無從命中、delAll 拋錯.
  2. insert、insertBulk 於任何寫入呼叫之前以 `procWritable` 逐列檢查: 主鍵經 `keyFits`(以 lmdb 公開匯出之 `keyValueToBuffer` 量編碼後位元組數,不超過 `client.maxKeySize`;量測拋錯即視為超長);值以 `client.encoder.encode` 試編碼.任一列不可寫即整批 reject,訊息為 `can not <fn> by data[k], <原訊息>`: 鍵超長者原訊息同 lmdb 之 `Key size is larger than the maximum key size (1978)`,值不可編碼者為編碼器之原訊息.
  3. 理由: insert 於同一同步迴圈排入各列之條件寫入,寫入呼叫內同步拋錯時,其前已排入者仍於其後提交(部分寫入,且 reject 當下與下一個 check 階段皆不可見);壞鍵為本實例自上一個 check 階段起之第一筆寫入時,另留下 lmdb-js 之孤兒批次,reject 後立即 close 使行程崩潰(累積經驗「lmdb-js 之孤兒批次」).insertBulk 雖由 childTransaction 回滾而全無寫入,仍經 procWritable 以統一訊息.
  4. save 之單列鍵超長、值不可編碼維持該筆 ok:0(規格 T4 第 65 行「錯誤只影響該筆資料者為前者」,前者即該筆失敗);其讀取判定依 R-READ-03.
  5. insert、insertBulk、save、del 之 `cloneDeep(data)` 置於 try 內: 無法複製之輸入(過深巢狀、取值拋錯之 getter)屬整批性錯誤,error 事件先於 reject(規格 T10.3).
  6. 交易外之 lmdb 寫入呼叫,其鍵不得於寫入呼叫內才被發現不合法: insert 經 procWritable;del 只刪 getValue 已查得之鍵;delAll 依資料庫實際鍵(R-DELALL-01 規則 2).
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `client\.(put|del|remove|ifNoExists|ifVersion|transaction|childTransaction|transactionSync|putSync|removeSync|batch|drop|clear\w*)\(` 與 `cloneDeep\(data\)|procPk\(|procWritable\(`.
- 站點全集與判準(2026-10-14): insert 之 `ifNoExists`、`put`: 交易外,已套(procWritable,`:510`);insertBulk 之 `childTransaction`、`put`: 交易內,已套(procWritable,`:632`,訊息一致);save 之 `transaction`、`put`×2: 交易內,刻意不套 procWritable(逐筆語義,規則 4);del 之 `client.del`: 鍵由 getValue 查得,不適用;delAll 之 `client.del`: 鍵取自 getRange(`:1158`),不適用;`cloneDeep(data)`×4(`:499`、`:621`、`:744`、`:982`): 已套(try 內).
- 測試: `test/unit-writeerror.test.mjs`(insert、insertBulk 之 7 類壞列×位置、元素非物件、save 逐筆、鍵長邊界、訊息格式、cloneDeep 事件順序、子程序之 reject 後立即 close);子程序工具 `test/tools/closeAfterReject.mjs`.
- 變更紀錄: 2026-10-14;修正前實測(探測腳本 `tmp/run01-probe/`,暫存): insert 壞列居中/末者其前之列於 reject 後提交(40 案中 20 案);壞鍵居首而 reject 後立即 close 之行程崩潰(insert 7 情境、delAll 1 情境,各 3/3);陣列、Date、Map、Set 元素寫成無 id 紀錄(12 案);cloneDeep 失敗不發 error 事件(5 案).

## R-FIND-01 條件(find)之入口檢核

- 狀態: 已套(2026-10-17,第 33 項批 1;外審處置見 `C:\opensrcOper\dnow\20261017093849\agent-out\run02-實作報告.md`).
- 單一擁有者: `select`(`:339`)與 `delAll`(`:1131`)入口對 wsemi `cvOrmFind` 之呼叫(`:350`、`:1143`),及其後 select、delAll 共用之逐筆判定 `mkMatch`(`:158`,含 `hasProtoName` `:140`).共用規則與契約案例表之單一擁有者為 wsemi(`src/cvOrmFind.mjs`、`src/_cvOrmFindCases.mjs`,wsemi ≥ 1.9.19,其帳本 R-FIND-01);本套件**不得**自行放寬運算子或欄名,亦不得把後端專屬限制搬入共用函數,不傳 `opt`(`$in` 上限即共用預設 10000,本套件無更低之後端限制).
- 規則:
  1. 入口順序: closed(R-CLOSE-01)→ find 檢核 → 讀取(`getData`/`readAll`).檢核自成一個 `try`(置於主 `try` 之前),失敗即走 `emitError`(於 reject 之前恰一次)後 `Promise.reject`,訊息原樣(`invalid find: <原因> at [<路徑>]`),不發 `change`,不讀取、不刪除、不重設快取;**不進入主 try 之 catch**: delAll 之 catch 於實例已關閉時會把任何錯誤改寫為 closed 訊息,不得套用於檢核之錯誤(外審發現: 檢核途中被關閉時 select 與 delAll 訊息不對稱).
  2. 檢核所回傳之全新 null 原型複本 `fc` 為其後**唯一**使用之條件: `mkMatch(fc)`、錯誤訊息皆用 `fc`,不再讀呼叫端之 `find`;複本無自有鍵(`Reflect.ownKeys(fc).length === 0`,即未給、`undefined`、`{}`)走「全部」分支.已移除 `iseobj(find)` 之分支判斷.
  3. `select` 與 `delAll` 經同一檢核、同一規則、同一逐筆判定 `mkMatch(fc)`(`select` 以 `ltdt.filter(mkMatch(fc))`,與原 `q.find(ltdt).all()` 等價: mingo 之 `find` 即以 `test` 過濾; 系列最低要求①: select 命中之數據即 delAll 刪除之數據).
  4. `useCache` 已填入快取時,壞條件仍 reject(檢核先於 `getData`),合法條件亦只用複本.
  5. 本套件屬 MongoDB 式族(`$ne: x`、`$nin` 含 null 與缺欄之列,`$ne: null` 排除之),命中集合取案例表 `ids.mongo`;lmdb 原生保存 Date,Date 條件不需轉換.
  6. **欄名同於 `Object.prototype` 成員**(`constructor`、`toString`、`hasOwnProperty`、`valueOf` 等,白名單內合法): mingo 7.2.4 以屬性存取取值,其原型成員過濾因 `new Set(….find(...))` 之筆誤(應為 `filter`)失效,數據無此自有欄位時取到繼承之函數而非缺欄,使 `{constructor:null}` 無命中、`{constructor:{$ne:null}}` 命中全部而 delAll 刪光(修前即如此,外審 sol F2/opus F2 發現).`hasProtoName(fc)` 判定複本之欄名(含 `$and/$or` 內子條件)是否屬 `Object.prototype` 成員,是者 `mkMatch` 改以 null 原型淺複本(`Object.assign(Object.create(null), v)`,只含自有可列舉欄位)判定,命中者仍回傳原數據;否者維持直接 `q.test(v)`(不增加成本).
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `cvOrmFind\(|mkMatch\(|new mingo\.Query\(|iseobj\(find\)|hasProtoName\(`.預期: `cvOrmFind(` 2 處(select、delAll)、`mkMatch(` 定義 1 + 呼叫 2、`new mingo.Query(` 1 處(在 mkMatch 內,傳 `fc`)、`iseobj(find)` 0 處.再 Grep 搜 `\bfind\b`,逐一確認除入口外無其他讀取 `find` 處.
- 站點全集與判準: `select` `:350`(檢核)、`:370`(判定)已套;`delAll` `:1143`(檢核)、`:1164`(判定)已套;`selectByPk`、`insert`、`insertBulk`、`save`、`del` 不接受 find(不適用).
- 軸(測試輸入空間): 案例表 241 案(T 頂層、E 純量、C 比較、I in/nin、L 邏輯、N null、F 欄名、X 白名單外、V 值形狀、H 異常結構與規模)× {select, delAll} ×(REJ 案: 訊息**逐字等於** `cvOrmFind` 直接給出者、error 事件恰一次且先於 reject、不發 change、資料不變; OK/ALL 案: 命中集合、刪除集合等於 select 命中集合、`n`/`nDeleted`)+ 複本(翻轉讀值之 Proxy ×{頂層, `$or` 內, 運算子物件內}×{select, delAll}; 熱快取×{select, delAll}; `ownKeys` 第 1 次回 `['g']` 其後回 `[]` 之 Proxy(空條件分支須依複本);呼叫端物件不被改動)+ 時序(已關閉先於檢核;關閉進行中先於檢核;檢核途中被關閉仍為 invalid find;檢核先於讀取,以無法解碼之紀錄驗證;`useCache`;`delAll` 壞條件 reject 後立即 `close()` 不崩潰)+ 原型成員欄名(14 種條件 × {select, delAll} + 對照組)+ Date(原生,各運算子與邏輯位置,含完整 `{n,nDeleted,ok}`).
- 測試: `test/unit-find-whitelist.test.mjs`(286 案)、`test/tools/closeAfterReject.mjs` 之情境 `delAllBadFind`.單檔指令: `npx mocha --no-parallel test/unit-find-whitelist.test.mjs --timeout 180000`.
- 修前紅(對 HEAD 副本,方法見累積經驗「不動 src 而以修改版跑全套測試」,腳本 `tmp/runvariant.mjs`、`tmp/classify.mjs`、`tmp/reconcile.mjs`): 案例表 239 案(H19、H20 未對修前版執行,預期使修前版長時間占用)中,**判定紅(該拒收而接受,或命中集合不符,不看訊息格式)121 案** = 審計矩陣 lmdb 95 − 重判 3(F04、F05、F20 由應拒收改為應接受)+ 重判 1(H09 由應接受改為應拒收)+ 新增案 28(此數只看案例表,不含外審後增加之原型成員欄名等測試);**最終測試對 HEAD 之 mocha 層紅 186 案(共 284 案,未含 H19、H20)**,其餘為修前版雖 reject 但訊息非 `invalid find:`、事件不符,或原型成員欄名、複本、時序等新增測試.修後 286 全綠,全套 492(基準 206 + 286)全綠.
- 變異(`tmp/mutate.mjs`,25 項 + 基準 M00,皆於副本;跳過 H19、H20 以免變異版掛住; 含 select「改寫訊息但保留 `invalid find:` 前綴」,此項在僅驗前綴之原測試必存活,現由逐字比對抓到): select/delAll 各自「不檢核」「mingo 改用原物件」「檢核移到讀取之後」「檢核移到已關閉之前」「reject 時不發 error 事件」「空條件邊界改為 >1」「空條件改為無命中」「熱快取時改用原物件」「空條件判斷改讀原物件之 ownKeys」,select「改寫訊息」,delAll「判定反向」「檢核移回主 try 內」,及「原型成員欄名: 不啟用 null 原型檢視 / hasProtoName 恆假 / 不遞迴 `$and/$or`」,全數轉紅(最少僅 1 案抓到者: 檢核移到讀取之後、熱快取改用原物件、空條件判斷改讀 ownKeys、檢核移回主 try 內).
- 已知限制: ① 檢核與規則在 wsemi,wsemi 日後調整規則時本套件之案例表測試即為偵測器,不得為遷就而改測試;② H19(共享參照指數展開)、H20(`length` 為 4294967295 之 Proxy 陣列)兩案未對修前版執行;③ **shared 缺口(歸 wsemi,外審實測)**: `cvOrmFind` 對「原型被改為 null/`Object.prototype` 且帶自有可列舉鍵之內建物件」與「被 Proxy 包住者」仍接受(後者被當空條件而 delAll 刪光全表),錯誤路徑於存取器/描述子 trap 出錯時缺該欄名——本套件無從在接線層補救(接線層只能依 `cvOrmFind` 回傳之複本),已回報主控; ④ **值非物件之紀錄(他方以裸 lmdb 寫入之純量)**: mingo 之 `resolve` 對純量文件回傳純量本身,故任何欄名之等值條件(如 `{status:'x'}`)皆命中值為 `'x'` 之純量紀錄,select 回傳之、delAll 刪除之(select 與 delAll 一致; 修前即如此,屬 R-READ-03 已知限制「值非物件之紀錄」範疇,未統一);⑤ delAll 於檢核失敗時不重設快取(與 level、lowdb 一致;資料未變,快取仍有效).此為外審後之調整: 檢核原置於主 try 內,失敗後仍執行其後之 `_cache = null`.
- 變更紀錄: 2026-10-17 新增;修正前 `iseobj(find)` 為假者(非條件物件、僅 Symbol 鍵、條件在原型上)一律走全部分支,`select` 回全表、`delAll` 刪光全表;見〈破壞性變更紀錄〉.同日外審後修正: 原型成員欄名(規則 6)、檢核移出主 try(規則 1)、README 破壞性變更節之三處不實敘述(類別實例、`$in:[undefined]`、Date)與 `$ne:null`、陣列欄位、字串限制、`useCache` 預覽之說明.

## R-SAVE-01 save 之 `option.merge`:整筆覆寫於同一寫交易內完成,不經刪除

- 狀態: 已套(2026-10-18,來源: 主控 `建議w-orm-lmdb修正.md`,w-data-pipeline 之 `openCollection.replace` 以「先刪再插」整筆取代而非原子).
- 單一擁有者: `save`(`:711`)內 `merge` 之判定 `isOverwrite`(宣告 `:728`,取值於主 `try` 內 `:741`)、快速路徑之比對(`:785`)、`client.transaction` 回調內之寫入內容 `vm`(`:817`,覆寫為輸入本身,合併為 `merge({}, vv, v)`)與其後共用之 `isEqual(vm, vv)`(`:821`)/ `client.put(v.id, vm)`(`:828`).不新增函數.
- 規則:
  1. **介面**: `save(data, option)` 之 `option.merge`(布林,預設 `true` = 現行深層合併語意,行為完全不變).**僅嚴格等於 `false` 者啟用整筆覆寫**,未給、`undefined`、`true` 及其他值(`null`、`0`、`'false'` 等)一律維持合併——覆寫會丟棄舊記錄之鍵,屬較具破壞性之一方,故不採 `autoInsert` 之真假值判定(`get(option,'autoInsert',true)`).**取值於主 `try` 內**(2026-10-18 外審 sol R3): 選項取值拋錯(getter、Proxy)屬整批性錯誤,依 T10.3 先 `emitError` 恰一次再 reject、不寫入、不發 `change`;`autoInsert` 之取值仍在 `try` 外(既有行為,拋錯時 reject 而無 error 事件;範圍外,未改).
  2. **語意(merge:false)**: 各筆於**同一 LMDB 寫交易**(`client.transaction`,與合併路徑同一個回調)內以新記錄整筆 `client.put(v.id, v)` 覆寫;存入者恰為該筆輸入(經 `procPk` 之複本與補值,與外部脫勾)之內容,舊記錄不在新記錄之鍵一律不保留,陣列欄位整筆替換,巢狀物件不遞迴合併.**不經 `client.remove`/`client.del`**,無「刪後插前」之空窗;回調須為同步函數,`client.get`/`client.put` 直接在回調內(R-WRITE-01 規則 2).
  3. **「內容相同」之判定基準隨 `merge` 而異**(規格 save 一節「合併後比對」之對應): 合併 = 待寫入物件合併進現值後與現值相同(現行);覆寫 = **待寫入物件與現值相同**(`isEqual(現值, 待寫入物件)`).相同者不寫入,`{ n:1, nInserted:0, nModified:0, ok:1 }`;不同(含僅少了鍵)者寫入,`nModified:1`.快速路徑(交易外預讀)同依此基準: 覆寫時 `isEqual(vp, v)` 才略過;不得沿用 `isEqual(merge({}, vp, v), vp)`,否則輸入為現值之子集(刪鍵)時被誤判為無須寫入而刪不了鍵.
  4. **與 `autoInsert` 之組合**: 原無數據(查無、值非物件、主鍵超過鍵長上限,依 R-READ-03)時,覆寫與合併相同: `autoInsert` 為 `true`(預設)則 `put` 插入,`{ n:1, nInserted:1, nModified:0, ok:1 }` 且發 `insert` 事件;為 `false` 則不寫入,`{ n:0, nInserted:0, nModified:0, ok:1 }`.`autoInsert` 對「已存在」之筆無影響.
  5. **逐筆結果與事件同現行 save**: 回傳與輸入等長保序,各筆 `{ n, nInserted, nModified, ok }`(失敗者另附 `err`);單筆失敗(值無法序列化等)該筆 `ok:0` + `err`、`n:1`,不 reject,另發逐筆 `error` 事件;整批性錯誤(元素非物件、`autoGenPk:false` 缺 id、已關閉、過程中被 close、cloneDeep 失敗)之時序與合併路徑相同(逐筆事件緩存於 `evs`,整批 reject 者不發出,R-EVENT-01);`change('save', data, res)` 整批一次;覆寫既有筆不發 `insert` 事件(同合併路徑之更新);寫入後 `_cache = null`.
  6. **失敗不丟舊記錄**: 值無法編碼等使 `client.put` 於回調內拋錯時,寫交易整個中止,原記錄原樣保留(因無獨立之刪除步驟);`merge:false` 不得為此另行引入 `procWritable` 預檢(R-WRITE-02 規則 4: save 單列失敗維持該筆 `ok:0`).
  7. 同批內同 id 多筆: 依輸入順序逐筆套用(`pmSeries`),`merge:false` 下最後一筆之內容即終值;`merge:true` 與 `merge:false` 可混用於不同呼叫,不可混用於同一呼叫(`option` 為整批設定).
  8. **「存入者恰為輸入」之範圍**: 指輸入經 LMDB 編碼器(msgpack)往返後之結果,型別與欄名之保真範圍同 `insert` 與預設 `save`(`Set` 讀回為陣列、RegExp 讀回字串陣列、自有欄名 `__proto__` 與 `__proto_` 併為一鍵、負零讀回正零、稀疏陣列補洞);不另行保證任意 JavaScript 輸入無損.與外部脫勾由 `cloneDeep` 達成,其不複製 `Map` 之物件鍵(共用外部參照).
  9. **相同判定用 lodash `isEqual`**(合併路徑既有): 覆寫沿用而不另換比較器,以免兩路徑判準分歧;其已知限制見〈已知限制〉.
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `isOverwrite|merge\(|client\.put\(|client\.(remove|del)\(`.預期(2026-10-18 實測): `isOverwrite` 宣告 1(`:728`)+ 取值 1(`:741`,須在主 `try` 內)+ 使用 2(`:785`、`:817`);`merge(` 兩處(`:785` 快速路徑、`:817` 交易內)皆在 `isOverwrite` 為假之分支;save 內 `client.put(` 2 處(`:828` 更新,覆寫與合併共用 `vm`;`:838` 插入);save 內 `client.remove`/`client.del` 0 處(`client.del` 僅在 del `:1022` 與 delAll `:1178`).
- 站點全集與判準: 快速路徑比對 `:785`(已套,隨 `merge` 分支)、交易內存在分支之寫入內容 `:817`(已套)與其後之相同判定 `:821`(兩種 `merge` 共用,以 `vm` 比對)、交易內不存在分支之插入 `:835-838`(`autoInsert`,兩種 `merge` 共用,已套);結果 `rest` 組裝、事件緩存、`_cache` 重設兩種 `merge` 共用,不分支.
- 軸(測試輸入空間): `merge`(未給/true/false/非布林值)× 現值(不存在/存在,且存在時與輸入之關係: 輸入缺舊鍵、輸入多新鍵、陣列欄位縮短/改變/置空、巢狀物件縮減、完全相同、僅 id)× `autoInsert`(未給/true/false)× 批次形狀(單物件/陣列/同批同 id/存在與不存在混合/保序)× 失敗(值不可編碼居首/居中/居末、鍵超長)× 整批性錯誤(元素非物件、autoGenPk:false 缺 id、已關閉)× 事件(有/無訂閱者、insert/save/error 次序、逐筆事件於整批定案後才發出)× `option` 本身(null/字串/數字/布林/陣列、取值拋錯或回 false 之 getter)× 輸入隔離(呼叫後 await 前修改輸入、無 id 輸入不被補 id)× `useCache` × 併發(同 id 多個整筆覆寫、相同覆寫、覆寫期間讀取者始終見到完整記錄).
- 測試: `test/unit-save-merge.test.mjs`(63 案: merge:false 覆寫 10、內容相同判定 3+鎖定 14[含 option 本身 5]、autoInsert 組合 8、失敗保留原記錄 5、整批性錯誤 ×2 種 option 各 4、事件 4、事件時機與選項取值 3、輸入隔離 3、useCache 1、原子性/併發 4).單檔指令: `npx mocha --no-parallel test/unit-save-merge.test.mjs --timeout 180000`.預設路徑(未給 `merge`)之既有行為由 `test/unit-save.test.mjs` 等既有測試與本檔之 `[lock]` 案鎖定(修前即綠).
- 修前紅(對 HEAD 之 src 副本,腳本 `tmp/run04-mutate.mjs head`,同時跑本檔與 `unit-save.test.mjs` 共 78 案,2026-10-18 最終版測試): 紅 23、綠 55;紅者為 merge:false 之新行為案(含「`option.merge` 為 getter 回 false 時須覆寫」),綠者為鎖定案(預設合併、非布林值與非物件 option 視同合併、整批性錯誤、失敗保留原記錄[修前 merge 被忽略,Symbol 值同樣使 put 失敗而原記錄在]、事件形狀與時機、輸入隔離等).
- 變異(`FULL=1 node tmp/run04-mutate.mjs mutants`,皆於副本,對**全套 555 案**,2026-10-18 最終版): M01 `merge:false` 仍合併(紅 22)、M02 忽略 merge 選項(紅 23)、M03 先刪後插[交易外 `client.remove` 再交易](紅 23,含「失敗保留原記錄」兩案與「並行讀者不見缺漏」案)、M04 覆寫時忽略 autoInsert(紅 4)、M05 快速路徑沿用合併基準(紅 4)、M06 覆寫恆寫入不比對(紅 1,需「5 個併發相同覆寫僅一筆 nModified:1」案才抓到——快速路徑會先擋下串行之相同輸入)、M07 merge 改真假值判定(紅 4)、M08 覆寫改淺合併(紅 19)、M10 save 不 `cloneDeep`(紅 4,外審 sol 實跑存活後補「輸入隔離」案)、M11 逐筆事件提前發出(紅 1,既有全套原即抓不到,外審 sol 發現,補「事件時機」案)、M12 option 為 null 時 throw(紅 1,原「option is null」案名實不符,補真正傳 null 之案)、M13 `merge` 取值移出 try(紅 1)、M14 取值於 try 內但不發 error 事件(紅 1)全數轉紅;M09 對照組(不變更)全綠 555.
- 已知限制: 跨程序「同時」寫入不保證(同 R-WRITE-01);`merge:false` 僅 w-orm-lmdb 提供,其他後端未要求(規格 save 一節已載明);**`isEqual` 之限制(合併路徑既有,覆寫沿用,外審 sol R1,2026-10-18 以 `tmp/run04-probe3.mjs` 自行重現,兩路徑結果相同)**: ① lodash `isEqual` 以無序方式比較 `Map` 且該旗標遞迴到內層,`Map` 之鍵值互換(`{a→b}` 與 `{b→a}`)、`Map` 值內陣列重排被判相同,覆寫因此被略過(`nModified:0`,舊值保留);② 資料含自有鍵 `__wrapped__` 者,`isEqual` 把它當 lodash 包裝器呼叫 `.value()` 而拋 `object.value is not a function`,該筆 `ok:0`、舊值保留(含想以覆寫刪除該鍵者).兩者皆需 `Map` 值或保留名稱鍵之輸入,JSON 來源之數據不受影響;根治須兩路徑一併改比較器(如 `isDeepStrictEqual`),屬預設路徑之行為變更,留待主控裁示,本次未改;**類別實例輸入**(`isobj` 允許)於 `merge:false` 下,「輸入與現值相同」以 lodash `isEqual(現值, 輸入)` 判定,因建構子不同而恆不等,故相同內容仍會寫入且 `nModified:1`(2026-10-18 實測 `tmp/run04-probe2.mjs`;預設合併路徑因 `merge({}, …)` 產生純物件而回 0);存入內容正確,僅多一次相同內容之寫入,純物件輸入(含 JSON 來源)不受影響,未列為缺陷.
- 變更紀錄: 2026-10-18 新增;修正前呼叫端(w-data-pipeline `openCollection.replace`)須以 `del` 再 `insert` 兩次獨立寫入整筆取代,刪後插前失敗則原記錄消失、刪後插前他方插入同 id 則本次寫入無聲遺失.

## R-ORMSTR-01 字串良構:主鍵、寫入資料、opt 不得含孤立代理字元(主鍵並不得含 NUL)

- 重現用工具腳本(修前紅 `run-pre.mjs`、變異 `mutate.mjs`/`mutate-backup.mjs`/`mutate-generic.mjs`、`analyze.mjs`、探測與建置檢查)本機 `./tmp/` 已於結案時清除,備份於 `C:\opensrcOper\dnow\20261020172106\agent-out\批1-evidence\lmdb\tools\`(派工之外審包與報告於同夾之 `review/`).
- 狀態: 已套(2026-10-20,第 66 項批 1a).依據: dspec `C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md`〈一之三〉9.(使用者裁示 D1 全系列一律拒收寫入值與鍵名、D2 主鍵併入 NUL、D4 排序規則只文件化;主控裁示 D3 `opt` 建構 throw、D5–D8);審計 `C:\opensrcOper\dnow\20261020172106\agent-out\run01-系列審計回應.md` §5.2、§6.2;wsemi 帳本 R-ORMSTR-01;批 1 共通要求 `review\批1共通要求.md`;建議檔 `建議w-orm-lmdb修正.md`;報告 `C:\opensrcOper\dnow\20261020172106\agent-out\批1-lmdb-level-lowdb-修正報告.md`.
- 成因: lmdb 之**鍵**以 CESU-8 保真(孤立代理字串可存、可取、可刪,不與 U+FFFD 之鍵相撞),但**值**(msgpackr,含 `id` 欄與全部欄位、巢狀值、陣列元素、物件鍵名)讀回時每個孤立碼元變為**三個 U+FFFD**: ①讀回之 `id` 與寫入者不同,無法再以讀回之 id `selectByPk`/`del`(舊列孤兒);②寫入值與讀回值不同(靜默改寫),並使 `select({name: U+FFFD×3})`/`delAll` 命中該列(條件側已全系列拒收孤立代理,故殘餘碰撞皆源於寫入側改寫);③同一文件內相異鍵名(`'k'+U+D800` 與 `'k'+U+FFFD×3`)改寫後撞鍵,後者之值**無聲覆蓋**前者(資料遺失).實測見審計 §3.2、§3.3、§3.4.
- 單一擁有者: 判定與詞彙在 wsemi `chkOrmStr`、`chkOrmData`、案例表 `_cvOrmStrCases`(wsemi ≥ 1.9.24),本套件**不得**自寫孤立代理/NUL 判定(盤點: 本套件 `src/` 內無 `isWellFormed`/`toWellFormed`/代理碼元字面值).本套件接線(以函數名 Grep 定位): `chkOptStr`(建構,`:81`)、`chkPk`(`:319`,主鍵檢核,另含 `''` 之 `string is empty`)、`getWellFormedMsg`(`:333`,逐筆之主鍵與資料)、`procWellFormed`(`:346`,insert/insertBulk 整批用)、save 之 `msgs`(`:839`)與合併路徑之 `vIn` 檢核、`selectByPk`(`:500`)、`del`(`:1106`).
- 規則:
  1. **範圍**: 主鍵(各函數之 id 參數與資料之 `id` 欄): 不得含孤立代理,**亦不得含 NUL**(D2,與 `find` 同標準);寫入資料(`insert`、`insertBulk`、`save`)之字串值與鍵名,含巢狀物件、陣列元素、`Map`/`Set` 項目、`Error`/`RegExp` 之文字(wsemi `chkOrmData` 之走訪範圍): 不得含孤立代理,**值與鍵名之 NUL 不拒**;`opt.url`、`opt.db`、`opt.cl`: 孤立代理者**建構時同步 throw** `invalid opt.<名>: string contains lone surrogate`,先於 `open()`(不建立任何資料夾),不檢 NUL(`{ nul: false }`).U+FFFD 與成對代理(emoji、CJK 擴充 B 區)為良構,照常接受.
  2. **處置與既有「無效主鍵」同形**: `insert`/`insertBulk` 於任何寫入之前整批 reject、不寫入任何一筆(`error` 事件先於 reject 恰一次,不發 `change`);`save`/`del` 該筆 `{ok:0, err}`、同批其他筆照常(`save` 之 `n:1,nInserted:0,nModified:0`;`del` 之 `n:0,nDeleted:0`),逐筆 `error` 事件於整批定案後依序各一次(`evs`,同 R-EVENT-01);`selectByPk` 回 `null`(不 reject、不發事件);`select`/`delAll` 之 find 依既有 `invalid find`(不動).
  3. **錯誤訊息固定詞彙、不回顯值、含批次索引**: `invalid id: <原因> at [data[<k>].id]`、`invalid data: <原因> at [data[<k>].<路徑>]`(鍵名不合格者路徑以 `<invalid key>` 表示,單一物件呼叫之索引為 `data[0]`);`<原因>` 為 wsemi 詞彙 `string contains lone surrogate`、`string contains NUL`(僅主鍵)、`not a string`,另本套件 `del`/`selectByPk` 之空字串以 `string is empty`(`chkPk`,因 `isestr('')` 為假而 wsemi 視 `''` 為良構).**`del` 之舊訊息 `can not delete by invalid id[<值>]` 因回顯值而取代**為 `invalid id: not a string|string is empty|... at [data[<k>].id]`(`pmSeries` 回呼第二參數取批次索引,D6).**刻意未改**: `insertBulk` 之 `can not insertBulk by existed id[<id>]`(該 id 已通過檢核,屬主鍵衝突之診斷訊息,`test/unit-readerror.test.mjs` 釘住).
  4. **檢核作用於已複製之物件且只用該複本**: `insert`/`insertBulk`/`save`/`del` 之 `cloneDeep(data)` 於 `async` 函數體內、首個 `await` 之前同步執行,故呼叫後立即改動原輸入不影響(案例表 `toctou` 全過);自有存取器於 `cloneDeep` 轉為資料屬性(讀一次);順序: `isClosed` → 非物件早退 → `cloneDeep` → `procPk`(各列須為物件、`autoGenPk:false` 須帶有效 id,補主鍵) → `procWellFormed`(主鍵先於資料,同批以最小索引為準) → `procWritable`(鍵長、可編碼) → 寫入.**繼承之可列舉存取器**(類別實例之原型上,`cloneDeep` 保留原型): `save` 合併路徑之 lodash `merge`(`keysIn`)會讀取之(實測讀 3 次,每次可回不同值),故合併路徑於交易內以 `vIn = merge({}, v)`(讀一次,純資料物件)再以 `chkOrmData` 檢核,並以 `vIn` 合併(`vm = merge({}, vv, vIn)`),檢查之值即寫入之值;只檢核 `vIn`,**不檢核現值 `vv`**(`merge:false` 之 `vm` 即 `v`,msgpack 只編碼自有屬性,不須再檢);`insert` 之 msgpack 路徑只編碼自有屬性,繼承屬性被丟,不受影響.
  5. **不隨 `autoGenPk` 改變**(如實存放性檢核,同 `keyFits`);`autoGenPk:true` 時未帶有效 id 之筆先補主鍵再檢核(補之主鍵恆良構).
  6. **`isClosed` 先於一切**: 已關閉之實例傳壞輸入仍回 closed 訊息(R-CLOSE-01);`selectByPk` 之檢核位於 `isClosed` 之後.
  7. **既有資料不遷移(D8)**: 修正前以孤立代理字串為 id 寫入之舊列,鍵為孤立字串、讀回之 `id` 為三個 U+FFFD(良構,故 `isWellFormed` 檢出法**無效**);檢出法為「`select()` 之列 `selectByPk(r.id) === null`」(讀回之 id 無法再定址該列,README 載明);清除: `delAll({ id: 讀回之id })`(依值比對、依資料庫實際鍵刪除,`nDeleted:1`)、依他欄之 `delAll`、或 `delAll()`.以孤立 id 本身之 `selectByPk`/`del` 已被拒.值內已被改寫為 U+FFFD 者無從與合法 U+FFFD 區分.**主鍵含 NUL 之舊列**(原可保真存入)同樣不可由主鍵定址,上列檢出法亦涵蓋(其 `selectByPk(r.id)` 回 `null`),但 `delAll({id})` 被 find 拒(`invalid find: string contains NUL`),須依他欄 `delAll({ 欄: 值 })` 或 `delAll()`(測試以裸 lmdb 造 NUL 舊列釘住,外審 minimax#2).
  8. **排序規則弱相等(D4)**: 屬 mssql/Jet 之值語意,不適用 lmdb,不處理.
- 盤點指令: ① Grep `src/WOrmLmdb.mjs` 搜 `chkOrmStr\(|chkOrmData\(|chkPk\(|chkOptStr\(|procWellFormed\(|getWellFormedMsg\(`,預期 `chkOrmStr(` 3 處(`chkOptStr`、`chkPk`、`backup` 之 fpTar)、`chkOrmData(` 2 處(`getWellFormedMsg`、save 合併 `vIn`)、`chkPk(` 3 處呼叫(`getWellFormedMsg`、`selectByPk`、`del`)+定義、`chkOptStr(` 3 處呼叫(url、db、cl)+定義、`procWellFormed(` 2 處呼叫(insert、insertBulk)+定義、`getWellFormedMsg(` 2 處呼叫(`procWellFormed`、save `msgs`)+定義;② Grep `src/` 搜 `isWellFormed|toWellFormed|codePointAt|\\u[dD][89a-fA-F]` 預期 0(不得自寫判定);③ Grep `src/` 搜 `cloneDeep\(data\)` 預期 4 處(insert、insertBulk、save、del),各處之後之檢核與使用皆為該複本.
- 站點全集與判準(2026-10-20): 建構 `url`/`db`/`cl` 已套(`chkOptStr`,`isestr` 者才檢,非字串/空字串走預設值不檢);`selectByPk` 已套(`isestr(pk) && chkPk(pk)===null` 才查);`insert`、`insertBulk` 已套(`procPk` 之後、`procWritable` 之前);`save` 已套(`msgs` 預先計算於 `procPk` 之後,逐筆於 `try` 內最先 throw 為該筆 `ok:0`;合併路徑 `vIn` 再檢);`del` 已套(`chkPk` 取代 `isestr`);`delAll`/`select` 之 find 不適用(`cvOrmFind`,既有);`close`、`backup` 之 `fpTar` 另見 R-BACKUP-01(同一檢核標準).
- 軸(測試輸入空間): wsemi 案例表 33 形態 × 位置(主鍵入口 5 API、單一物件呼叫、資料值 6 位置 × 寫入 4 路徑、實存型別 8 種、建構 3 名稱、toctou 3 型 × 3 API、order 15)× `autoGenPk` {預設,false} × `save` 之 `merge` {預設,false};後端特有: 文件鍵名撞鍵、`name` 條件 FFFD3 不誤中、舊資料(裸 lmdb 造孤立鍵之舊列: 檢出、定址被拒、清除)、繼承存取器(合併路徑 3 型)、事件與時序(整批 reject 之 error 恰一次、save/del 逐筆事件含索引、`selectByPk` 不發事件、已關閉先於檢核、建構不建立資料夾、`autoGenPk` 補主鍵再檢核、單一物件索引、`del` 非字串/空字串不回顯、emoji/U+FFFD 往返).
- 測試: `test/unit-wellformed.test.mjs`(單檔指令 `npx mocha --no-parallel test/unit-wellformed.test.mjs --timeout 180000`;案例表逐案排程 2814 案〔API 層預設 1305 + `autoGenPk:false` 重跑 1038 + `save` 之 `merge:false` 重跑 471〕,其中 210 案為 GridFS(本後端無,`this.skip()` 並以計數斷言)、2604 案執行,另特有/事件/完整度/契約測試 26 案,合計 2630 passing、210 pending;每案全新資料表並於案後刪除資料夾;`runCase` 對每案另斷言 `error` 事件〔整批 reject 者恰一次且訊息全等 reject、逐筆失敗者依序各一次且全等該筆 `err`、回 null 與成功者無〕,外審 agyflash#5).
- 修前紅(對 git HEAD 之 src 副本搭最終測試,腳本 `tmp/run-pre.mjs`): 2840 案(含 210 pending)中**紅 1696**(含 `after` 鉤子 1: 修前無檢核使建構不 throw 而留下開啟中之環境致資料夾無法刪除)、綠 935、略過 210;案例表紅之分布(預設組): 主鍵位置 105(`insert`、`insertBulk`、`save`、`del`、`save(autoInsert:false)` 各 21 個壞形態)、單一物件 84、資料值 432(6 位置 × 4 寫入路徑 × 18 孤立形態)、實存型別 48、order 15、opt 48、toctou 3,案例表以外之特有/事件測試紅 17(+ after 鉤子 1)、綠 9(含對照組);對照審計矩陣「修前紅」: 審計 lmdb 之值/鍵 MANGLE 92 格(其 5 孤立形態 × name/nested/array/key 4 位置 × 4 寫入路徑 = 80,餘 12 為 `k_insert` 等)——本批對應 80 格**全紅**;審計 pk 位置 MANGLE 19×3 + DUPID 6×3 為其形態集(8 形態)與操作集之計數,本批案例表為 33 形態(孤立 17 + NUL 4 + …),對應之 `insert`/`insertBulk`/`save` × 17 個孤立形態 51 格**全紅**,另加 D2(NUL 4 形態)與 `del`/`save(autoInsert:false)`(審計未列為缺陷,規格新增拒收)之紅.兩者之案例分類不同,數量不可逐格相減,以「審計之紅格 ⊂ 本批之紅格」核對.
- 變異(`tmp/mutate.mjs`,皆於副本,對 `unit-wellformed.test.mjs`;對照組 M00 全綠): 拿掉 insert/insertBulk 之 `procWellFormed`、save 之逐筆檢核、資料檢核、主鍵檢核、主鍵 NUL(`{nul:false}`)、`selectByPk` 檢核、`del` 檢核、建構 url/db/cl 各檢核、資料先於主鍵、`del`/`save` 之索引固定為 0、insert/save/insertBulk 不 `cloneDeep`、`autoGenPk:false` 時略過、`root` 無索引、空字串主鍵接受、save 合併路徑不再檢核/`root` 錯誤、檢核置於 `procPk` 之前,另有 save 合併路徑改用 `v` 而非 `vIn` 合併之變異,共 **24/24 全數轉紅**(最少僅 1 案抓到者: `selectByPk` 檢核〔舊資料夾具,孤立與 NUL 兩種舊列〕、insert 不 `cloneDeep`、空字串、檢核先於 `procPk`、改用 `v` 合併〔繼承存取器自第 k 次讀取起回孤立代理〕).注: 「合併後連現值 `vv` 一併檢核」之變異對 lmdb 為等價(lmdb 之值讀回已將孤立代理改寫為 U+FFFD,現值不可能含孤立代理),故僅 level、lowdb 之變異表列之.
- 已知限制: ① `String` 包裝物件(`new String('…')`)含成對代理者,lmdb 以字元索引為鍵存放而被拆成孤立碼元——wsemi `chkOrmData` 不拒(其字串值良構),**本批不解包、不改存放形式**(主控 2026-10-20 複審裁示,wsemi 已知限制 ⑧);② `toJSON`/自訂序列化之輸出不檢核;③ 已存入之舊值(已被改寫為 U+FFFD)無從還原或辨識;④ 值與鍵名之 NUL 不在本規則(案例表 `free` 組只約束詞彙);⑤ 孤立代理之主鍵之 `selectByPk` 回 `null` 而不發事件,呼叫端無從區分「無效」與「不存在」(與規格「主鍵值無效 → null」同).
- 變更紀錄: 2026-10-20 新增.修正前(HEAD)孤立代理之 id/值/鍵名/`opt` 皆被接受(見成因).行為變更見 README「Strings must be well-formed」與〈破壞性變更紀錄〉.

## R-BACKUP-01 一致性備份 `backup(fpTar, { compact })`(lmdb 專屬擴充;Gitea issue #11)

- 狀態: 已套(2026-10-20,與第 66 項批 1a 同交付,獨立項;任務 `C:\opensrcOper\dnow\20261020205125\task.md`,建議檔 `建議w-orm-lmdb調整.md`,報告 `C:\opensrcOper\dnow\20261020205125\agent-out\run01-修正報告.md`).來源: rddmanager 之備份服務(`serbak`)須於服務運行中備份各資料表(每表一個 LMDB 環境),直接複製 `data.mdb` 可能讀到寫入中之頁面而不一致,現況以 `import { open } from 'lmdb'` 自行 `backup`.
- 介面決定(主控授權自定): **實例方法** `WOrmLmdb(opt).backup(fpTar, option)`,而非獨立函數 `backupEnv`.取捨: ①獨立匯出須改 UMD 之匯出形狀(預設匯出加具名匯出,rollup 之 `exports` 由 default 轉 named,CJS 之 `require()` 預設用法破壞),實例方法無此影響;②實例方法沿用實例之 `isClosed`/事件/`close` 語意;③缺點: 備份行程須以同一 `url/db/cl` 建構實例(建構不存在之資料表會建立空環境),README 載明「只對既有資料表備份」.JSDoc、README 載明為 lmdb 專屬擴充,不列入系列統一規格.
- 單一擁有者: `backup`(`:1360`)、`close`(`:1512`)與 `closing`(`:220`)/`isClosed`(`:224`)/`pmBackups`(`:1330`);內部以 lmdb-js `client.backup(path, compact)`(`node_modules/lmdb/open.js:431`: 僅建立其上層而**不建立目標資料夾**,實測目標不存在即回「找不到路徑」,故本函數自建;`env.cpp:918` `mdb_env_copy2`,compact 為 `MDB_CP_COMPACT`;目標已有 `data.mdb` 時 LMDB 以 `O_EXCL` 拒絕).
- 規則:
  1. **一致性**: 複製於讀交易下進行,所得為某一提交點之完整快照,不阻擋寫入;含呼叫前已 await 完成之寫入,進行中未完成者不保證含入.驗證見測試(同程序寫入中、另一程序寫入中、還原後逐筆相同).
  2. **目標**: 不存在(含上層)自建(`fs.mkdirSync` recursive,回傳值為本次建立之最上層資料夾 `created`);已存在須為空資料夾,否則 reject `can not backup by target folder which is not empty` 且不寫入任何東西;為檔案 reject `…which is not a folder`;為來源資料表資料夾自身或其內 reject `…inside the source table folder`(`path.relative` 以**實體路徑**判定: `realOf` 將已存在之最深上層以 `fs.realpathSync` 解開、其下未建立之各層原樣接回,故經符號連結/連接點之別名亦被擋;外審 agyflash#8、minimax#1,測試以 `fs.symlinkSync(…, 'junction')` 驗證;**目標為來源之上一層(資料庫資料夾)下之同層新資料夾屬允許**,與直接傳該路徑相同);完成後目標內只有 `data.mdb`(實測 `mdb_env_copy` 不產生 `lock.mdb`).
  3. **完整或不存在**: 先備份於目標內之暫存子資料夾 `.backup-<pid>-<時間>-<序號>-<亂數>`(名稱含序號與亂數,同程序同毫秒對同一目標並行備份不撞名;暫存夾**建立成功後才記入 `tmp`**,使撞名失敗之清理不誤刪他人之暫存夾,外審 agyflash#4),複製完成後**再確認目標內除本次暫存夾外別無他物**(備份期間他處於目標內建立之檔案、或並行之另一備份之成果,皆使本呼叫 reject `…not empty` 且不動該物,外審 agyflash#3/minimax#5),才以 `fs.renameSync` 將 `data.mdb` 移入目標(同資料夾內之改名為原子)、再刪暫存夾;故目標內出現之 `data.mdb` 必完整,程序被終止時至多殘留暫存夾(目標即非空,下次備份被拒,須人工移除);失敗(含移入失敗)移除暫存夾,並**自目標向上逐層以非遞迴之 `fs.rmdirSync` 移除本次所建立之資料夾至 `created` 為止,遇非空即停止**(不刪非本次建立之內容,外審 agyflash#1;舊作法為對 `created` 遞迴 `rmSync`);**Windows 之 `fs.mkdirSync(recursive)` 回傳之 `created` 帶長路徑前綴 `\\?\`**,須去除後方可與 `dir` 逐層比對(實測踩到: 未去除則只移除目標一層,測試轉紅).已存在之空目標原樣保留.不寫目標以外之位置(除其不存在之上層).
  4. **`fpTar` 字串良構**(任務指示:路徑參數適用字串良構規則): 非空字串且 `chkOrmStr`(含 NUL)通過,否則 reject `invalid fpTar: <原因>`(`not a string`、`string is empty`、`string contains NUL`、`string contains lone surrogate`);不回顯值;先於任何檔案系統存取.`isClosed` 先於檢核.
  5. **`option.compact`**: 預設 `true`;僅布林值生效,其餘視同預設(與 `autoGenPk` 同慣例);`option` 取值拋錯(getter)走同一錯誤路徑.
  6. **錯誤路徑**: 已關閉(`procClosed('backup')`)、檢核與目標不安全(固定訊息)、檔案系統或 lmdb 之錯誤(`can not backup, <原訊息>`;原訊息可能隨系統語系,如 Windows zh-TW 之 Big5 訊息於 UTF-8 解碼下為亂碼,屬 `mdb_strerror` 輸出,本套件對可預期之失敗原因另以預檢給固定訊息)皆**先發出一次 `error` 事件(mode `'backup'`,data `null`)再 reject**,不發 `change`(不異動資料).
  7. **`close()` 遇備份進行中先等候其結束**: 實測(lmdb-js 3.5.6,Windows)備份(`env.copy` 於原生執行緒)進行中呼叫 `client.close()` 使行程**段錯誤**(離開碼 139).故 `backup` 於 `pmBackups` 登記恆不 reject 之 Promise,`close()` 見 `pmBackups.size > 0` 時先設 `closing = true`(令 `isClosed()` 為真,新操作立即 reject closed,R-CLOSE-01)再 `await` 全部備份,才呼叫 `client.close()`;無備份進行中時 `closing` 恆為 false,`close()` 同步呼叫 `client.close()`,行為與修正前相同(狀態即時轉 `closing`).
  8. 回傳 `{ ok:1, fpData(data.mdb 之絕對路徑), size(位元組), compact }`.
- 盤點指令: Grep `src/WOrmLmdb.mjs` 搜 `client\.backup\(|fs\.|pmBackups|closing`,預期 `client.backup(` 1 處(`backup` 內)、`pmBackups` 4 處(宣告、`add`、`delete`、`close` 內 `size` 與等候)、`closing` 3 處(宣告、`isClosed`、`close`);Grep `toolg/gDistRollup.mjs` 之 `external` 含 `fs`、`path`(新增 `import fs/path` 之 UMD 建置需要,實測 `tmp/build-check.mjs` 建置成功且產物 `require("fs")`、`require("path")`).
- 站點全集與判準: `backup` 入口 `isClosed` 已套、`fpTar` 檢核已套、`option.compact` 已套;`close` 等候已套;其他函數不受影響(`isClosed` 加 `closing` 後,無備份進行中時恆同前).
- 測試: `test/unit-backup.test.mjs`(25 案: 基本功能 6〔自建與只有 data.mdb、還原後逐筆相同含 selectByPk 與唯讀開啟、備份不改變來源且實例可續用、空資料表、`compact` 預設/布林/非布林與碎片下大小差、相對路徑與中文 emoji 路徑〕、目標路徑安全 8〔空資料夾沿用、非空拒收與事件、重複備份至同目標、檔案、來源資料夾自身或其內、經連接點別名、上層為檔案、失敗不留殘餘〕、失敗與崩潰之殘餘 4〔注入 `fs.renameSync` 失敗之清理含他人於其間建立之內容不被刪、備份期間他處於目標內建檔之再檢查、同程序同目標並行備份恰一成功、備份進行中 `SIGKILL` 後目標內無殘缺 data.mdb〕、字串良構 2、已關閉 2、一致性 2〔同程序持續寫入、另一程序持續寫入且填充約 80MB 使備份耗時〕、備份進行中 `close()` 1〔子程序,離開碼 0〕);子程序工具 `test/tools/batchWriter.mjs`(裸 lmdb,每交易寫一批 40 筆、批號連續遞增,故任一提交點之快照必為「批 0 至批 m-1 各完整」之前綴)、`closeDuringBackup.mjs`、`killDuringBackup.mjs`、`countRows.mjs`(於子程序開啟可能殘缺之檔,避免測試程序暴露於原生層存取違規).單檔指令 `npx mocha --no-parallel test/unit-backup.test.mjs --timeout 180000`.
- 變異(`tmp/mutate-backup.mjs`,皆於副本,對 `unit-backup.test.mjs`,含子程序崩潰判紅;對照 B00 全綠): 24 項(含外審後新增 B20 清理改回遞迴刪 `created`、B21 無移入前再檢查、B22 暫存夾名撞名、B24 不用實體路徑、B25 未去除 `\\?\` 前綴)——直接複製檔案取代 env copy(B01: 同程序一致性測試以「寫入被同步複製阻塞」轉紅,跨程序測試於本機 3/3 次**未見撕裂**,見已知限制)、不自建目標、不檢非空/非資料夾/來源內、不驗 `fpTar`、不經暫存夾直接寫目標(B07: 注入失敗與強制終止測試轉紅)、`close` 不等候(子程序段錯誤轉紅)、不設 `closing`、忽略 `compact`、不發 error 事件、不加錯誤前綴、不清 `created`/暫存夾、不檢已關閉、`size` 錯、成功後不刪暫存夾、非布林 `compact` 不視同預設、不登記 `pmBackups`,**全數轉紅**.
- 已知限制: ① 跨程序一致性測試對「直接複製 `data.mdb`」之錯誤作法於本機未見撕裂(LMDB 寫時複製使撕裂需特定頁面重用時序),故該測試證明「備份未撕裂」而非「直接複製必撕裂」;其鑑別力主要在檢出缺筆、批不連續、填充列不全;② 備份進行期間環境持有讀交易,資料庫檔可能暫時增大;③ 程序被終止後殘留之 `.backup-*` 須人工移除;④ 備份行程對不存在之資料表建構實例會建立空環境;⑤ 僅 lmdb 提供,不列入系列統一規格;⑥ POSIX 上 `fs.renameSync` 於目標檔已存在時為原子覆寫(Windows 則拋錯): 再檢查(目標內除暫存夾外別無他物)與改名之間仍有極小空窗,於該空窗內他處建立同名 `data.mdb` 者在 POSIX 會被覆寫(本機為 Windows,未於 POSIX 實機驗證).
- 變更紀錄: 2026-10-20 新增.

## R-DIAG-01 診斷輸出與對外錯誤值之遮罩:用 wsemi 共用模組(`getDiag`、`getDiagSecrets`),本套件只宣告秘密來源、代碼集合、具名樣式與 safe 鍵(排程第 161 項,mig-lmdb;前身第 153 項 lmdb-run01 之自寫版本已刪)

- 狀態: 已遷移(2026-10-29,任務 `C:\opensrcOper\dnow\20261029002013`,報告 `agent-out\mig-lmdb-修正報告.md`).**規則正本: 跨套件規格 `C:\opensrcOper\dspec\診斷輸出與遮罩.md`(D1~D5、Q1~Q4、P1~P9)與 wsemi 帳本 R-DIAG-01(`sgud/rule/rulebook_diag.md`,來源 repo `C:\opensrc\開源-JS-002-3-wsemi\wsemi\sgud\rule\rulebook_diag.md`);本節只記本套件之宣告、站點全集與接線測試,不重述遮罩細則(文字遮罩、細節拷貝、對外值、摘要、出口之行為以 wsemi 之 JSDoc 與其帳本為準).**依賴 `wsemi >= 1.9.31`.
- 單一擁有者: wsemi(`getDiag`、`getDiagSecrets`、`diag.err`、`diag.mask`、`diag.brief`);**本套件不得另寫遮罩或診斷函數**(`src/getDiag.mjs` 已刪;`src/` 只剩 `WOrmLmdb.mjs`、`reqMingo.js`).本套件接線全在 `src/WOrmLmdb.mjs` 建構段(`diag`、`ownErrs`、`mkErr`、`outErr`、`outMsg`)與各 catch 站點.
- 宣告(建構段,`getDiag(opt, {…})`): `secrets: getDiagSecrets({ paths })`(`paths` 為 `url`、`url/db`、`url/db/cl` 三層各取原樣、去尾斜線、`path.normalize`、`path.resolve` 共 12 個基底;**本套件無帳號、密碼、權杖,故 reject 清單為空,對外錯誤值只靠 wsemi 之通用憑證樣式〔`scheme://帳密@`、Bearer、`token=`/`password=` 等鍵值對〕**);`patterns: ['fsPathTail', 'receivedTail']`(只作用於診斷輸出之訊息段,對外值不用);`codes: ['lmdb']`(`MDB_xxx`);`safeKeys: ['mode']`;不宣告 `types`、`nested`.
- 規則:
  1. **預設只輸出固定標籤與摘要**: 唯一的診斷輸出是 change 或 error 事件之訂閱函數拋錯時,`diag('change handler threw' | 'error handler threw', { mode, ...diag.brief(err) }, () => err, 'log')`,預設 `console.log(label, { mode, errType[, errCode] })`;`errType` 只列內建類別名(其餘 `Error`),`errCode` 只收 SYS、NODE、`lmdb` 集合內之明列代碼(**整數與樣式相符之字串不再收**).label 為固定字面值,不做文字遮罩(固定字串挖空可反推短密碼).
  2. **`logDetail`/`funLog`** 之語意以 wsemi 為準: `logDetail === true` 才生效,輸出 `console.log(label, 遮罩拷貝)`(**兩引數;原 `bare` 單引數無標籤之形式已刪**);`funLog` 為函數才生效,給予時只交給它.選項於建構時取快照,型別不符者忽略.
  3. **兩種清單**: 診斷輸出(console、funLog)用 detail 清單(儲存路徑＋憑證樣式＋具名樣式);**對外錯誤值用 reject 清單(只憑證,不遮儲存路徑與單獨帳號;dspec Q3/D3)**.
  4. **外來值經 `diag.err`,本套件自身之固定詞彙錯誤不包**: catch 站點取得之錯誤〔lmdb、檔案系統、msgpackr 編碼/解碼器、mingo、使用端資料之 getter〕經 `outErr`(`ownErrs.has(err) ? err : diag.err(err)`);逐筆 `err` 與編碼失敗/backup 之訊息後綴經 `outMsg`(`diag.mask(getErrMsg(err))`);建構時 `open()` 失敗 `throw diag.err(err)`.**本套件自身以固定詞彙組成之錯誤以 `mkErr` 建立並登記於 `ownErrs`(WeakSet)後原樣交出**——含回顯呼叫端自給之 id、欄名、運算子名者(`can not insertBulk by existed id[<id>]`、`invalid data: … at [data[k].<鍵名>]`、`invalid find: … [<名>]`)屬回傳契約,不得被遮罩改寫.`backup` 原用自有屬性 `isBad` 辨識自身錯誤(外來錯誤可偽造),改以 `ownErrs`.
  5. **`'error'` 事件之訊息與 reject 同源**: 站點先取得遮罩後之值(`res = outErr(err)`),再 `emitError`(其訊息 `getErrMsg(res)`)與 `Promise.reject(res)`;逐筆失敗之 `err` 於建立時即遮,change 事件與回傳值同源.
  6. **level**: 兩個 diag 站點皆 `'log'`(寫 stdout,與修改前之 `console.log` 相同);由子程序實跑驗證 stdout/stderr.
  7. **回傳契約不變**: 各操作之 resolve/reject 內容(本套件自身之錯誤)、事件 payload 與時機;以修改前 HEAD(dfa8bb7)產生之固定預期值 `test/tools/diagGolden.json` 對照(`test/tools/diagScenario.mjs`).
  8. **永不使呼叫端流程拋錯**: 由 wsemi 保證(`diag` 與遮罩本身之失敗不外溢).
- 盤點指令(`test/unit-diagsite.test.mjs` 以原始碼掃描鎖定,去除註解後): ① `src/` 只有 `WOrmLmdb.mjs`、`reqMingo.js`,且無 `maskOpenErr|maskPathText|getSecrets|getBriefErr|getOptLog|cpMask|maskText`;② `\bconsole\.` 0 處、`process.stdout|stderr` 0 處;③ `diag(` 2 處(label 為字面值、level `'log'`、safe 為 `{ mode, ...diag.brief(x) }`);④ `res = outErr(err)` 7 處、`err: outMsg(err)` 2 處、`${outMsg(err)}` 2 處、`throw diag.err(err)` 1 處、`ownErrs.has(err)` 3 處、`res = err` 與 `err: getErrMsg(err)` 0 處;⑤ `new Error(` 僅 4 處(建構時良構檢核、`mkErr`、已關閉之 `procClosed`、`backup` 之 reject 包裝);⑥ `Promise.reject(` 之對象只可為 `err`(2 處,cvOrmFind 檢核)、`res`(7 處)、`new Error(msg)`(2 處);⑦ `.emit('error'` 僅 `emitError` 1 處.
- 站點全集 × 處置(2026-10-29 以 Grep 列全,函數名定位;行號為當日現況):

  | # | 站點(函數,行) | 值的來源 | 處置 | 出口 | 測試 |
  |---|---|---|---|---|---|
  | 1 | 建構段 `open()` catch(`:186`) | 外來(lmdb、Node fs) | `throw diag.err(err)` | throw | `unit-urlerr`(13 情境＋3) |
  | 2 | `emitChange` catch(`:315`) | 使用端訂閱函數拋出之值 | `diag(…, () => err, 'log')`;細節由 wsemi 遮罩 | stdout / funLog | `unit-diaglog`(語料 detail 9 案＋brief 1 案,各×2 站點)、`unit-diaglog-run`、`unit-diagsite` |
  | 3 | `emitError` catch(`:327`) | 同上 | `diag(…, () => errEmit, 'log')` | stdout / funLog | 同上 |
  | 4 | `emitError` 之 `ee.emit('error', mode, data, getErrMsg(err))`(`:324`) | 已遮罩之值或自身固定訊息 | 站點先遮再傳入(見 5~15) | 'error' 事件 | `unit-diagerr` |
  | 5 | `select` catch(`:515`) | 外來(解碼、mingo、lmdb) | `res = outErr(err)` | reject＋事件 | `unit-diagerr`(解碼接縫) |
  | 6 | `selectByPk` catch(`:571`) | 外來 | `res = outErr(err)` | reject＋事件 | 同上 |
  | 7 | `insert` catch(`:692`) | 外來(使用端 getter、編碼、lmdb)＋自身 | `res = outErr(err)`;自身 `mkErr` | reject＋事件 | `unit-diagerr`(getter 接縫、固定詞彙案) |
  | 8 | `insertBulk` catch(`:800`) | 同上 | `res = outErr(err)`;已關閉改寫 `mkErr` | reject＋事件 | 同上 |
  | 9 | `save` 外層 catch(`:1082`) | 外來＋自身 | `res = outErr(err)` | reject＋事件 | 同上 |
  | 10 | `save` 逐筆 catch(`:1049`) | 外來＋自身 | `err: outMsg(err)`(建立時遮) | 結果物件 `err`＋逐筆事件 | `unit-diagerr`(解碼接縫、編碼接縫) |
  | 11 | `del` 外層 catch(`:1235`) | 外來＋自身 | `res = outErr(err)` | reject＋事件 | 同 7 |
  | 12 | `del` 逐筆 catch(`:1216`) | 外來＋自身 | `err: outMsg(err)` | 結果物件 `err`＋逐筆事件 | 同 10 |
  | 13 | `delAll` catch(`:1350`) | 外來；已關閉改寫 `mkErr` | `res = outErr(err)` | reject＋事件 | 同 5 |
  | 14 | `procWritable` 編碼失敗(`:438`) | 外來(編碼器訊息)＋固定前綴 | `mkErr(前綴 + outMsg(err))` | 隨 7、8、10 | `unit-diagerr`(編碼接縫) |
  | 15 | `backup` catch(`:1550`) | 外來(fs、lmdb)；自身檢核錯誤(`bad` = `mkErr`) | 自身原樣;外來 `can not backup, ${outMsg(err)}` | reject＋事件 | `unit-diagerr`(fs 接縫、非空資料夾) |
  | 16 | 條件檢核 catch(`select`、`delAll` 入口,cvOrmFind) | 本套件自身之固定詞彙(回顯欄名/運算子名) | **不包**(`emitError`＋`Promise.reject(err)` 原樣) | reject＋事件 | `unit-diagerr`(固定詞彙案) |
  | 17 | 自身固定詞彙錯誤:`procClosed`、`prepareRead`、`procPk`、`procWellFormed`、`procWritable` 鍵長、`insertBulk` 衝突、`save`/`del` 之良構與已關閉 | 自身 | `mkErr`(或直接 `new Error` 後立即 reject〔`procClosed`、建構時 `invalid opt.*`〕),**不包** | reject/throw＋事件 | `unit-diagerr`(固定詞彙案)、`unit-diagsite`(`new Error` 全集) |
  | 18 | label、safe(`diag` 的 1、2 參數) | 固定詞彙 | **不遮**(wsemi 規則);safe 白名單 `safeKeys` | — | `unit-diaglog-run`、`unit-diagsite` |
  | 19 | lmdb-js 自身之 `console.error/warn` 與原生層直寫 stderr | 第三方 | **不攔**(JS 層無從攔截;見已知限制) | stderr | README、本帳本(依專案原則 15,不向上游通報) |

- 軸(測試輸入空間): 站點(上表 1~16)× 出口(reject、`'error'` 事件、逐筆 `err`、throw、stdout、funLog)× 值的來源(外來含憑證、外來不含憑證、外來自帶儲存路徑、自身固定詞彙、自身回顯憑證形式之文字)× 憑證形式(`scheme://帳密@`〔雙斜線存留與否〕、`token=`、`password=`、`Bearer`)× 診斷輸出之儲存位置形式(三層、原樣/絕對/斜線互換/去尾斜線)× 設定(預設、logDetail、funLog)× 站點之 level(stdout/stderr).
- 測試: `test/unit-diaglog.test.mjs`(26 案: 語料 wire 案處置表 1＋detail 9 案×change/error 兩站點 18＋brief 1 案×2 站點 2＋patterns/paths 接線 5)、`test/unit-diagerr.test.mjs`(22 案: 外來值 13＋自身固定詞彙 7＋語料 T07/R01 2)、`test/unit-urlerr.test.mjs`(16 案: 建構失敗 13 情境＋3)、`test/unit-diagsite.test.mjs`(10 案: 站點盤點 8＋stdout/stderr 子程序 2)、`test/unit-diaglog-run.test.mjs`(29 案: 實例層,含對照 HEAD 之固定預期值);工具 `test/tools/diagScenario.mjs`、`diagGolden.json`、`genDiagGolden.mjs`(只可對修改前之程式重產)、`diagStream.mjs`.**形態類案(深度、循環、Proxy、二進位、包裝物件、存取器、Map/Set、stack 條件等)由 wsemi 共用語料 `_diagCases.mjs` 於 wsemi 內逐案測(有/無 util 兩環境),本套件以其 wire 案經真實站點重跑**,處置表見 `unit-diaglog` 之 `WIRE`(語料新增 wire 案時該測試失敗,須判定適用與否).接縫(皆為真實執行路徑上的外來程式,非替換被測函數): 暫時替換 `msgpackr` 之 `Unpackr.prototype.decode`、以 `addExtension` 登記編碼拋錯之類別、使用端資料 getter 拋錯、備份目標置於一般檔案之下.單檔指令 `npx mocha --no-parallel test/unit-diaglog.test.mjs test/unit-diagerr.test.mjs test/unit-urlerr.test.mjs test/unit-diagsite.test.mjs test/unit-diaglog-run.test.mjs --timeout 180000`.
- 修前紅(`git archive HEAD src` 之副本搭五個測試檔〔當時 102 案;其後 `unit-diagerr` 增 1 案、Q3 案增斷言〕,`tmp/mig-lmdb/head/`,日誌存證據夾 `red-head-run.log`): 102 案**紅 44、綠 58**(綠者為回傳契約對照、預設輸出、自身固定詞彙之原樣案〔修前後皆原樣,屬防過度遮罩之守門,由變異 F 系列驗證其鑑別力〕);44 案紅於: 語料 detail 接線(K09、S07、T07 等修前不遮憑證鍵名/通用憑證/Map 與 Set 內容)、patterns(修前 detail 不遮 fs 路徑尾段)、外來值 reject 未遮(11)、建構錯誤改為原樣(13)、站點盤點(6)、logDetail 兩引數(4).
- 變異(`tmp/mig-lmdb/mutate.mjs`,於副本,對上列五個測試檔;基準全綠 103 案): **42 項全數殺死、存活 0**;其中 `F10`(`procWritable` 鍵長錯誤未登錄為自身)與 `L3`(省略 level 引數,預設即 log)為行為等價變異,僅被站點盤點掃描(`unit-diagsite`)殺死,其餘皆被行為測試殺死.涵蓋: 7 個操作站點漏包、逐筆 err 未遮(2)、編碼訊息/backup 訊息未遮、建構時 open 未經 diag.err、誤包固定詞彙(`outErr`/`outMsg` 不分自身外來、backup 自身錯誤加前綴、5 處 `mkErr` 未登錄、`mkErr` 不登錄、`bad()` 未登錄)、宣告(paths 各形式 5、patterns 3、codes、safeKeys、opt)、出口(level 誤為 error 2、safe 改為訊息文字 2、label 改變 2、細節未遮 1).
- 已知限制: ① lmdb 驅動自身之輸出不攔——JS 層 `console.error/warn` 與**原生層直寫 stderr**(`compression.cpp:86-89` 於 LZ4 解壓失敗時傾印該筆紀錄全部位元組之十進位值;dspec §5 載明為不可攔範圍,依專案原則 15 於 README 與本帳本寫明部署面之注意〔視該程序之 stderr 為敏感〕,**不向上游通報**);② **對外錯誤值不遮儲存路徑(Q3)**: 建構錯誤、reject、事件訊息與逐筆 `err` 保留完整儲存路徑;`url` 若寫成含帳密之形式,檔案系統訊息中的 `scheme://` 經路徑正規化成 `scheme:\`(雙斜線消失),通用樣式不認得而不遮(`token=`、`password=`、Bearer 與 Node 以 `util.inspect` 回顯而雙斜線存留者仍遮);**本套件無帳密欄位,`url` 是資料夾,請勿放憑證**;③ 本套件不宣告帳號、密碼、權杖,reject 清單為空,故 wsemi 語料中依宣告憑證遮罩之案(R02、R04、R12)不適用;④ wsemi 之共同限制(私有欄位、Proxy 偵測、跨 realm、reject 清單短密碼不遮、載入前 `Error.prepareStackTrace` hook 等)見 dspec §5;⑤ 含憑證之 Node 內部錯誤(`NodeError` 子類)經 `diag.err` 之副本其原型為內建基底類別(`TypeError.prototype`),`instanceof`、`constructor.name`、`code`、`errno`、`syscall` 不變(wsemi 行為);⑥ 診斷輸出之儲存路徑形式長度 <3 者不單獨列為秘密(wsemi `getDiagSecrets` 門檻;修改前為 <4),預設 `url: './_db'` 之正規化形式 `_db`(3 字元)現於診斷輸出被遮;⑦ 測試於 Windows 實跑,POSIX 路徑形式未於實機驗證;⑧ 測試接縫依賴 `msgpackr`(lmdb 之相依,hoisted)之 `Unpackr.prototype.decode` 與 `addExtension`,lmdb/msgpackr 升級後若改為實例方法須調整接縫.
- 變更紀錄: 2026-10-29 自第 153 項之自寫版本遷移至 wsemi 共用模組(mig-lmdb);行為變更見 README「Breaking changes (破壞性變更: diagnostic output)」及〈破壞性變更紀錄〉.2026-10-28 新增(自寫版本,已刪).


## 〈破壞性變更紀錄〉

- 2026-10-29(排程第 161 項,診斷輸出與遮罩改用 wsemi 共用模組;依 dspec Q3;不設過渡期;版號由使用者統籌;需 `wsemi >= 1.9.31`)——① **對外錯誤值不再遮儲存路徑**: 建構時 `open()` 失敗之例外、各操作之 reject、`'error'` 事件之訊息、`save`/`del` 逐筆 `err` 保留完整儲存路徑(`err.path` 不再為 `'***'`),只遮憑證(`scheme://帳密@`、Bearer、`token=`/`password=` 鍵值對);含憑證者為同類別副本(保留 `code`、`errno`、`syscall`、`path` 與其餘自有屬性,`cause` 亦保留),不含者為原物件;② `logDetail: true` 由 `console.log(<遮罩拷貝>)` 單引數改為 `console.log(label, <遮罩拷貝>)` 兩引數;③ 摘要 `errCode` 改為明列集合(系統與 Node 代碼、`MDB_xxx`),整數與僅樣式相符之字串不再收,`errType` 只列內建類別名(其餘 `Error`);④ 診斷輸出之儲存路徑遮罩門檻由「各回顯形式 <4 字元不列」改為 wsemi 之「<3 字元不列」,預設 `./_db` 之正規化形式 `_db` 現於診斷輸出被遮;⑤ 編碼失敗(`can not <op> by data[k], <編碼器訊息>`)與 `backup` 之外來訊息中的憑證被遮.回傳契約(本套件自身之錯誤文字、事件 payload 與時機)不變.
- 2026-10-28(第 153 項,診斷輸出;不設過渡期;版號由使用者統籌)——① 訂閱函數(change/error)拋錯時之 console 輸出由 `console.log(<拋出值>)` 改為 `change handler threw { mode, errType[, errCode] }`/`error handler threw {…}`(不含訊息、堆疊、資料值),`logDetail: true` 可還原、`funLog` 可取完整細節;② 建構時 `open()` 失敗之例外訊息不再回顯儲存路徑(`***`),前綴、型態、`code`、`errno`、`syscall` 不變.新增選項: `opt.logDetail`、`opt.funLog`.回傳、事件 payload 與時機不變.
- 2026-10-20(第 66 項批 1a,字串良構;不設過渡期;版號由使用者統籌)——本套件原接受、現 reject 或訊息改變者: ① 主鍵含孤立代理或 NUL(原: 孤立代理者鍵保真而 `id` 欄讀回為三個 U+FFFD、NUL 保真);② 寫入資料之字串值、鍵名含孤立代理(原: 讀回為三個 U+FFFD,鍵名相撞者無聲覆蓋另一欄);③ `opt.url`/`db`/`cl` 含孤立代理(原: 路徑經檔案系統改寫而與他名稱共用同一資料夾,現建構 throw);④ `del` 對未給/非字串/空字串 `id` 之 `err` 由 `can not delete by invalid id[<值>]` 改為 `invalid id: not a string|string is empty at [data[k].id]`(去回顯、含索引);⑤ `selectByPk` 對含 NUL 或孤立代理之主鍵回 `null`(原: 孤立代理者可取到舊孤立鍵之列).新增功能: `backup`(R-BACKUP-01).
- 2026-10-17(第 33 項批 1,條件(find)系列白名單;不設過渡期,fail-closed;版號依全庫既有慣例,由使用者統籌發布)——本套件原接受或靜默放行、現 reject 或語意改變者(方案 §5.1):
  - **移除(R-)**: mingo 運算子 `$not $nor $regex $options $exists $type $mod $elemMatch $size $all $expr $where(函數) $bitsAllSet` 及其餘白名單外運算子;`RegExp` 值(`{name:/x/i}`、`$in:[/x/]`);陣列字面值之等值、巢狀物件等值(`{tags:['x','y']}`、`{o:{k:1}}`)與 `$elemMatch/$all/$size`(欄位值為陣列時,純量與 `$in` 條件之元素比對語意仍保留: `{tags:'x'}` 命中 `tags:['x','y']`);點路徑(`{'o.k':1}`).
  - **改為 reject(R+)**: 頂層 `null`、陣列(含 `[]`、`[{…}]`)、字串、數字、布林、函數、`Date`、`RegExp`、`Map`、`Set`、`new String`、`BigInt`、`Symbol`、僅 Symbol 鍵物件、條件在原型上之物件(原 `select` 回全表、`delAll` 刪光;帶自有鍵之類別實例原即由 mingo 拒收,訊息為 `query criteria must be an object`,現統一為 `invalid find`——外審實測更正: 方案 §5.1 稱類別實例原亦全表,不實);`undefined` 值(`{age:undefined}`、`$eq:undefined` 原命中 null/缺欄列;`$in:[undefined]` 原無命中——外審實測更正: 方案稱其命中 null/缺欄列,不實;稀疏陣列之洞原被當缺欄);`NaN`、`±Infinity`、無效 `Date`、`BigInt`、函數、`Symbol` 值;`$in/$nin` 元素含 null 或非純量;`$nin:[]`、`$and:[]`、`$or:[]`、`$and:[{}]`、`$or:[{}]`、`{age:{}}`(原多為刪光全表);白名單外欄名(空字串、連字號、空白、`__proto__` 等);存取器、不可列舉鍵、Symbol 鍵;巢狀深度 > 32、`$in` > 10000 等.
  - **文件**: README 新增「條件(find)白名單」與「破壞性變更」兩節;`README.md`、`g-basic.mjs` 之「mingo 不支援 regex」註解更正(原註解與 mingo 7.2.4 不符,實為系列白名單不支援 `$regex`).
  - **影響面**: 庫內呼叫端(w-web-task、w-web-sso、w-web-perm、w-web-api、w-knowledge-extract、w-data-syncer、w-data-pipeline 等)只用欄位等值與兩處非空 `{id:{$in:[…]}}`,皆在白名單內;w-web-sso `server/procCore.mjs` 若有路徑傳入 `undefined` 值,原命中 null/缺欄列,現 reject(行為改善,待該專案確認);w-web-perm、w-web-sso 之本套件測試替身(`test/tools/unitOrm.mjs`、`test/tools/unitProc.mjs`)改變了對外語意(非條件物件之 find 由放行改為 reject),已請主控決定是否通知;庫外專案由 README 破壞性變更節告知.

## 〔條件(find)之支援範圍〕(主控 2026-10-16 依使用者交辦寫入;2026-10-17 白名單實作後更新)

- 系列盤點與處置原則:`C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md`.
- 轉譯路徑(2026-10-17 起):入口先經 wsemi `cvOrmFind` 檢核(R-FIND-01),select 以 mingo 7.2.4 之 `new mingo.Query(fc).find(...)`、delAll 以同一複本 `fc` 之 `q.test(dt)` 逐筆判定(同一引擎、同一條件複本).
- 支援範圍(2026-10-17 起):**系列條件白名單**(〈系列條件白名單〉;細節與上限見 wsemi `cvOrmFind` 與規格 T11)——頂層未給/`undefined`/`{}` 為全部、欄位等值(純量)、`$eq $ne $gt $gte $lt $lte`、`$in $nin`、文件層 `$and $or`;mingo 運算子全集(`$not $nor $regex $options $exists $type $mod $elemMatch $size $all $expr $where` 等)自此**不再支援**(原支援範圍見〈破壞性變更紀錄〉).
- 不支援之處置:白名單外者由 `cvOrmFind` 於入口整批 reject(`invalid find: <原因> at [<路徑>]`),不再仰賴 mingo 之 `unknown query operator`.
- 語意:NULL 為 MongoDB 式(`{f:null}` 含缺欄;`$ne`、`$nin` 亦含 null 與缺欄之列;`$ne:null` 排除缺欄與 null);可攜寫法見 README.原 README 註解「mingo 不支援 regex」與 mingo 7.2.4 不符之文件錯誤已於 2026-10-17 更正為「系列條件白名單不支援 `$regex`」(`README.md`、`g-basic.mjs`).
- 已查證之限制:`$where`、`$function` 只接受 Function 物件(mingo 無 `new Function`/`eval`),經 JSON 傳入之條件不能執行程式碼——非缺陷.
- ~~待辦/系列議題:非物件之 find 視為無條件~~ 已於 2026-10-17 依〈delAll 與 find 之頂層語意〉B 裁示修正(傳了值卻不是條件物件者 reject).
- **非缺陷聲明**:本套件之支援範圍(運算子集合、NULL、`$regex`、大小寫之語意)屬設計.**不得**僅因「MongoDB 或其他 w-orm 套件支援」而視為缺陷、撰寫建議書或自行擴充;擴充或改變語意須主控同意(有更好之解法,並有全盤矩陣與第三方審佐證時才同意).
- **系列最低要求(違反者才屬缺陷)**:①同一 find 於 select 命中之數據即 delAll 刪除之數據;②不支援之運算子與無法轉譯之值必須 reject,不得靜默忽略、改變語意或擴大刪除範圍;③不得因條件清單恰為空或值為 `undefined` 而刪全表.

## 〔delAll 與 find 之頂層語意〕(主控 2026-10-16 依使用者裁示寫入;全系列規格)

- **`delAll()` 不給條件就是全刪;給條件才是部分刪除.**「不給條件」指:未傳 find、傳 `undefined`、或傳空物件 `{}`——三者皆為全部(select 亦同).
- **傳了值但不是條件物件者一律 reject**(使用者裁示 B):`null`、陣列、字串、數值、布林、函數、Date、RegExp、Map、Set、類別實例等,以整批性錯誤 reject,不得視同 `{}` 而全刪.適用於所有接受 find 之函數.
- 規格出處:`C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md` 〈一之一〉.本條已於 2026-10-17 實作(R-FIND-01).

## 〔系列條件白名單〕(使用者 2026-10-17 裁示;主控寫入)

- 全文與定案細節在 `C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md`〈一之三〉;本節為摘要,兩者不一致時以 dspec 為準.
- **只支援白名單,其他一律不支援(reject,整批性錯誤,不得靜默忽略)**:
  1. 頂層:未給、`undefined`、`{}` 為全部;其餘須為條件物件,否則 reject(〈delAll 與 find 之頂層語意〉).
  2. 欄位直接等值 `{欄: 純量}`,多欄隱含 AND.
  3. 欄位運算子 `$eq $ne $gt $gte $lt $lte`(純量)、`$in $nin`(純量陣列).
  4. 文件層 `$and`、`$or`(條件物件之陣列).
  5. 其餘(`$not $nor $regex $options $exists $where $expr $function $like $ilike $custom`、子查詢、欄位層 `$and/$or`、sequelize `Op` Symbol、函數值等)一律 reject.
- 純量型別、NULL 語意、空 `$in/$nin`、欄名文法、上限等細節由排程第 33 項審計提案、主控複審後定案,回寫 dspec 與本節.
- **delAll 之 JSDoc 須載明使用須知**(使用者 2026-10-17):不給條件(未給、`undefined`、`{}`)即刪除全部;傳非條件物件會 reject;白名單外運算子會 reject;delAll 須審慎使用、非必要不用.經 w-serv-webdata 資料通道呼叫者語意與本套件一致,不另加限制.
- 狀態:**已套(2026-10-17,第 33 項批 1;見 R-FIND-01)**.
