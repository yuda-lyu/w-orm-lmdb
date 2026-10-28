//診斷輸出之單一出口(R-DIAG-01): src內所有console輸出皆經getDiag回傳之diag, 除本檔外不得直接呼叫console
//預設只輸出固定標籤與不含個資之摘要(不含資料值、id、使用者識別、錯誤原文、路徑); 儲存位置(opt.url、opt.db、opt.cl組成之連線設定)與其內嵌之帳密於任何輸出(含logDetail與funLog之細節)一律以***遮罩
//選項名稱與語意比照w-serv-orm、w-orm-reladb: opt.logDetail(還原細節輸出)、opt.funLog(label, safe, detail)(取代console收完整細節)
//不使用Node內建模組(util等): dist由rollup打包, gDistRollup.mjs之external只列lmdb、mingo、fs、path, 其餘內建模組未列者會被當成未解析之外部相依


//遮罩字串
const MASK = '***'

//cpMask之上限: 深度, 處理的節點與鍵合計數; 超過者以標記字串代替(不交出原參照)
const CP_DEPTH = 6
const CP_MAX = 2000

//遮罩用之儲存位置字串最短長度(各回顯形式分別判定): 過短者(如'.'、'db')遮罩會吞掉大量無關文字; 其完整儲存路徑(url/db/cl與其絕對路徑)恆夠長, 仍被遮罩
const SECRET_MIN = 4

//錯誤類別名稱的白名單樣式: 只放行建構子名稱(程式識別字), 其餘(可能夾帶資料值之文字)不進預設輸出
const RE_ERR_TYPE = /^[A-Za-z][A-Za-z0-9_]{0,39}$/

//錯誤代碼的白名單樣式: 系統與資料庫慣用之機器代碼(ENOENT、EAI_AGAIN、E2BIG、ERR_xxx、MDB_xxx, E開頭者其後只含大寫字母與底線, 不放行E加數字之編號如證號); 整數另行判斷
const RE_ERR_CODE = /^(E2BIG|E[A-Z]{2,15}(?:_[A-Z]{2,15})*|ERR_[A-Z0-9_]{1,40}|MDB_[A-Z0-9_]{1,40})$/

//連線字串內嵌之帳密(scheme://帳密@主機), 不依賴已知秘密亦遮罩; 帳密部分含未編碼之@時取至最後一個@(至空白或/為止);
//帳密中的引號只在其後不是JSON/文字分隔符(, : } ])時視為帳密的一部分, 否則視為字串結尾之引號而停止(緊湊JSON中的 "url":"https://x","mail":"a@b" 不被誤吞; 引號後接空白或結尾者本就無法再接@)
const RE_URL_CRED = /([A-Za-z][A-Za-z0-9+.-]*:\/\/)(?:[^\s/'"]|['"](?![,:}\]]))*@/g

//Node檔案系統錯誤訊息之路徑尾段(`, mkdir '路徑'`、`, rename '路徑' -> '路徑'`): 路徑為訊息最後一段, 取至訊息尾(路徑含換行時亦然), 不依賴已知秘密(路徑前綴、別名形式亦涵蓋)
const RE_FS_PATH = /(,\s+[A-Za-z]+\s+)'[\s\S]*$/

//Node參數錯誤訊息之Received尾段(`Received '路徑'`, 長字串被拆成多行亦然): 取至訊息尾
const RE_RECEIVED = /(\bReceived\s+)[\s\S]*$/

//訊息與堆疊框架之分界(V8的框架行恆為換行加4個空白再`at `; Node訊息自身折行的接續行只縮排2個空白)
const RE_FRAME = /\n {4}at /

//二進位資料byteLength的內建getter: 不經屬性查找, 不執行使用端自訂之byteLength
const BL_AB = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength').get
const BL_TA = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(Uint8Array.prototype), 'byteLength').get
const BL_DV = Object.getOwnPropertyDescriptor(DataView.prototype, 'byteLength').get
const BL_SAB = typeof SharedArrayBuffer !== 'undefined' ? Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, 'byteLength').get : null

//包裝物件(new String('x')等)的類別與其內建valueOf: 先以instanceof篩選(避免對一般物件逐一試探), 再以內部槽位解包, 不執行使用端程式
const BOXES = [[String, String.prototype.valueOf], [Number, Number.prototype.valueOf], [BigInt, BigInt.prototype.valueOf], [Boolean, Boolean.prototype.valueOf], [Symbol, Symbol.prototype.valueOf]]

//Error的stack於V8為自有存取器, 其getter為同一realm內共用的內建函數: 僅當描述子的getter與此相同才讀取(不執行使用端程式)
const STACK_GETTER = (() => {
    try {
        let d = Object.getOwnPropertyDescriptor(new Error(), 'stack')
        return d !== undefined && !Object.prototype.hasOwnProperty.call(d, 'value') && typeof d.get === 'function' ? d.get : null
    }
    catch (e) {
        return null
    }
})()

//maskOpenErr重建錯誤時可沿用之內建錯誤類別(沿原型鏈比對, 其餘一律用Error)
const ERR_CLASSES = [TypeError, RangeError, SyntaxError, ReferenceError, EvalError, URIError]
if (typeof AggregateError !== 'undefined') {
    ERR_CLASSES.push(AggregateError)
}

//maskOpenErr可原樣視為「已處理」之自有屬性(其餘自有屬性, 含cause、errors、自訂欄位, 一律不沿用)
const OPEN_ERR_KEYS = ['stack', 'message', 'code', 'errno', 'syscall', 'path', 'dest']


function getOptLog(opt) {
    //診斷輸出的設定, 供WOrmLmdb建構時使用, 回傳固定快照{ logDetail, funLog }, 其後各路徑共用此快照, 不再讀取使用端的opt
    //型別錯誤者忽略並用預設(安全輸出), 讀取opt拋錯者亦同, 與本套件其他選項(useCache、autoGenPk等型別錯誤即用預設)一致; 各選項分別讀取, 單一getter拋錯只丟該選項
    //logDetail: 須為true才生效, 預設false; true時console輸出還原為含資料值與錯誤原文的完整細節(儲存位置仍遮罩), 供除錯
    //funLog: 須為函數才生效, 預設無; 給予時診斷輸出只交給它(不寫console), detail為完整細節(儲存位置仍遮罩), 供使用端遮罩或轉送
    let logDetail = false
    try {
        logDetail = opt.logDetail === true
    }
    catch (e) {
        //忽略
    }
    let funLog = null
    try {
        let f = opt.funLog
        funLog = typeof f === 'function' ? f : null
    }
    catch (e) {
        //忽略
    }
    return { logDetail, funLog }
}


function escInspect(s, quote) {
    //字串於Node錯誤訊息(util.inspect)中之回顯形式: 反斜線加倍、控制字元(含C1)以跳脫表示(如NUL為\x00)
    //util.inspect於字串含單引號且不含雙引號時改用雙引號而不跳脫單引號, 故單引號有跳脫(quote為true)與不跳脫兩種形式
    let m = { '\b': '\\b', '\t': '\\t', '\n': '\\n', '\f': '\\f', '\r': '\\r' }
    let r = s.replace(/\\/g, '\\\\')
    if (quote) {
        r = r.replace(/'/g, '\\\'')
    }
    return r.replace(/[\u0000-\u001f\u007f-\u009f]/g, (c) => m[c] || `\\x${c.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0')}`)
}


function getSecrets(bases) {
    //儲存位置(連線設定)之遮罩清單: 輸入為字串清單(opt.url、url/db、url/db/cl及其正規化、絕對路徑等), 各取原樣、斜線與反斜線互換形式(Windows路徑正規化)、
    //錯誤訊息之回顯形式(反斜線加倍、控制字元跳脫、單引號有無跳脫、JSON字串形式)與URL編碼(encodeURIComponent與encodeURI, 後者為file:///網址形式);
    //各形式分別判定長度, 不足SECRET_MIN者不列; 長者在前, 使長字串先被遮罩
    let all = []
    for (let b0 of bases) {
        if (typeof b0 !== 'string') {
            continue
        }
        //Windows磁碟機代號大小寫(堆疊或錯誤訊息可能以另一種大小寫顯示同一路徑)
        let bs = /^[A-Za-z]:/.test(b0) ? [b0, (b0[0] === b0[0].toUpperCase() ? b0[0].toLowerCase() : b0[0].toUpperCase()) + b0.slice(1)] : [b0]
        for (let b of bs) {
            for (let x of [b, b.replace(/\//g, '\\'), b.replace(/\\/g, '/')]) {
                all.push(x, escInspect(x, true), escInspect(x, false), JSON.stringify(x).slice(1, -1))
                try {
                    all.push(encodeURIComponent(x), encodeURI(x))
                }
                catch (e) {
                    //孤立代理碼元無法編碼, 略過
                }
            }
        }
    }
    return all.filter((s, k) => s.length >= SECRET_MIN && all.indexOf(s) === k).sort((a, b) => b.length - a.length)
}


function getNums(secrets) {
    //秘密中純數字且至少4位者: 數值型別之同值亦須遮罩; 過短者遮罩會吞掉大量無關數值, 不納入
    return new Set(secrets.filter((s) => /^[0-9]{4,}$/.test(s)))
}


function maskText(s, secrets = []) {
    //字串中之儲存位置以***取代: 已知秘密(getSecrets)、scheme://帳密@(帳密含@時取至最後一個@)
    if (typeof s !== 'string') {
        return s
    }
    let r = s
    for (let sec of secrets) {
        if (r.indexOf(sec) >= 0) {
            r = r.split(sec).join(MASK)
        }
    }
    return r.replace(RE_URL_CRED, `$1${MASK}@`)
}


function maskPathText(s, secrets = []) {
    //建構時open()失敗之錯誤文字的遮罩: 已知儲存位置形式(maskText), 再補Node檔案系統訊息之路徑尾段與參數錯誤之Received尾段(路徑前綴、別名形式、含換行之路徑亦涵蓋);
    //訊息(可多行)與堆疊框架分開處理: 尾段取至訊息尾, 不吞後方之框架行
    if (typeof s !== 'string') {
        return s
    }
    let m = maskText(s, secrets) //已知儲存位置先對全文遮罩(位置若恰跨過訊息與框架分界亦不被切開), 再做結構遮罩
    let i = m.search(RE_FRAME)
    let head = i < 0 ? m : m.slice(0, i)
    let tail = i < 0 ? '' : m.slice(i)
    return head.replace(RE_FS_PATH, `$1'${MASK}'`).replace(RE_RECEIVED, `$1${MASK}`) + tail
}


function ownData(o, k) {
    //取o自有資料屬性k之值, 不執行getter; 非資料屬性或無則undefined
    try {
        if (o === null || (typeof o !== 'object' && typeof o !== 'function')) {
            return undefined
        }
        let d = Object.getOwnPropertyDescriptor(o, k)
        return d !== undefined && Object.prototype.hasOwnProperty.call(d, 'value') ? d.value : undefined
    }
    catch (e) {
        return undefined
    }
}


function hasAccessor(o, k) {
    //o之原型鏈上最先出現的屬性k是否為存取器(讀取它會執行程式)
    for (let x = o; x !== null; x = Object.getPrototypeOf(x)) {
        let d = Object.getOwnPropertyDescriptor(x, k)
        if (d !== undefined) {
            return !Object.prototype.hasOwnProperty.call(d, 'value')
        }
    }
    return false
}


function readStack(e) {
    //取Error的stack字串(只讀一次): 資料屬性直接取; 存取器僅當為同realm共用之內建getter、且message與name非存取器(V8格式化首行時會讀取它們)才讀取; 其餘不執行, 回undefined
    let d = Object.getOwnPropertyDescriptor(e, 'stack')
    if (d === undefined) {
        return undefined
    }
    if (Object.prototype.hasOwnProperty.call(d, 'value')) {
        return typeof d.value === 'string' ? d.value : undefined
    }
    if (STACK_GETTER !== null && d.get === STACK_GETTER && !hasAccessor(e, 'message') && !hasAccessor(e, 'name')) {
        let s = STACK_GETTER.call(e)
        return typeof s === 'string' ? s : undefined
    }
    return undefined
}


function maskOpenErr(err, secrets = []) {
    //建構函式open()拋出之錯誤的遮罩(回顯儲存位置者): 訊息前綴與Error型態不變, 儲存位置換成***(message、stack、path、dest);
    //錯誤本身帶的path、dest亦併入遮罩清單; 只重建需要處理者(內容有變動、帶path或dest、或帶標準欄位以外之自有屬性如cause):
    //以同型別(TypeError、AggregateError等內建類別, Node內部之NodeError子類別以其內建祖先, 其餘Error)之新Error取代, 保留code、errno、syscall, 其餘自有屬性(含cause)不沿用;
    //無需處理者與非Error值原樣回傳(字串則遮罩); 以自有訊息為字串判定跨realm之Error; 全程不執行使用端程式
    try {
        if (typeof err === 'string') {
            return maskPathText(err, secrets)
        }
        let msg = ownData(err, 'message')
        let isErrLike = err instanceof Error || (err !== null && typeof err === 'object' && typeof msg === 'string' && Object.prototype.hasOwnProperty.call(err, 'stack')) //後者為跨realm之Error
        if (!isErrLike) {
            return err
        }
        let sec = secrets
        let paths = ['path', 'dest'].map((k) => ownData(err, k)).filter((v) => typeof v === 'string')
        if (paths.length > 0) {
            sec = getSecrets(paths).concat(secrets).sort((a, b) => b.length - a.length)
        }
        let msgM = maskPathText(msg, sec)
        let stack = readStack(err)
        let stackM = maskPathText(stack, sec)
        let hasPath = ownData(err, 'path') !== undefined || ownData(err, 'dest') !== undefined
        let hasExtra = Reflect.ownKeys(err).some((k) => {
            //標準欄位以外之自有屬性, 或標準欄位為存取器(內建stack之getter除外): 其值不一定已被遮罩, 一律重建而不沿用
            let d = Object.getOwnPropertyDescriptor(err, k)
            return !OPEN_ERR_KEYS.includes(k) || (d !== undefined && !Object.prototype.hasOwnProperty.call(d, 'value') && !(k === 'stack' && STACK_GETTER !== null && d.get === STACK_GETTER))
        })
        if (msgM === msg && stackM === stack && !hasPath && !hasExtra) {
            return err
        }
        let C = Error
        for (let x = Object.getPrototypeOf(err); x !== null; x = Object.getPrototypeOf(x)) { //Node內部錯誤為TypeError等之子類別(NodeError), 沿原型鏈找; 以建構子名稱比對(跨realm之原型物件不同, 但名稱同)
            let n = ownData(ownData(x, 'constructor'), 'name')
            let c = typeof n === 'string' ? ERR_CLASSES.find((v) => v.name === n) : undefined
            if (c !== undefined) {
                C = c
                break
            }
        }
        let m = typeof msgM === 'string' ? msgM : ''
        let e = typeof AggregateError !== 'undefined' && C === AggregateError ? new AggregateError([], m) : new C(m)
        for (let k of ['code', 'errno', 'syscall']) {
            let v = ownData(err, k)
            if (v !== undefined) {
                Object.defineProperty(e, k, { value: v, writable: true, enumerable: true, configurable: true })
            }
        }
        for (let k of ['path', 'dest']) {
            if (ownData(err, k) !== undefined) {
                Object.defineProperty(e, k, { value: MASK, writable: true, enumerable: true, configurable: true })
            }
        }
        if (typeof stackM === 'string') {
            Object.defineProperty(e, 'stack', { value: stackM, writable: true, enumerable: false, configurable: true })
            let code = ownData(err, 'code')
            if (typeof code === 'string' && stackM.indexOf(` [${code}]: `) >= 0) {
                //Node內部錯誤(NodeError)之toString與stack首行帶[代碼], 重建後仍保持一致
                Object.defineProperty(e, 'toString', { value: function() {
                    return `${this.name} [${code}]: ${this.message}`
                }, writable: true, enumerable: false, configurable: true })
            }
        }
        return e
    }
    catch (e2) {
        return new Error(MASK)
    }
}


function maskKey(k, c) {
    //屬性鍵之遮罩: 字串鍵與Symbol之描述文字皆可能含儲存位置
    if (typeof k === 'string') {
        return maskText(k, c.secrets)
    }
    let d = k.description
    let m = typeof d === 'string' ? maskText(d, c.secrets) : d
    return m === d ? k : Symbol(m)
}


function maskSafe(safe, secrets, nums) {
    //safe摘要之防禦性遮罩: 摘要由src以固定欄位組成(操作別、類別名稱、機器代碼等), 其值若恰等於已知秘密(如數字路徑同時為錯誤代碼)仍以***取代
    if (safe === null || typeof safe !== 'object') {
        return safe
    }
    let r = {}
    for (let k of Object.keys(safe)) {
        let v = safe[k]
        r[k] = typeof v === 'string' ? maskText(v, secrets) : ((typeof v === 'number' || typeof v === 'bigint') && nums.has(String(v)) ? MASK : v)
    }
    return r
}


function getBinaryLen(v) {
    //二進位資料(ArrayBuffer、SharedArrayBuffer、TypedArray、DataView)的byteLength: 先以brand檢查/instanceof篩選(一般物件不拋例外), 再以內建getter取(內部槽位判定, 不執行使用端自訂之byteLength); 非二進位回null
    try {
        if (ArrayBuffer.isView(v)) {
            try {
                return BL_TA.call(v)
            }
            catch (e) {
                return BL_DV.call(v) //DataView
            }
        }
        if (v instanceof ArrayBuffer) {
            return BL_AB.call(v)
        }
        if (BL_SAB !== null && v instanceof SharedArrayBuffer) {
            return BL_SAB.call(v)
        }
    }
    catch (e) {
        //非預期之物件(如Proxy陷阱拋錯、跨realm): 視為非二進位, 以一般物件處理
    }
    return null
}


function unbox(v) {
    //包裝物件(new String('x')、new Number(1)等)解包: 先以instanceof篩選(一般物件不拋例外), 再以內建valueOf取原始值(內部槽位判定, 不執行使用端程式); 非包裝物件回null, 否則回{ v: 原始值 }
    for (let [C, f] of BOXES) {
        try {
            if (v instanceof C) {
                return { v: f.call(v) }
            }
        }
        catch (e) {
            //原型鏈偽裝而無內部槽位者: 視為非包裝物件
        }
    }
    return null
}


function cp(v, c, depth) {
    //遞迴複製並遮罩, 不執行使用端程式(不呼叫getter、實例方法、建構子), 不交出原參照; 副本原型一律為內建原型(不沿用使用端的原型: 其繼承屬性未遮罩、util.inspect.custom會在格式化時執行)
    if (typeof v === 'string') {
        return maskText(v, c.secrets)
    }
    if (typeof v === 'number' || typeof v === 'bigint') {
        return c.nums.has(String(v)) ? MASK : v
    }
    if (v === null || (typeof v !== 'object' && typeof v !== 'function')) {
        return v
    }
    if (typeof v === 'function') {
        return '[Function]'
    }
    if (depth >= CP_DEPTH || c.n >= CP_MAX) {
        return '[Truncated]'
    }
    if (c.seen.has(v)) {
        return c.seen.get(v)
    }
    c.n++
    try {
        let len = getBinaryLen(v)
        if (len !== null) {
            return `[Binary ${len} bytes]`
        }
        let box = unbox(v)
        if (box !== null) {
            return typeof box.v === 'symbol' ? '[Object]' : cp(box.v, c, depth) //包裝物件視同其原始值(字串仍須遮罩)
        }
        if (v instanceof Date) {
            return new Date(Date.prototype.getTime.call(v))
        }
        if (v instanceof Map || v instanceof Set || v instanceof WeakMap || v instanceof WeakSet || v instanceof RegExp || v instanceof Promise) {
            return '[Object]' //無法複製的內建物件
        }
        let isArr = Array.isArray(v)
        let isErr = v instanceof Error //不用Object.prototype.toString(會執行Symbol.toStringTag之getter)
        let r = isArr ? new Array(v.length) : (isErr ? new Error() : {})
        c.seen.set(v, r)
        for (let k of Reflect.ownKeys(v)) {
            if (c.n >= CP_MAX) {
                break
            }
            if (k === 'length' && isArr) {
                continue
            }
            let d = Object.getOwnPropertyDescriptor(v, k)
            if (d === undefined) {
                continue
            }
            c.n++
            let val = '[Getter/Setter]'
            if (Object.prototype.hasOwnProperty.call(d, 'value')) {
                val = cp(d.value, c, depth + 1)
            }
            else if (isErr && k === 'stack' && STACK_GETTER !== null && d.get === STACK_GETTER && !hasAccessor(v, 'message') && !hasAccessor(v, 'name')) {
                //stack於V8為內建存取器: 僅當其getter嚴格等於同realm共用之內建getter、且message與name非存取器(V8格式化首行時會讀取它們)才讀取(不執行使用端程式)
                let s = STACK_GETTER.call(v)
                val = typeof s === 'string' ? maskText(s, c.secrets) : val
            }
            let isIdx = isArr && typeof k === 'string' && /^(0|[1-9][0-9]*)$/.test(k) //陣列之索引鍵不遮罩, 其餘自有鍵(字串與Symbol)皆遮罩
            Object.defineProperty(r, isIdx ? k : maskKey(k, c), { value: val, writable: true, enumerable: d.enumerable, configurable: true })
        }
        if (isErr && !Object.prototype.hasOwnProperty.call(r, 'name')) {
            //類別名稱常在原型上(class MyError extends Error): 取原型鏈上第一個資料屬性name, 副本才保有類別名稱
            for (let x = Object.getPrototypeOf(v); x !== null; x = Object.getPrototypeOf(x)) {
                let d = Object.getOwnPropertyDescriptor(x, 'name')
                if (d !== undefined) {
                    if (Object.prototype.hasOwnProperty.call(d, 'value') && typeof d.value === 'string') {
                        Object.defineProperty(r, 'name', { value: maskText(d.value, c.secrets), writable: true, enumerable: false, configurable: true })
                    }
                    break
                }
            }
        }
        return r
    }
    catch (e) {
        return '[Uncopyable]'
    }
}


function cpMask(v, secrets = []) {
    //交給console或funLog之完整細節的遮罩拷貝: 儲存位置以***取代(值、屬性鍵、Symbol描述、同值數字), 不改動呼叫端的原物件(細節若與其後仍要使用的物件共用, 使用端原地遮罩會改變結果)
    //複製: 純物件與類別實例(複製自有屬性成普通物件, 不保留原型)、陣列(含非索引之自有鍵)、原生Error(含message、stack、cause、類別名稱name)、Date、包裝物件(視同其原始值);
    //標記字串代替: 函數、二進位資料、Map與Set等內建物件、存取器、過深或過大者
    return cp(v, { secrets, nums: getNums(secrets), n: 0, seen: new WeakMap() }, 0)
}


function noop() {
    //吞下funLog回傳之promise的reject
}


function swallow(r) {
    //funLog的回傳若為promise或thenable, 附上reject處理, 使其reject不成為unhandledRejection; 全程不使診斷輸出拋錯
    //then的兩個參數皆給noop: 非標準thenable於resolve時直接呼叫首參數亦不拋錯
    try {
        if (r !== null && (typeof r === 'object' || typeof r === 'function')) {
            let t = r.then
            if (typeof t === 'function') {
                let x = t.call(r, noop, noop)
                if (x !== null && (typeof x === 'object' || typeof x === 'function') && typeof x.then === 'function') {
                    x.then(noop, noop)
                }
            }
        }
    }
    catch (e) {
        //放棄
    }
}


function getDiag(optLog, secrets = []) {
    //回傳診斷輸出函數diag(label, safe, getDetail, bare), 所有診斷輸出皆經此(R-DIAG-01)
    //label: 固定標籤; safe: 不含資料值、id、錯誤原文與路徑的摘要(預設輸出), 無則undefined; getDetail: 回傳完整細節的函數(只在有消費者時才呼叫), 無細節則undefined
    //bare: 為true者, logDetail還原時只輸出細節而不加label(舊版該站即為console.log(細節))
    //輸出不得使呼叫端的流程拋錯: 細節的取得、摘要的遮罩、console與funLog的拋錯一律吞下; funLog回傳的promise之reject亦吞下
    let { logDetail, funLog } = getOptLog(optLog) //optLog為getOptLog之快照; 不論傳入何值皆經getOptLog正規化
    let nums = getNums(secrets)
    return (label, safeRaw, getDetail, bare = false) => {

        //safe, 摘要(防禦性遮罩後)
        let safe
        try {
            safe = maskSafe(safeRaw, secrets, nums)
        }
        catch (e) {
            safe = undefined
        }

        //getArgs, 完整細節(儲存位置遮罩後); 僅在需要時取得
        let getArgs = () => {
            if (getDetail === undefined) {
                return []
            }
            try {
                return [cpMask(getDetail(), secrets)]
            }
            catch (e) {
                return ['(unprintable)']
            }
        }

        //funLog, 取代console輸出
        if (funLog !== null) {
            try {
                swallow(funLog(label, safe, ...getArgs()))
            }
            catch (e) {
                //放棄輸出
            }
            return
        }

        //console, 預設為安全摘要; logDetail為true時還原完整細節
        try {
            if (logDetail) {
                console.log(...(bare ? [] : [label]), ...getArgs())
            }
            else if (safe === undefined) {
                console.log(label)
            }
            else {
                console.log(label, safe)
            }
        }
        catch (e) {
            try {
                console.log(label, '(unprintable)')
            }
            catch (e2) {
                //放棄輸出
            }
        }
    }
}


function isErrCode(c) {
    //錯誤代碼是否為機器代碼: 絕對值至99999之整數, 或ENOENT類、ERR_開頭、MDB_開頭之字串
    if (typeof c === 'number') {
        return Number.isInteger(c) && Math.abs(c) <= 99999
    }
    return typeof c === 'string' && RE_ERR_CODE.test(c)
}


function getBriefErr(e) {
    //錯誤(訂閱函數拋出的值)的摘要: 只列類別與代碼, 不含訊息文字; 全程不執行getter與使用端程式
    //字串列為string; Error取其建構子名稱(程式識別字, 非資料), 不合識別字樣式者列為Error; 代碼只取Error之自有資料屬性code(非Error物件如資料列不取), 限機器代碼樣式
    try {
        if (typeof e === 'string') {
            return { errType: 'string' }
        }
        if (e === null || (typeof e !== 'object' && typeof e !== 'function')) {
            return { errType: e === null ? 'null' : typeof e }
        }
        let r = { errType: typeof e === 'function' ? 'function' : 'object' }
        if (e instanceof Error) { //不用Object.prototype.toString(會執行Symbol.toStringTag之getter)
            let cname
            for (let x = e; x !== null && cname === undefined; x = Object.getPrototypeOf(x)) {
                let ctor = ownData(x, 'constructor')
                if (typeof ctor === 'function') {
                    cname = ownData(ctor, 'name')
                    break
                }
            }
            r.errType = typeof cname === 'string' && RE_ERR_TYPE.test(cname) ? cname : 'Error'
            let c = ownData(e, 'code')
            if (isErrCode(c)) {
                r.errCode = c
            }
        }
        return r
    }
    catch (e2) {
        return { errType: 'unknown' }
    }
}


export { getOptLog, getSecrets, maskText, maskPathText, maskOpenErr, cpMask, getDiag, getBriefErr }
