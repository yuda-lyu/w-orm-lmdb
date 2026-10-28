import assert from 'assert'
import util from 'util'
import vm from 'vm'
import { getOptLog, getSecrets, maskText, maskPathText, maskOpenErr, cpMask, getDiag, getBriefErr } from '../src/getDiag.mjs'


//rulebook R-DIAG-01: 診斷輸出單一出口(src/getDiag.mjs)之純函數測試, 不開資料庫
//對應: 預設只輸出標籤與不含個資之摘要(R-DIAG-01-1)、logDetail還原細節且儲存位置遮罩(-2)、funLog收完整細節(-3)、輸出不得使呼叫端拋錯(-4)、建構時open失敗之訊息遮罩(-5)


let ID = 'u-secret-001@example.com'
let PW = 'S3cretPw'
let P_REL = `./data/svc-${PW}@host`
let P_ABS = `C:\\work\\data\\svc-${PW}@host`
let SEC = getSecrets([P_REL, `${P_REL}/worm/test`, P_ABS, `${P_ABS}\\worm\\test`])


//cap, 擷取console各方法之輸出
function cap(fn) {
    let lines = []
    let ks = ['log', 'info', 'warn', 'error', 'debug']
    let olds = ks.map((k) => console[k])
    ks.forEach((k) => {
        console[k] = (...a) => lines.push(a.map((v) => (typeof v === 'string' ? v : JSON.stringify(v, (kk, vv) => (typeof vv === 'bigint' ? String(vv) : vv)))).join(' '))
    })
    try {
        fn()
    }
    finally {
        ks.forEach((k, i) => {
            console[k] = olds[i]
        })
    }
    return lines
}


describe('getOptLog', function() {

    it('R-DIAG-01-3 未給或型別錯誤者忽略並用預設(與本套件其他選項型別錯誤之處理一致: 不拋錯)', function() {
        for (let o of [undefined, null, {}, 1, 'x', { logDetail: 'true' }, { logDetail: 1 }, { logDetail: {} }, { logDetail: false }, { funLog: 'x' }, { funLog: {} }, { funLog: null }]) {
            assert.deepStrictEqual(getOptLog(o), { logDetail: false, funLog: null }, JSON.stringify(o))
        }
    })

    it('R-DIAG-01-3 logDetail為true、funLog為函數才生效', function() {
        let f = () => {}
        assert.deepStrictEqual(getOptLog({ logDetail: true, funLog: f }), { logDetail: true, funLog: f })
        assert.deepStrictEqual(getOptLog({ logDetail: true }), { logDetail: true, funLog: null })
        assert.deepStrictEqual(getOptLog({ funLog: f }), { logDetail: false, funLog: f })
    })

    it('R-DIAG-01-3 讀取選項拋錯者忽略該選項, 不連累另一個有效選項', function() {
        let f = () => {}
        let o1 = {
            get logDetail() {
                throw new Error('x')
            },
            funLog: f
        }
        assert.deepStrictEqual(getOptLog(o1), { logDetail: false, funLog: f })
        let o2 = {
            logDetail: true,
            get funLog() {
                throw new Error('x')
            }
        }
        assert.deepStrictEqual(getOptLog(o2), { logDetail: true, funLog: null })
    })

})


describe('getDiag', function() {

    it('R-DIAG-01-1 預設只輸出標籤與摘要, 不呼叫getDetail(細節在預設路徑不被取得)', function() {
        let n = 0
        let diag = getDiag(getOptLog({}))
        let lines = cap(() => {
            diag('L1', { mode: 'insert', errType: 'Error' }, () => {
                n++
                return `detail ${ID}`
            })
            diag('L2', undefined, () => {
                n++
                return ID
            })
            diag('L3')
        })
        assert.deepStrictEqual(lines, ['L1 {"mode":"insert","errType":"Error"}', 'L2', 'L3'])
        assert.strictEqual(n, 0)
    })

    it('R-DIAG-01-2 logDetail為true時還原細節: 標籤加細節; bare者只輸出細節(與舊版該站之console.log(細節)相同)', function() {
        let diag = getDiag(getOptLog({ logDetail: true }))
        let lines = cap(() => {
            diag('L1', { a: 1 }, () => `detail ${ID}`)
            diag('L2', { a: 1 }, () => `bare ${ID}`, true)
            diag('L3', undefined)
        })
        assert.deepStrictEqual(lines, [`L1 detail ${ID}`, `bare ${ID}`, 'L3'])
    })

    it('R-DIAG-01-3 funLog收(label, safe, detail), 不寫console; 無細節者只收兩個引數', function() {
        let got = []
        let diag = getDiag(getOptLog({ funLog: (...a) => got.push(a) }))
        let lines = cap(() => {
            diag('L1', { a: 1 }, () => ({ id: ID }))
            diag('L2', undefined)
        })
        assert.deepStrictEqual(lines, [])
        assert.strictEqual(got.length, 2)
        assert.deepStrictEqual(got[0], ['L1', { a: 1 }, { id: ID }])
        assert.strictEqual(got[1].length, 2)
        assert.deepStrictEqual(got[1], ['L2', undefined])
    })

    it('R-DIAG-01-3 funLog存在時logDetail不另寫console(funLog取代console)', function() {
        let got = []
        let diag = getDiag(getOptLog({ logDetail: true, funLog: (...a) => got.push(a) }))
        let lines = cap(() => {
            diag('L1', { a: 1 }, () => 'd')
        })
        assert.deepStrictEqual(lines, [])
        assert.deepStrictEqual(got, [['L1', { a: 1 }, 'd']])
    })

    it('R-DIAG-01-1 摘要(safe)亦防禦性遮罩: 值含儲存位置或其同值數字者以***取代, 不含者原樣', function() {
        let got = []
        let sec = getSecrets(['./12345', '12345'])
        let diag = getDiag(getOptLog({ funLog: (...a) => got.push(a) }), sec)
        diag('L', { mode: './12345', errType: 'Error', errCode: 12345, other: 7 })
        diag('L', { mode: 'insert', errCode: 'ENOENT' })
        assert.deepStrictEqual(got[0][1], { mode: '***', errType: 'Error', errCode: '***', other: 7 })
        assert.deepStrictEqual(got[1][1], { mode: 'insert', errCode: 'ENOENT' })
        let lines = cap(() => {
            getDiag(getOptLog({}), sec)('L', { mode: './12345' })
        })
        assert.deepStrictEqual(lines, ['L {"mode":"***"}'])
    })

    it('R-DIAG-01-4 輸出不得使呼叫端拋錯: funLog拋錯、getDetail拋錯、console拋錯皆吞下', function() {
        let diag1 = getDiag(getOptLog({
            funLog: () => {
                throw new Error('x')
            }
        }))
        diag1('L', { a: 1 }, () => 'd')

        let got = []
        let diag2 = getDiag(getOptLog({ logDetail: true }))
        let lines = cap(() => {
            diag2('L', { a: 1 }, () => {
                throw new Error('boom')
            })
        })
        got.push(...lines)
        assert.deepStrictEqual(got, ['L (unprintable)'])

        let diag3 = getDiag(getOptLog({ funLog: (l, s, d) => got.push(d) }))
        diag3('L', undefined, () => {
            throw new Error('boom')
        })
        assert.strictEqual(got[got.length - 1], '(unprintable)')

        let diag4 = getDiag(getOptLog({}))
        let old = console.log
        console.log = () => {
            throw new Error('console broken')
        }
        try {
            diag4('L', { a: 1 })
        }
        finally {
            console.log = old
        }
    })

    it('R-DIAG-01-4 funLog回傳reject之promise不成為unhandledRejection', async function() {
        let bad = []
        let fn = (e) => bad.push(e)
        process.on('unhandledRejection', fn)
        try {
            let diag = getDiag(getOptLog({ funLog: () => Promise.reject(new Error('rej')) }))
            diag('L', { a: 1 }, () => 'd')
            let diag2 = getDiag(getOptLog({ funLog: () => ({ then: (a, b) => Promise.reject(new Error('rej2')) }) }))
            diag2('L', { a: 1 }, () => 'd')
            await new Promise((resolve) => setTimeout(resolve, 50))
        }
        finally {
            process.off('unhandledRejection', fn)
        }
        assert.deepStrictEqual(bad.map((v) => v.message), [])
    })

    it('R-DIAG-01-4 console第一次拋錯: 退而印「label (unprintable)」; 再失敗才放棄', function() {
        let diag = getDiag(getOptLog({}))
        let old = console.log
        let calls = []
        console.log = (...a) => {
            calls.push(a)
            if (calls.length === 1) {
                throw new Error('first call broken')
            }
        }
        try {
            diag('L', { a: 1 })
        }
        finally {
            console.log = old
        }
        assert.deepStrictEqual(calls, [['L', { a: 1 }], ['L', '(unprintable)']])
    })

    it('R-DIAG-01-4 匯出函數之邊界: optLog不是快照(null、原始值)不拋錯並用預設; 摘要帶拋錯getter不使diag拋錯; 非標準thenable於稍後resolve時亦不拋出未捕捉例外', async function() {
        for (let o of [null, undefined, 1, 'x', {}]) {
            let lines = cap(() => {
                getDiag(o)('L', { a: 1 })
            })
            assert.deepStrictEqual(lines, ['L {"a":1}'])
        }
        let got = []
        let diag = getDiag(getOptLog({ funLog: (...a) => got.push(a) }))
        diag('L', {
            get x() {
                throw new Error('safe getter boom')
            }
        })
        assert.strictEqual(got.length, 1)
        assert.strictEqual(got[0][1], undefined, '摘要無法安全取得時視為無摘要')
        let bad = []
        let fn = (e) => bad.push(e)
        process.on('uncaughtException', fn)
        try {
            let diag2 = getDiag(getOptLog({
                funLog: () => ({
                    then(res) {
                        setTimeout(() => res(1), 5) //稍後呼叫首參數: 若傳入undefined將在計時器內拋出未捕捉例外
                    }
                })
            }))
            diag2('L', { a: 1 })
            await new Promise((resolve) => setTimeout(resolve, 50))
        }
        finally {
            process.off('uncaughtException', fn)
        }
        assert.deepStrictEqual(bad.map((v) => v.message), [])
    })

    it('R-DIAG-01-2 儲存位置遮罩: 細節(console與funLog)中之儲存路徑、其絕對路徑一律以***取代, 其餘細節(id等)保留', function() {
        let err = new Error(`listener ${ID} at ${P_ABS}\\worm\\test and ${P_REL}`)
        err.code = 'ERR_X'
        err.extra = { nested: [P_ABS, { deep: `x${P_REL}y` }] }

        let got = []
        let diag1 = getDiag(getOptLog({ funLog: (...a) => got.push(a) }), SEC)
        diag1('L', { a: 1 }, () => ({ state: 'error', msg: err }))
        let d = got[0][2]
        let s = JSON.stringify(d) + String(d.msg.message) + String(d.msg.stack)
        assert.ok(s.indexOf(PW) < 0, '儲存位置未遮罩')
        assert.ok(d.msg.message.indexOf(ID) >= 0, '非儲存位置之細節被誤遮')
        assert.strictEqual(d.msg.message, `listener ${ID} at *** and ***`)
        assert.strictEqual(d.msg.code, 'ERR_X')
        assert.ok(d.msg instanceof Error && d.msg !== err)
        assert.strictEqual(err.message.indexOf('***'), -1, '不得改動呼叫端之原物件')
        assert.ok(err.message.indexOf(PW) >= 0)

        let diag2 = getDiag(getOptLog({ logDetail: true }), SEC)
        let lines = cap(() => {
            diag2('L', { a: 1 }, () => ({ state: 'error', msg: err }))
            diag2('L', { a: 1 }, () => `path ${P_REL}`, true)
        })
        let all = lines.join('\n')
        assert.ok(all.indexOf(PW) < 0, `console輸出含儲存位置: ${all.slice(0, 300)}`)
        assert.ok(all.indexOf('***') >= 0)
    })

})


describe('getBriefErr', function() {

    it('R-DIAG-01-1 只列類別與機器代碼, 不含訊息文字', function() {
        let e = new Error(`failed for ${ID} ${PW}`)
        assert.deepStrictEqual(getBriefErr(e), { errType: 'Error' })
        class MyListenerError extends Error {}
        let e2 = new MyListenerError(`dup ${ID}`)
        e2.code = 'MDB_MAP_FULL'
        assert.deepStrictEqual(getBriefErr(e2), { errType: 'MyListenerError', errCode: 'MDB_MAP_FULL' })
        let e3 = new TypeError('x')
        e3.code = 'ERR_INVALID_ARG_VALUE'
        assert.deepStrictEqual(getBriefErr(e3), { errType: 'TypeError', errCode: 'ERR_INVALID_ARG_VALUE' })
    })

    it('R-DIAG-01-1 字串或非物件之拋出值只列型別(字串內容可能夾帶資料)', function() {
        assert.deepStrictEqual(getBriefErr(`fail ${ID}`), { errType: 'string' })
        assert.deepStrictEqual(getBriefErr(null), { errType: 'null' })
        assert.deepStrictEqual(getBriefErr(undefined), { errType: 'undefined' })
        assert.deepStrictEqual(getBriefErr(12), { errType: 'number' })
        assert.deepStrictEqual(getBriefErr({ message: ID }), { errType: 'object' })
        assert.deepStrictEqual(getBriefErr(() => {}), { errType: 'function' })
    })

    it('R-DIAG-01-1 code須為機器代碼樣式, 資料值樣式者不列(含過大整數、一般字串、電郵、E開頭加數字之編號如證號)', function() {
        for (let c of [ID, 'alice', 'U-0001', 99999999, 100000, -100000, 1.5, NaN, {}, [], 'E', 'ERR_', 'mdb_busy', '', 'a'.repeat(60), 'E123456789', 'E12345678', 'EMP00123', 'E1', 'EA', 'ENO ENT']) {
            let e = new Error('x')
            e.code = c
            assert.deepStrictEqual(getBriefErr(e), { errType: 'Error' }, String(c))
        }
        for (let c of ['ENOENT', 'EAI_AGAIN', 'E2BIG', 'ERR_INVALID_ARG_TYPE', 'MDB_MAP_FULL', 18, 0, -1, -4058, -30798, 99999, -99999]) {
            let e = new Error('x')
            e.code = c
            assert.deepStrictEqual(getBriefErr(e), { errType: 'Error', errCode: c }, String(c))
        }
    })

    it('R-DIAG-01-1 只有Error取code: 非Error物件(如被誤拋之資料列)之code不列', function() {
        assert.deepStrictEqual(getBriefErr({ id: ID, code: 48291 }), { errType: 'object' })
        assert.deepStrictEqual(getBriefErr({ id: ID, code: 'E123456789' }), { errType: 'object' })
        assert.deepStrictEqual(getBriefErr({ code: 'ENOENT' }), { errType: 'object' })
        let f = () => {}
        f.code = 'ENOENT'
        assert.deepStrictEqual(getBriefErr(f), { errType: 'function' })
    })

    it('R-DIAG-01-1 建構子名稱至多40字元(識別字樣式), 超過者列為Error', function() {
        function Ok() {}
        Object.defineProperty(Ok, 'name', { value: 'A'.repeat(40) })
        function TooLong() {}
        Object.defineProperty(TooLong, 'name', { value: 'A'.repeat(41) })
        let e = new Error('x')
        Object.defineProperty(e, 'constructor', { value: Ok, configurable: true })
        assert.deepStrictEqual(getBriefErr(e), { errType: 'A'.repeat(40) })
        Object.defineProperty(e, 'constructor', { value: TooLong, configurable: true })
        assert.deepStrictEqual(getBriefErr(e), { errType: 'Error' })
    })

    it('R-DIAG-01-1 建構子名稱不合識別字樣式者列為Error; 不執行getter與使用端程式', function() {
        let e = new Error('x')
        function Weird() {}
        Object.defineProperty(Weird, 'name', { value: `bad name ${ID}` })
        Object.defineProperty(e, 'constructor', { value: Weird, configurable: true })
        assert.deepStrictEqual(getBriefErr(e), { errType: 'Error' })
        function Okay() {}
        Object.defineProperty(e, 'constructor', { value: Okay, configurable: true })
        assert.deepStrictEqual(getBriefErr(e), { errType: 'Okay' })

        let called = 0
        let e2 = new Error('x')
        Object.defineProperty(e2, 'code', {
            get() {
                called++
                return 'ENOENT'
            },
            configurable: true
        })
        assert.deepStrictEqual(getBriefErr(e2), { errType: 'Error' })
        assert.strictEqual(called, 0)
    })

    it('R-DIAG-01-4 Proxy與陷阱拋錯之值不使其拋錯', function() {
        let p = new Proxy({}, {
            getOwnPropertyDescriptor() {
                throw new Error('trap')
            },
            getPrototypeOf() {
                throw new Error('trap')
            }
        })
        let r = getBriefErr(p)
        assert.ok(r !== null && typeof r === 'object' && typeof r.errType === 'string')
        assert.ok(JSON.stringify(r).indexOf('trap') < 0)
    })

})


describe('getSecrets 與 maskText', function() {

    it('R-DIAG-01-2 getSecrets: 略過非字串、空值與過短者(各回顯形式分別判定長度); 含原樣、斜線互換、反斜線加倍、URL編碼形式; 長者在前', function() {
        assert.deepStrictEqual(getSecrets([]), [])
        assert.deepStrictEqual(getSecrets(['', null, undefined, 5, '.', 'db', '_db']), [])
        assert.ok(getSecrets(['./_db']).includes('./_db'))
        let ss = getSecrets([P_ABS])
        for (let v of [P_ABS, 'C:/work/data/svc-S3cretPw@host', 'C:\\\\work\\\\data\\\\svc-S3cretPw@host', encodeURIComponent(P_ABS)]) {
            assert.ok(ss.includes(v), v)
        }
        let rel = getSecrets(['./data/x'])
        assert.ok(rel.includes('.\\data\\x') && rel.includes('.\\\\data\\\\x'), JSON.stringify(rel))
        for (let k = 1; k < ss.length; k++) {
            assert.ok(ss[k - 1].length >= ss[k].length)
        }
        assert.strictEqual(new Set(ss).size, ss.length, '不得重複')
    })

    it('R-DIAG-01-2 getSecrets: 含控制字元(NUL、\\t、\\n、\\r、C1)之路徑, 其錯誤訊息回顯形式(\\x00、\\t、\\n、\\r、\\x85)亦列入; 轉碼後夠長者即使原樣過短亦列入', function() {
        let ss = getSecrets(['C:\\work\u0000x', 'C:\\a\tb\nc\rd\u0085e', 'a\u0000b'])
        assert.ok(ss.includes('C:\\\\work\\x00x'), JSON.stringify(ss))
        assert.ok(ss.includes('C:\\\\a\\tb\\nc\\rd\\x85e'), JSON.stringify(ss))
        assert.ok(ss.includes('a\\x00b') && !ss.includes('a\u0000b'), JSON.stringify(ss))
        assert.strictEqual(maskText('Received \'C:\\\\work\\x00x\'', ss), 'Received \'***\'')
    })

    it('R-DIAG-01-2 getSecrets: 單引號有跳脫(\\\')與未跳脫(util.inspect改用雙引號)兩種形式、JSON字串形式、encodeURI(file:///網址)形式皆列入', function() {
        let base = 'C:\\it\'s\\svc-S3cretPw\u0000'
        let ss = getSecrets([base])
        assert.ok(ss.includes('C:\\\\it\\\'s\\\\svc-S3cretPw\\x00'), '單引號跳脫形式')
        assert.ok(ss.includes('C:\\\\it\'s\\\\svc-S3cretPw\\x00'), '單引號未跳脫形式(雙引號包覆)')
        assert.ok(ss.includes('C:\\\\it\'s\\\\svc-S3cretPw\\u0000'), 'JSON形式')
        assert.strictEqual(maskText('"C:\\\\it\'s\\\\svc-S3cretPw\\x00"', ss), '"***"')
        assert.strictEqual(maskText('"C:\\\\it\'s\\\\svc-S3cretPw\\u0000"', ss), '"***"')
        let ss2 = getSecrets(['C:\\開源 data\\x@host'])
        assert.ok(ss2.includes(encodeURI('C:/開源 data/x@host')), JSON.stringify(ss2))
        assert.strictEqual(maskText('at fn (file:///C:/%E9%96%8B%E6%BA%90%20data/x@host/a.mjs:1:2)', ss2), 'at fn (file:///***/a.mjs:1:2)')
    })

    it('R-DIAG-01-2 getSecrets: 孤立代理碼元無法URL編碼者略過該形式而不拋錯', function() {
        let ss = getSecrets(['C:\\work\\bad\ud800name'])
        assert.ok(ss.includes('C:\\work\\bad\ud800name'))
    })

    it('R-DIAG-01-2 maskText: 已知儲存位置全部出現處皆遮罩, 其餘文字與非字串原樣', function() {
        assert.strictEqual(maskText(`a ${P_REL} b ${P_ABS} c ${P_REL}`, SEC), 'a *** b *** c ***')
        assert.strictEqual(maskText('plain text no secret', SEC), 'plain text no secret')
        assert.strictEqual(maskText(12, SEC), 12)
        assert.strictEqual(maskText(null, SEC), null)
    })

    it('R-DIAG-01-2 maskText: scheme://帳密@之帳密亦遮罩(不依賴已知秘密); 帳密含@時取至最後一個@; 不吞掉其後以空白分隔之文字', function() {
        assert.strictEqual(maskText('connect postgres://who:pw@host:5432/db now', []), 'connect postgres://***@host:5432/db now')
        assert.strictEqual(maskText('connect postgres://alice:secret@part@host/db', []), 'connect postgres://***@host/db')
        assert.strictEqual(maskText('see http://example.com:80 and mail me@x.com', []), 'see http://example.com:80 and mail me@x.com')
    })

    it('R-DIAG-01-2 maskText: scheme://帳密@之密碼含單引號、雙引號時亦遮罩; 緊湊JSON中之網址與其後欄位的@不被誤吞', function() {
        assert.strictEqual(maskText('postgres://admin:p\'S3cretPw@db.host/app', []), 'postgres://***@db.host/app')
        assert.strictEqual(maskText('postgres://admin:p"S3cretPw@db.host/app', []), 'postgres://***@db.host/app')
        let json = '{"url":"https://example.com","contact":"admin@test.com"}'
        assert.strictEqual(maskText(json, []), json)
        let arr = '["http://a.com","b@c.d"] [\'http://a.com\',\'b@c.d\']'
        assert.strictEqual(maskText(arr, []), arr)
        for (let s of ['["http://a.com"]x@y.z', '{"u":"http://a.com"}x@y.z', '{"u":"http://a.com":x@y.z', '"http://a.com",x@y.z']) {
            assert.strictEqual(maskText(s, []), s, s)
        }
    })

    it('R-DIAG-01-2 getSecrets: Windows磁碟機代號大小寫互換之形式亦列入(堆疊或錯誤訊息可能以另一種大小寫顯示同一路徑)', function() {
        let ss = getSecrets(['C:\\secret\\db'])
        assert.strictEqual(maskText('at c:\\secret\\db\\x and C:\\secret\\db and c:/secret/db', ss), 'at ***\\x and *** and ***')
        let ss2 = getSecrets(['d:/Secret Dir/x@host'])
        assert.strictEqual(maskText('file:///D:/Secret%20Dir/x@host/a.mjs', ss2), 'file:///***/a.mjs')
        assert.deepStrictEqual(getSecrets(['./relative/path']).filter((v) => v.startsWith('C:')), [])
    })

})


describe('maskPathText 與 maskOpenErr', function() {

    it('R-DIAG-01-5 maskPathText: Node檔案系統訊息之路徑尾段(任何路徑, 含前綴與別名形式)與Received尾段皆遮罩, 前綴文字保留', function() {
        assert.strictEqual(maskPathText('ENOTDIR: not a directory, mkdir \'C:\\other\\prefix\\x\'', []), 'ENOTDIR: not a directory, mkdir \'***\'')
        assert.strictEqual(maskPathText('ENOENT: no such file or directory, rename \'C:\\a\\b\' -> \'C:\\c\\d\'', []), 'ENOENT: no such file or directory, rename \'***\'')
        assert.strictEqual(maskPathText('The argument \'path\' must be a string, Uint8Array, or URL without null bytes. Received \'C:\\\\x\\x00y\'', []), 'The argument \'path\' must be a string, Uint8Array, or URL without null bytes. Received ***')
        assert.strictEqual(maskPathText('no path here', SEC), 'no path here')
        assert.strictEqual(maskPathText(5, SEC), 5)
    })

    it('R-DIAG-01-5 maskPathText: 多行文字(stack)只遮罩訊息行之路徑尾段, 不動堆疊框架行', function() {
        let s = 'Error: ENOENT: no such file or directory, mkdir \'C:\\x\'\n    at mkdirSync (node:fs:1:1)\n    at file:///C:/repo/node_modules/lmdb/open.js:10:5'
        assert.strictEqual(maskPathText(s, []), 'Error: ENOENT: no such file or directory, mkdir \'***\'\n    at mkdirSync (node:fs:1:1)\n    at file:///C:/repo/node_modules/lmdb/open.js:10:5')
    })

    it('R-DIAG-01-5 maskPathText: 路徑或Received尾段含換行(長字串被拆成多行)時整段遮罩, 框架行不受影響', function() {
        let s1 = 'The argument \'path\' must be a string, Uint8Array, or URL without null bytes. Received \'user:S3cretPw@host\\x00/worm/test\' +\n  \'xxxxxxxx\\ny\''
        assert.strictEqual(maskPathText(s1, []), 'The argument \'path\' must be a string, Uint8Array, or URL without null bytes. Received ***')
        let s2 = 'ENOENT: no such file or directory, mkdir \'C:\\work\\svc-S3cretPw@host\nsecond line\' x'
        assert.strictEqual(maskPathText(s2, []), 'ENOENT: no such file or directory, mkdir \'***\'')
        let s3 = `TypeError [ERR_INVALID_ARG_VALUE]: The argument 'path' ... Received 'a\\x00b' +\n  'S3cretPw'\n    at open (node:fs:1:1)\n    at file:///C:/repo/src/WOrmLmdb.mjs:1:1`
        assert.strictEqual(maskPathText(s3, []), 'TypeError [ERR_INVALID_ARG_VALUE]: The argument \'path\' ... Received ***\n    at open (node:fs:1:1)\n    at file:///C:/repo/src/WOrmLmdb.mjs:1:1')
        let s4 = `Error: x\n    at fn (${P_ABS}\\a.js:1:1)`
        assert.strictEqual(maskPathText(s4, SEC), 'Error: x\n    at fn (***\\a.js:1:1)', '堆疊框架行中之已知儲存位置亦遮罩')
    })

    function mkFsErr(p) {
        let e = new Error(`ENOENT: no such file or directory, mkdir '${p}'`)
        e.errno = -4058
        e.code = 'ENOENT'
        e.syscall = 'mkdir'
        e.path = p
        return e
    }

    it('R-DIAG-01-5 maskOpenErr: 訊息、stack、path皆遮罩, 型態(Error)與code、errno、syscall不變', function() {
        let e = mkFsErr(`${P_ABS}\\worm\\test`)
        let r = maskOpenErr(e, SEC)
        assert.notStrictEqual(r, e)
        assert.ok(r instanceof Error)
        assert.strictEqual(Object.getPrototypeOf(r), Error.prototype)
        assert.strictEqual(r.message, 'ENOENT: no such file or directory, mkdir \'***\'')
        assert.strictEqual(r.code, 'ENOENT')
        assert.strictEqual(r.errno, -4058)
        assert.strictEqual(r.syscall, 'mkdir')
        assert.strictEqual(r.path, '***')
        assert.ok(String(r.stack).indexOf(PW) < 0 && String(r.stack).indexOf('C:\\work') < 0, r.stack)
        assert.ok(String(r.stack).startsWith('Error: ENOENT: no such file or directory, mkdir \'***\''))
        assert.strictEqual(e.path, `${P_ABS}\\worm\\test`, '不得改動原物件')
    })

    it('R-DIAG-01-5 maskOpenErr: 未知路徑(不在已知清單)亦由結構遮罩; 內建錯誤類別沿原型鏈保留(Node內部錯誤為TypeError之子類別)', function() {
        class NodeError extends TypeError {}
        let e = new NodeError('The argument \'path\' must be a string, Uint8Array, or URL without null bytes. Received \'C:\\\\unknown\\x00x\'')
        e.code = 'ERR_INVALID_ARG_VALUE'
        let r = maskOpenErr(e, [])
        assert.ok(r instanceof TypeError)
        assert.strictEqual(Object.getPrototypeOf(r), TypeError.prototype)
        assert.strictEqual(r.code, 'ERR_INVALID_ARG_VALUE')
        assert.ok(r.message.endsWith('Received ***'), r.message)
        class Custom extends Error {}
        let c = mkFsErr('C:\\unknown\\x')
        Object.setPrototypeOf(c, Custom.prototype)
        let rc = maskOpenErr(c, [])
        assert.strictEqual(Object.getPrototypeOf(rc), Error.prototype)
    })

    it('R-DIAG-01-5 maskOpenErr: 不沿用其餘自有屬性(含cause), 副本不帶儲存位置', function() {
        let e = mkFsErr(`${P_ABS}\\worm\\test`)
        e.cause = new Error(`cause ${P_ABS}`)
        e.extra = P_ABS
        e.dest = P_REL
        let r = maskOpenErr(e, SEC)
        assert.strictEqual(r.cause, undefined)
        assert.strictEqual(r.extra, undefined)
        assert.strictEqual(r.dest, '***')
        let all = [r.message, String(r.stack), JSON.stringify(r), util.inspect(r)].join('\n')
        assert.ok(all.indexOf(PW) < 0, all)
    })

    it('R-DIAG-01-5 maskOpenErr: 無需遮罩者原樣回傳同一物件; 非Error值原樣, 字串則遮罩', function() {
        let e = new Error('MDB_MAP_FULL: environment mapsize limit reached')
        assert.strictEqual(maskOpenErr(e, SEC), e)
        let o = { message: P_ABS }
        assert.strictEqual(maskOpenErr(o, SEC), o)
        assert.strictEqual(maskOpenErr(undefined, SEC), undefined)
        assert.strictEqual(maskOpenErr(null, SEC), null)
        assert.strictEqual(maskOpenErr(7, SEC), 7)
        assert.strictEqual(maskOpenErr(`bad ${P_ABS}`, SEC), 'bad ***')
    })

    it('R-DIAG-01-5 maskOpenErr: 陷阱拋錯之值不使其拋錯(回固定遮罩錯誤)', function() {
        let p = new Proxy(new Error('x'), {
            getPrototypeOf() {
                throw new Error('trap')
            }
        })
        let r = maskOpenErr(p, SEC)
        assert.ok(r instanceof Error)
        assert.strictEqual(r.message, '***')
    })

    it('R-DIAG-01-5 maskOpenErr: 錯誤自帶之path、dest併入遮罩清單(訊息以非標準格式回顯儲存路徑者亦遮罩, 不依賴已知清單)', function() {
        let e = new Error(`weird format ${P_ABS}\\worm\\test end`)
        e.path = `${P_ABS}\\worm\\test`
        let r = maskOpenErr(e, [])
        assert.strictEqual(r.message, 'weird format *** end')
        assert.ok(String(r.stack).indexOf(PW) < 0)
        let e2 = new Error(`from ${P_REL} to ${P_ABS}`)
        e2.dest = P_ABS
        e2.path = P_REL
        assert.strictEqual(maskOpenErr(e2, []).message, 'from *** to ***')
    })

    it('R-DIAG-01-5 maskOpenErr: path併入遮罩清單後長者在前(較短之path不得先遮罩而留下較長已知位置之後半)', function() {
        let e = new Error(`x ${P_ABS}\\worm\\test y`)
        e.path = P_ABS
        assert.strictEqual(maskOpenErr(e, SEC).message, 'x *** y')
    })

    it('R-DIAG-01-5 maskOpenErr: 路徑本身含「換行加空白加at」時不被誤認為堆疊框架分界(訊息中的框架分界為換行加4個空白)', function() {
        let msg = 'ENOENT: no such file or directory, mkdir \'/data/run\n at /private/storage\''
        assert.strictEqual(maskPathText(msg, []), 'ENOENT: no such file or directory, mkdir \'***\'')
        let e = new Error(msg)
        e.path = '/data/run\n at /private/storage'
        e.code = 'ENOENT'
        assert.strictEqual(maskOpenErr(e, []).message, 'ENOENT: no such file or directory, mkdir \'***\'')
        let s = `${msg}\n    at mkdirSync (node:fs:1:1)`
        assert.strictEqual(maskPathText(s, []), 'ENOENT: no such file or directory, mkdir \'***\'\n    at mkdirSync (node:fs:1:1)')
    })

    it('R-DIAG-01-5 maskOpenErr: name為存取器(原型上)的Error不讀stack, 存取器不被執行', function() {
        let called = 0
        class NamedErr extends Error {
            get name() {
                called++
                return 'Named'
            }
        }
        let e = new NamedErr(`x ${P_ABS}`)
        let r = maskOpenErr(e, SEC)
        assert.strictEqual(called, 0)
        assert.strictEqual(r.message, 'x ***')
    })

    it('R-DIAG-01-5 maskOpenErr: 標準欄位以外之自有屬性(cause、errors、自訂欄位)或標準欄位為存取器者, 即使訊息未變亦重建並不沿用, 存取器不被執行', function() {
        let e = new Error('MDB_X: nothing to mask')
        e.cause = new Error(`inner ${P_ABS}`)
        let r = maskOpenErr(e, SEC)
        assert.notStrictEqual(r, e)
        assert.strictEqual(r.cause, undefined)
        assert.strictEqual(r.message, 'MDB_X: nothing to mask')
        assert.ok(util.inspect(r).indexOf(PW) < 0)

        let called = 0
        let e2 = new Error('MDB_X')
        Object.defineProperty(e2, 'path', {
            get() {
                called++
                return P_ABS
            },
            enumerable: true,
            configurable: true
        })
        Object.defineProperty(e2, 'stack', {
            get() {
                called++
                return `Error: ${P_ABS}`
            },
            configurable: true
        })
        let r2 = maskOpenErr(e2, SEC)
        assert.strictEqual(called, 0, '存取器不得被執行')
        assert.notStrictEqual(r2, e2)
        assert.ok(util.inspect(r2).indexOf(PW) < 0 && String(r2.stack).indexOf(PW) < 0)
        assert.strictEqual(r2.path, undefined)

        let e3 = new Error(`x ${P_ABS}`)
        e3.errors = [new Error(P_ABS)]
        assert.strictEqual(maskOpenErr(e3, SEC).errors, undefined)

        //message為存取器: 讀stack時V8會執行它, 故不讀stack
        let called3 = 0
        let e4 = new Error('MDB_X')
        Object.defineProperty(e4, 'message', {
            get() {
                called3++
                return P_ABS
            },
            configurable: true
        })
        let r4 = maskOpenErr(e4, SEC)
        assert.strictEqual(called3, 0, 'message存取器不得被執行')
        assert.notStrictEqual(r4, e4)
        assert.ok(util.inspect(r4).indexOf(PW) < 0)
    })

    it('R-DIAG-01-5 maskOpenErr: 跨realm之Error(instanceof Error為false)亦遮罩, 內建類別以名稱比對保留(TypeError不降成Error); AggregateError類別保留且不沿用errors', function() {
        let xr = vm.runInNewContext(`new TypeError('cross ${P_ABS.replace(/\\/g, '\\\\')}')`)
        assert.ok(!(xr instanceof Error))
        let r = maskOpenErr(xr, SEC)
        assert.strictEqual(r.message, 'cross ***')
        assert.ok(r instanceof TypeError, '跨realm之TypeError重建後仍為TypeError')
        assert.ok(String(r.stack).indexOf(PW) < 0)

        let ag = new AggregateError([new Error(P_ABS)], `agg ${P_ABS}`)
        let r2 = maskOpenErr(ag, SEC)
        assert.ok(r2 instanceof AggregateError)
        assert.strictEqual(r2.message, 'agg ***')
        assert.deepStrictEqual(r2.errors, [])
    })

    it('R-DIAG-01-5 maskOpenErr: Node內部錯誤(stack首行與toString帶[代碼])重建後仍帶[代碼], 與stack首行一致', function() {
        let e = new TypeError(`The argument 'path' must be a string. Received '${P_ABS}'`)
        e.code = 'ERR_INVALID_ARG_VALUE'
        Object.defineProperty(e, 'stack', { value: `TypeError [ERR_INVALID_ARG_VALUE]: ${e.message}\n    at x (node:fs:1:1)`, writable: true, configurable: true })
        let r = maskOpenErr(e, SEC)
        assert.strictEqual(String(r), 'TypeError [ERR_INVALID_ARG_VALUE]: The argument \'path\' must be a string. Received ***')
        assert.strictEqual(String(r.stack).split('\n')[0], String(r))
        assert.strictEqual(r.code, 'ERR_INVALID_ARG_VALUE')
        let plain = new Error(`plain ${P_ABS}`)
        assert.strictEqual(String(maskOpenErr(plain, SEC)), 'Error: plain ***')
    })

})


describe('cpMask', function() {

    it('R-DIAG-01-2 不改動原物件, 循環參照與過深/過大結構有界, 函數與二進位以標記字串代替', function() {
        let a = { name: ID, loc: P_ABS, list: [P_REL, 1, null, undefined], fn() {}, buf: Buffer.from(PW), d: new Date(0) }
        a.self = a
        let deep = { v: P_ABS }
        let cur = deep
        for (let k = 0; k < 30; k++) {
            cur.next = { v: P_ABS }
            cur = cur.next
        }
        a.deep = deep
        let r = cpMask(a, SEC)
        assert.notStrictEqual(r, a)
        assert.strictEqual(a.loc, P_ABS)
        assert.strictEqual(r.name, ID)
        assert.strictEqual(r.loc, '***')
        assert.deepStrictEqual(r.list, ['***', 1, null, undefined])
        assert.strictEqual(r.fn, '[Function]')
        assert.ok(String(r.buf).startsWith('[Binary'))
        assert.strictEqual(r.d.getTime(), 0)
        assert.notStrictEqual(r.d, a.d, 'Date亦為拷貝, 不交出原參照')
        assert.strictEqual(r.self, r)
        let n = 0
        for (let x = r.deep; typeof x === 'object' && x !== null; x = x.next) {
            n++
            assert.ok(JSON.stringify(x.v).indexOf(PW) < 0)
        }
        assert.ok(n < 30, `過深結構未截斷: ${n}`)
        assert.ok(JSON.stringify(r, (k, v) => (k === 'self' ? undefined : v)).indexOf(PW) < 0)
    })

    it('R-DIAG-01-2 過大結構有界: 節點數超過上限(2000)者其餘不複製, 已複製者皆已遮罩', function() {
        let r = cpMask({ list: new Array(5000).fill(P_ABS) }, SEC)
        let n = Object.keys(r.list).length
        assert.ok(n > 0 && n <= 2000, `複製筆數: ${n}`)
        assert.ok(r.list.every((v) => v === '***'))
    })

    it('R-DIAG-01-2 Error之副本仍為Error: message、stack、cause、自有屬性皆遮罩', function() {
        let e = new Error(`m ${P_ABS}`, { cause: new Error(`c ${P_ABS}`) })
        e.extra = `x ${P_REL}`
        let r = cpMask(e, SEC)
        assert.ok(r instanceof Error)
        assert.strictEqual(r.message, 'm ***')
        assert.ok(typeof r.stack === 'string' && r.stack.indexOf(PW) < 0 && r.stack.indexOf('m ***') >= 0)
        assert.strictEqual(r.cause.message, 'c ***')
        assert.strictEqual(r.extra, 'x ***')
        assert.strictEqual(e.message, `m ${P_ABS}`)
    })

    it('R-DIAG-01-2 屬性鍵與Symbol描述含儲存位置者亦遮罩; 數字路徑之同值數字亦遮罩(至少4位)', function() {
        let sec = getSecrets(['./12345', '12345'])
        let e = new Error('x')
        e[P_REL] = 'v'
        e[Symbol(P_REL)] = 'w'
        let r = cpMask(e, SEC)
        let keys = Reflect.ownKeys(r).map((k) => String(k))
        assert.ok(keys.every((k) => k.indexOf(PW) < 0), keys.join('|'))
        assert.ok(keys.includes('***') && keys.includes('Symbol(***)'), keys.join('|'))
        let r2 = cpMask({ n: 12345, count: 7, big: 12345n, s: '12345' }, sec)
        assert.deepStrictEqual(r2, { n: '***', count: 7, big: '***', s: '***' })
    })

    it('R-DIAG-01-2 副本不沿用原型: 繼承屬性不外帶, 繼承之util.inspect.custom不被執行; Error之name取原型鏈上之資料屬性', function() {
        class MyErr extends Error {}
        MyErr.prototype.name = 'MyErr'
        MyErr.prototype.secretInherited = P_ABS
        let called = 0
        MyErr.prototype[util.inspect.custom] = function() {
            called++
            return 'CUSTOM'
        }
        let e = new MyErr('m')
        let r = cpMask(e, SEC)
        assert.strictEqual(r.secretInherited, undefined)
        assert.strictEqual(r.name, 'MyErr')
        let s = util.inspect(r)
        assert.strictEqual(called, 0)
        assert.ok(s.indexOf('CUSTOM') < 0)
        assert.strictEqual(Object.getPrototypeOf(r), Error.prototype)
    })

    it('R-DIAG-01-4 不執行使用端程式: getter以標記字串代替且不被呼叫, Proxy與陷阱拋錯之值不使其拋錯', function() {
        let called = 0
        let o = {
            get g() {
                called++
                return P_ABS
            },
            [Symbol.toStringTag]: 'X',
        }
        Object.defineProperty(o, 'tag', {
            get() {
                called++
                return 1
            },
            enumerable: true,
            configurable: true
        })
        let r = cpMask(o, SEC)
        assert.strictEqual(called, 0)
        assert.strictEqual(r.g, '[Getter/Setter]')
        assert.strictEqual(r.tag, '[Getter/Setter]')
        let p = new Proxy({}, {
            ownKeys() {
                throw new Error('trap')
            }
        })
        assert.strictEqual(cpMask(p, SEC), '[Uncopyable]')
        let ea = new Error('x')
        Object.defineProperty(ea, 'stack', {
            get() {
                called++
                return P_ABS
            },
            configurable: true
        })
        let re = cpMask(ea, SEC)
        assert.strictEqual(called, 0)
        assert.strictEqual(re.stack, '[Getter/Setter]')
    })

    it('R-DIAG-01-2 內建物件以標記字串代替(Map、Set、RegExp、Promise), 非字串原始值原樣', function() {
        let r = cpMask({ m: new Map([[P_ABS, 1]]), s: new Set([P_ABS]), re: /x/, p: Promise.resolve(1), b: true, u: undefined, z: null, n: 1.5 }, SEC)
        assert.deepStrictEqual(r, { m: '[Object]', s: '[Object]', re: '[Object]', p: '[Object]', b: true, u: undefined, z: null, n: 1.5 })
    })

    it('R-DIAG-01-2 陣列之非索引自有鍵(字串與Symbol)亦遮罩, 索引鍵與length不變', function() {
        let a = ['x', P_ABS]
        a[P_ABS] = 'v'
        a[Symbol(P_REL)] = 'w'
        a['01'] = 'not an index'
        let r = cpMask(a, SEC)
        let keys = Reflect.ownKeys(r).map((k) => String(k))
        assert.ok(keys.every((k) => k.indexOf(PW) < 0), keys.join('|'))
        assert.deepStrictEqual(keys.sort(), ['***', '01', '0', '1', 'Symbol(***)', 'length'].sort())
        assert.strictEqual(r[1], '***')
        assert.strictEqual(r.length, 2)
    })

    it('R-DIAG-01-2 陣列之索引鍵須為標準索引(無前導零): 含秘密之非標準數字鍵仍遮罩', function() {
        let sec = getSecrets(['00012'])
        let a = ['x']
        a['00012'] = 'v'
        let keys = Reflect.ownKeys(cpMask(a, sec)).map(String)
        assert.deepStrictEqual(keys.sort(), ['***', '0', 'length'].sort())
    })

    it('R-DIAG-01-2 包裝物件(new String等)視同其原始值: 字串仍遮罩, 不逐字元外洩', function() {
        let r = cpMask({ s: new String(P_ABS), t: new String('plain'), n: new Number(7), b: new Boolean(true), g: Object(BigInt(5)), y: Object(Symbol(P_ABS)) }, SEC)
        assert.strictEqual(r.s, '***')
        assert.strictEqual(r.t, 'plain')
        assert.strictEqual(r.n, 7)
        assert.strictEqual(r.b, true)
        assert.strictEqual(r.g, 5n)
        assert.strictEqual(r.y, '[Object]')
        assert.ok(util.inspect(r).indexOf(PW) < 0)
    })

    it('R-DIAG-01-4 二進位資料以內建getter取長度: 自訂之byteLength存取器不被執行亦不外帶; ArrayBuffer、DataView、SharedArrayBuffer皆為標記字串', function() {
        let called = 0
        let u8 = new Uint8Array(3)
        Object.defineProperty(u8, 'byteLength', {
            get() {
                called++
                return P_ABS
            }
        })
        let r = cpMask({ u8, ab: new ArrayBuffer(5), dv: new DataView(new ArrayBuffer(4)), sab: new SharedArrayBuffer(8), buf: Buffer.from('abc') }, SEC)
        assert.strictEqual(called, 0)
        assert.deepStrictEqual(r, { u8: '[Binary 3 bytes]', ab: '[Binary 5 bytes]', dv: '[Binary 4 bytes]', sab: '[Binary 8 bytes]', buf: '[Binary 3 bytes]' })
    })

    it('R-DIAG-01-4 Error之message或name為存取器時不讀stack(V8格式化首行會執行它們): 存取器不被執行, 副本以標記字串代替', function() {
        let called = 0
        let e = new Error('x')
        Object.defineProperty(e, 'message', {
            get() {
                called++
                return P_ABS
            },
            configurable: true
        })
        let r = cpMask(e, SEC)
        assert.strictEqual(called, 0)
        assert.strictEqual(r.stack, '[Getter/Setter]')
        assert.strictEqual(r.message, '[Getter/Setter]')
        class NamedErr extends Error {
            get name() {
                called++
                return P_ABS
            }
        }
        let r2 = cpMask(new NamedErr('y'), SEC)
        assert.strictEqual(called, 0)
        assert.ok(JSON.stringify(r2).indexOf(PW) < 0 && String(r2.stack).indexOf(PW) < 0)
    })

    it('R-DIAG-01-2 深度上限為6: 第6層之物件為「[Truncated]」, 之內不複製; 第5層以內照常', function() {
        let r = cpMask({ a: { a: { a: { a: { a: { a: { a: P_ABS } } } } } } }, SEC)
        assert.strictEqual(typeof r.a.a.a.a.a, 'object')
        assert.strictEqual(r.a.a.a.a.a.a, '[Truncated]')
        let r2 = cpMask({ a: { a: { a: { a: { a: P_ABS } } } } }, SEC)
        assert.strictEqual(r2.a.a.a.a.a, '***')
    })

})
