import assert from 'assert'
import fs from 'fs'
import path from 'path'
import util from 'util'
import { fileURLToPath } from 'url'
import D from 'wsemi/src/_diagCases.mjs'
import WOrm from '../src/WOrmLmdb.mjs'


//rulebook R-DIAG-01(依dspec 診斷輸出與遮罩; 遮罩與出口為wsemi之getDiag): 診斷輸出(detail出口)之接線測試, 形態由wsemi共用語料(_diagCases)驅動
//接線: 令change或error事件之訂閱函數拋出語料make()產生之值, 經本套件真實站點('change handler threw'、'error handler threw')由funLog收到(label, safe, detail), 以語料之期望判定detail與safe;
//本套件不宣告帳號、密碼與權杖, 其秘密為儲存路徑(只於診斷輸出遮罩), 故語料中以ctx.sec.password產生之值改以本實例之儲存路徑代入, 比對之秘密針對該路徑(語料verify之5字元片段檢查針對固定測試秘密, 不適用於路徑,
//其會把堆疊中的檔案路徑片段誤判為洩漏, 故本檔以語料之collect走訪輸出並比對完整路徑)
//形態類(深度、循環、Proxy、二進位、包裝物件、存取器等)由wsemi自身逐案測試(有util與無util兩種環境), 不在本檔重複


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let dirBase = path.join(__dirname, '_tmp', 'diaglog-corpus')
let url = path.join(dirBase, 'svc-S3cretPw@host')

//WIRE, 本套件對語料wire案(各套件接線測試應重跑之案)之處置: detail=經訂閱函數拋出由funLog收到之detail依語料期望驗(change與error兩站點各一次);
//brief=safe摘要依語料期望驗; reject=於unit-diagerr以真實catch站點驗; own=以本套件自身之等價案驗(原因見註); na=不適用(原因見註)
let WIRE = {
    T01: ['detail'],
    T07: ['detail', 'reject'],
    K01: ['detail'],
    K09: ['detail'],
    V02: ['detail'],
    E01: ['detail'],
    E07: ['detail'],
    P01: ['detail'],
    S07: ['detail'],
    C01: ['brief'],
    R01: ['reject'],
    R02: ['na'], //畸形網址片段: 本套件不宣告urls(儲存路徑非連線字串)
    R04: ['na'], //字串reject依宣告之憑證遮罩: 本套件不宣告帳號、密碼、權杖, reject清單無本套件宣告之項
    R07: ['own'], //固定詞彙之reject原樣: 本套件之等價案為回顯呼叫端自給之id、欄名、運算子名之訊息(unit-diagerr)
    R11: ['own'], //Q3儲存路徑: reject不遮(unit-diagerr)、診斷輸出遮(本檔)
    R12: ['na'], //單獨帳號: 本套件不宣告帳號
    O01: ['own'], //logDetail出口: 標籤為本套件之固定標籤, 由unit-diaglog-run驗
    O15: ['own'], //safe白名單: safe由本套件內部建構, 不接受外部注入, 以safeKeys宣告驗(unit-diaglog-run之摘要欄位)
    A01: ['na'], //genDiagFunLog: 本套件不使用轉接器
    A07: ['na'],
}

let nCl = 0
let ws = []

//mk, 建立帶funLog之實例, got收(label, safe, detail?)
let mk = (optExtra = {}) => {
    let cl = `c${++nCl}`
    let got = []
    let w = WOrm({ url, db: 'worm', cl, funLog: (...a) => got.push(a), ...optExtra })
    ws.push(w)
    return { w, got, storage: path.resolve(`${url}/worm/${cl}`) }
}

//fire, 令指定站點之訂閱函數拋出value
let fire = async(w, site, value) => {
    if (site === 'change') {
        w.on('change', () => {
            throw value
        })
        await w.insert({ id: 'd1' })
    }
    else {
        w.on('error', () => {
            throw value
        })
        await w.insert({ id: 'd1' })
        await w.insertBulk({ id: 'd1' }).catch(() => {})
    }
}

let win5 = (s) => {
    let r = []
    for (let i = 0; i + 5 <= s.length; i++) {
        r.push(s.slice(i, i + 5))
    }
    return r
}

//leakOf, 走訪輸出之全部字串、鍵與Symbol描述, 含任一秘密(或逐字元串接)回傳之, 否則null
let leakOf = (out, needles) => {
    let { ss, chars, trunc } = D.collect(out)
    if (trunc) {
        return 'walk limit exceeded'
    }
    for (let s of ss) {
        for (let x of needles) {
            if (s.indexOf(x) >= 0) {
                return x
            }
        }
    }
    for (let x of needles) {
        if (x.length > 1 && chars.indexOf(x) >= 0) {
            return `${x} (char by char)`
        }
    }
    return null
}

let caseOf = (id) => {
    let c = D.cases.find((v) => v.id === id)
    assert.ok(c !== undefined && c.wire === true, `語料應有wire案${id}`)
    return c
}

let mkCtx = (storage) => {
    let ctx = D.mkCtx({ util: true, isNativeError: util.types.isNativeError })
    ctx.sec.password = storage //本套件之秘密為儲存路徑
    return ctx
}


describe('diaglog', function() {

    before(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
        fs.mkdirSync(dirBase, { recursive: true })
    })

    after(async function() {
        for (let w of ws) {
            await w.close()
        }
        fs.rmSync(dirBase, { recursive: true, force: true })
    })

    describe('共用語料之wire案處置表', function() {

        it('R-DIAG-01-8 語料中全部wire案皆有處置(語料新增wire案時須判定本套件適用與否), 且處置表中無語料已不存在之案', function() {
            let ids = D.cases.filter((c) => c.wire).map((c) => c.id).sort()
            assert.deepStrictEqual(Object.keys(WIRE).sort(), ids)
        })

    })

    describe('語料驅動之detail接線(change與error兩站點)', function() {

        for (let [id, kinds] of Object.entries(WIRE)) {

            if (kinds.includes('detail')) {
                for (let site of ['change', 'error']) {
                    it(`${id} (detail, ${site}站點) 訂閱函數拋出語料之值 → funLog收到之detail依語料期望遮罩`, async function() {
                        let c = caseOf(id)
                        let { w, got, storage } = mk()
                        let ctx = mkCtx(storage)
                        let value = c.make(ctx)
                        await fire(w, site, value)
                        assert.strictEqual(got.length, 1, `funLog應收到一次: ${got.length}`)
                        assert.strictEqual(got[0].length, 3)
                        assert.strictEqual(got[0][0], site === 'change' ? 'change handler threw' : 'error handler threw')
                        let detail = got[0][2]
                        let needles = [storage, storage.replace(/\\/g, '/'), ...ctx.extra, ...ctx.extra.flatMap((s) => win5(s))]
                        assert.strictEqual(leakOf(detail, needles), null, '秘密不得出現於detail')
                        if (c.expect.userCode === 0) {
                            assert.strictEqual(ctx.hits, 0, '使用端程式(getter)不得被執行')
                        }
                        assert.strictEqual(c.expect.check === undefined ? true : c.expect.check(detail, ctx), true)
                    })
                }
            }

            if (kinds.includes('brief')) {
                for (let site of ['change', 'error']) {
                    it(`${id} (brief, ${site}站點) 訂閱函數拋出語料之錯誤 → funLog收到之safe依語料期望(僅mode、errType、errCode)`, async function() {
                        let c = caseOf(id)
                        let safes = []
                        for (let e of c.make(mkCtx('x'))) {
                            let { w, got } = mk()
                            await fire(w, site, e)
                            assert.strictEqual(got.length, 1)
                            assert.strictEqual(got[0].length, 3)
                            let { mode, ...rest } = got[0][1]
                            assert.strictEqual(mode, site === 'change' ? 'insert' : 'insertBulk')
                            safes.push(rest)
                        }
                        assert.strictEqual(c.expect.check(safes, mkCtx('x')), true)
                    })
                }
            }

        }

    })

    describe('宣告(patterns、paths)之接線: 診斷輸出中的儲存位置與Node訊息尾段', function() {

        //detailOf, 訂閱函數拋出value, 回funLog收到之detail
        let detailOf = async(value, optExtra = {}) => {
            let { w, got } = mk(optExtra)
            await fire(w, 'change', value)
            assert.strictEqual(got.length, 1)
            return got[0][2]
        }

        it('R-DIAG-01-2 fsPathTail: Node檔案系統訊息之路徑尾段(任何路徑, 非儲存位置亦然)於診斷輸出遮罩, 訊息前綴與堆疊框架保留', async function() {
            let d = await detailOf(new Error('ENOENT: no such file or directory, open \'D:\\other\\unknown\\p.txt\''))
            assert.strictEqual(d.message, 'ENOENT: no such file or directory, open \'***\'')
            assert.ok(String(d.stack).indexOf('D:\\other\\unknown') < 0)
            assert.ok(String(d.stack).includes('\n    at '), '堆疊框架行不被吞')
        })

        it('R-DIAG-01-2 receivedTail: Node參數錯誤之Received尾段於診斷輸出遮罩', async function() {
            let d = await detailOf(new TypeError('The "path" argument must be of type string. Received \'D:\\\\q\\\\r\''))
            assert.ok(d.message.startsWith('The "path" argument must be of type string. Received '))
            assert.ok(d.message.indexOf('D:') < 0 && d.message.indexOf('***') >= 0, d.message)
        })

        it('R-DIAG-01-2 儲存位置三層(url、url/db、url/db/cl)之原樣、絕對路徑與斜線互換形式於診斷輸出皆遮罩, 其餘文字保留', async function() {
            let { w, got, storage } = mk()
            let flip = (s) => s.replace(/\\/g, '/')
            let uDb = `${url}/worm`
            w.on('change', () => {
                throw new Error(`a ${url} | b ${uDb} | c ${storage} | d ${flip(storage)} | e ${flip(url)} | id u-1`)
            })
            await w.insert({ id: 'x1' })
            assert.strictEqual(got.length, 1)
            assert.strictEqual(got[0][2].message, 'a *** | b *** | c *** | d *** | e *** | id u-1')
        })

        it('R-DIAG-01-2 Q3: 錯誤自帶path為儲存路徑 → 診斷輸出之path與訊息遮罩, 原物件不被改動(對外reject則不遮, 見unit-diagerr)', async function() {
            let { w, got, storage } = mk()
            let thrown = Object.assign(new Error(`ENOENT: no such file or directory, open '${storage}'`), { path: storage, code: 'ENOENT' })
            w.on('change', () => {
                throw thrown
            })
            await w.insert({ id: 'x1' })
            let d = got[0][2]
            assert.strictEqual(d.path, '***')
            assert.strictEqual(d.code, 'ENOENT')
            assert.ok(d.message.indexOf(storage) < 0)
            assert.strictEqual(thrown.path, storage)
            assert.ok(thrown.message.indexOf(storage) >= 0)
        })

        it('R-DIAG-01-2 憑證不分來源皆遮: scheme://帳密@、Bearer、token=於診斷輸出遮罩(通用樣式, 不依賴宣告)', async function() {
            let d = await detailOf(new Error('x https://user:PwZz1234@h.example.com/a Bearer abc.def token=qq77'))
            assert.strictEqual(d.message, 'x https://***@h.example.com/a Bearer *** token=***')
        })

    })

})
