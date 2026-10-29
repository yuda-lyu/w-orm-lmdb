import assert from 'assert'
import fs from 'fs'
import path from 'path'
import util from 'util'
import { fileURLToPath } from 'url'
import { Unpackr, addExtension } from 'msgpackr'
import D from 'wsemi/src/_diagCases.mjs'
import WOrm from '../src/WOrmLmdb.mjs'


//rulebook R-DIAG-01-7: 對外錯誤值(reject、'error'事件之訊息、逐筆結果之err)不含憑證(dspec 診斷輸出與遮罩 D3、Q3)之接線測試
//規則: 外來值(lmdb、msgpackr編碼器與解碼器、檔案系統、使用端回呼拋出者)於catch站點經wsemi之diag.err(逐筆err與事件訊息經diag.mask), 含憑證者改為同型別副本, 不含者原物件;
//本套件自身以固定詞彙組成之錯誤(含回顯呼叫端自給之id、欄名、運算子名者)原樣交出, 不經遮罩;
//儲存路徑不遮(Q3); 遮罩只認通用憑證形式(scheme://帳密@、Bearer、token=與password=等鍵值對), 本套件不宣告帳號密碼
//
//接縫(皆為真實執行路徑上的外來程式, 非替換被測函數):
//  decode: 暫時替換msgpackr之Unpackr.prototype.decode(lmdb讀取時用以解碼), 令select、selectByPk、delAll、del、save逐筆走到解碼失敗之catch站點
//  encode: 以msgpackr之addExtension登記一個類別, 其編碼拋錯(使用端類型之編碼器失敗), 令insert、insertBulk、save走到編碼失敗之站點
//  getter: 使用端資料之getter拋錯(cloneDeep於各操作之try內), 令insert、insertBulk、save、del走到整批性catch站點
//  fs: 備份目標路徑於一般檔案之下, 令fs回報含路徑之錯誤
//註: msgpackr為lmdb之相依套件(hoisted於node_modules)


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let dirBase = path.join(__dirname, '_tmp', 'diagerr')
let url = path.join(dirBase, 'svc')
let barrier = path.join(dirBase, 'afile.txt')
let PW = 'S3cretPw'
let TK = 'TK9zQ7'
let FOREIGN_MSG = `driver failed https://user:${PW}@db.example.com/x token=${TK}`
let MASKED_MSG = 'driver failed https://***@db.example.com/x token=***'
let ENC_MSG = `encoder failed https://user:${PW}@db.example.com/x token=${TK}`
let ENC_MASKED = 'encoder failed https://***@db.example.com/x token=***'

//Boom, 編碼時拋出含憑證之錯誤的使用端類型(addExtension為全域登記, 只有Boom之實例會觸發)
class Boom {}
addExtension({
    Class: Boom,
    type: 0x7e,
    write() {
        throw new Error(ENC_MSG)
    },
    read() {
        return null
    },
})

let mkForeign = () => Object.assign(new Error(FOREIGN_MSG), { code: 'ECONNRESET' })

let nCl = 0
let ws = []

//mkW, 建立實例並記錄'error'事件
let mkW = (optExtra = {}) => {
    let cl = `c${++nCl}`
    let w = WOrm({ url, db: 'worm', cl, ...optExtra })
    ws.push(w)
    let evs = []
    w.on('error', (mode, data, msg) => {
        evs.push({ mode, data, msg })
    })
    return { w, evs, storage: path.resolve(`${url}/worm/${cl}`) }
}

//settle, 取結算(resolve值或reject值)
let settle = async(pm) => {
    try {
        return { ok: await pm }
    }
    catch (err) {
        return { err }
    }
}

//withDecodeError, 解碼接縫
let withDecodeError = async(fn, mk = mkForeign) => {
    let old = Unpackr.prototype.decode
    Unpackr.prototype.decode = function() {
        throw mk()
    }
    try {
        return await fn()
    }
    finally {
        Unpackr.prototype.decode = old
    }
}

//badData, 使用端資料: 取值時拋出外來錯誤(cloneDeep讀取getter)
let badData = (id = 'g1', mk = mkForeign) => ({
    id,
    get v() {
        throw mk()
    },
})

//noLeak, 憑證不得出現於錯誤之任何呈現形式
let noLeak = (e) => {
    for (let o of [e.message, String(e.stack), JSON.stringify(e), util.inspect(e, { depth: 6 }), String(e)]) {
        for (let s of [PW, TK]) {
            assert.ok(o.indexOf(s) < 0, `「${s}」出現於: ${o.slice(0, 400)}`)
        }
    }
}

//assertMasked, 整批reject: 同型別副本(instanceof、code保留)、憑證被遮, 'error'事件之訊息與reject同源
let assertMasked = (e, evs, mode, msg = MASKED_MSG) => {
    assert.ok(e instanceof Error, `reject值應為Error: ${String(e)}`)
    assert.strictEqual(e.message, msg)
    assert.strictEqual(e.code, 'ECONNRESET', '副本保留code')
    noLeak(e)
    let k = evs.filter((v) => v.mode === mode)
    assert.strictEqual(k.length, 1, `'error'事件應發出一次: ${evs.map((v) => v.mode)}`)
    assert.strictEqual(k[0].msg, e.message, '\'error\'事件之訊息與reject同源')
    noLeakText(k[0].msg)
}

let noLeakText = (s) => {
    for (let x of [PW, TK]) {
        assert.ok(String(s).indexOf(x) < 0, `「${x}」出現於: ${String(s).slice(0, 300)}`)
    }
}


describe('diagerr', function() {

    before(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
        fs.mkdirSync(dirBase, { recursive: true })
        fs.writeFileSync(barrier, 'x')
    })

    after(async function() {
        for (let w of ws) {
            await w.close()
        }
        fs.rmSync(dirBase, { recursive: true, force: true })
    })

    describe('外來值經diag.err: 含憑證之外來reject被遮(站點 x 出口)', function() {

        //整批性catch站點(使用端資料之getter拋錯)
        for (let [name, call] of [
            ['insert', (w) => w.insert(badData())],
            ['insertBulk', (w) => w.insertBulk(badData())],
            ['save', (w) => w.save(badData())],
            ['del', (w) => w.del(badData())],
        ]) {
            it(`R-DIAG-01-7 ${name}: 使用端回呼(資料getter)拋出含憑證之錯誤 → reject為同型別副本且憑證被遮, 'error'事件之訊息同源`, async function() {
                let { w, evs } = mkW()
                let { err } = await settle(call(w))
                assertMasked(err, evs, name)
            })
        }

        //解碼接縫
        for (let [name, mode, call, pre] of [
            ['select', 'select', (w) => w.select({}), true],
            ['selectByPk', 'selectByPk', (w) => w.selectByPk('p1'), true],
            ['delAll', 'delAll', (w) => w.delAll({}), true],
        ]) {
            it(`R-DIAG-01-7 ${name}: 讀取階段(解碼)拋出含憑證之外來錯誤 → reject為副本且憑證被遮, 'error'事件之訊息同源`, async function() {
                let { w, evs } = mkW()
                if (pre) {
                    await w.insert({ id: 'p1', a: 1 })
                }
                let { err } = await withDecodeError(() => settle(call(w)))
                assertMasked(err, evs, mode)
            })
        }

        it('R-DIAG-01-7 save逐筆: 解碼拋出含憑證之外來錯誤 → 整批resolve, 該筆ok為0且err被遮(建立時即遮), 逐筆error事件之訊息同源', async function() {
            let { w, evs } = mkW()
            await w.insert({ id: 'p1', a: 1 })
            let { ok, err } = await withDecodeError(() => settle(w.save({ id: 'p1', a: 2 })))
            assert.strictEqual(err, undefined)
            assert.deepStrictEqual(ok, [{ n: 1, nInserted: 0, nModified: 0, ok: 0, err: MASKED_MSG }])
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['save', MASKED_MSG]])
            noLeakText(JSON.stringify(ok))
        })

        it('R-DIAG-01-7 del逐筆: 解碼拋出含憑證之外來錯誤 → 整批resolve, 該筆ok為0且err被遮, 逐筆error事件之訊息同源', async function() {
            let { w, evs } = mkW()
            await w.insert({ id: 'p1', a: 1 })
            let { ok, err } = await withDecodeError(() => settle(w.del({ id: 'p1' })))
            assert.strictEqual(err, undefined)
            assert.deepStrictEqual(ok, [{ n: 1, nDeleted: 0, ok: 0, err: MASKED_MSG }])
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['del', MASKED_MSG]])
        })

        it('R-DIAG-01-7 編碼階段: 使用端類型之編碼器拋出含憑證之錯誤 → insert與insertBulk之reject訊息為固定前綴加遮罩後之原訊息; save逐筆err被遮', async function() {
            let { w, evs } = mkW()
            let r1 = await settle(w.insert({ id: 'e1', v: new Boom() }))
            assert.strictEqual(r1.err.message, `can not insert by data[0], ${ENC_MASKED}`)
            noLeak(r1.err)
            let r2 = await settle(w.insertBulk({ id: 'e2', v: new Boom() }))
            assert.strictEqual(r2.err.message, `can not insertBulk by data[0], ${ENC_MASKED}`)
            noLeak(r2.err)
            let r3 = await settle(w.save({ id: 'e3', v: new Boom() }))
            assert.deepStrictEqual(r3.ok, [{ n: 1, nInserted: 0, nModified: 0, ok: 0, err: ENC_MASKED }])
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['insert', r1.err.message], ['insertBulk', r2.err.message], ['save', ENC_MASKED]])
        })

        it('R-DIAG-01-7 backup: 檔案系統錯誤回顯含憑證形式之目標路徑 → reject訊息為固定前綴加遮罩後之原訊息, 路徑本身不遮, error事件同源', async function() {
            let { w, evs } = mkW()
            let { err } = await settle(w.backup(path.join(barrier, `token=${TK}`, 'sub')))
            assert.ok(err instanceof Error)
            assert.ok(err.message.startsWith('can not backup, '), err.message)
            assert.ok(err.message.indexOf('afile.txt') >= 0, '路徑存留(Q3: 儲存路徑不遮)')
            assert.ok(err.message.indexOf('token=***') >= 0, err.message)
            noLeak(err)
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.data, v.msg]), [['backup', null, err.message]])
        })

        it('R-DIAG-01-7 不含憑證之外來錯誤: 原物件交出(身分不變, code與其餘屬性不動)', async function() {
            let { w, evs } = mkW()
            await w.insert({ id: 'p1', a: 1 })
            let thrown = Object.assign(new Error('plain upstream failure MDB_PANIC'), { code: 'MDB_PANIC', extra: { n: 1 } })
            let { err } = await withDecodeError(() => settle(w.selectByPk('p1')), () => thrown)
            assert.strictEqual(err, thrown, '不含憑證者不改為副本')
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['selectByPk', 'plain upstream failure MDB_PANIC']])
        })

        it('R-DIAG-01-7 Q3: 外來錯誤自帶儲存路徑 → reject不遮路徑(原物件, err.path完整); 同一路徑於診斷輸出則遮(見unit-diaglog)', async function() {
            let { w, storage } = mkW()
            await w.insert({ id: 'p1', a: 1 })
            let thrown = Object.assign(new Error(`ENOENT: no such file or directory, open '${storage}'`), { path: storage, code: 'ENOENT' })
            let { err } = await withDecodeError(() => settle(w.select({})), () => thrown)
            assert.strictEqual(err, thrown)
            assert.strictEqual(err.path, storage)
            assert.ok(err.message.indexOf(storage) >= 0)

            //非儲存位置之路徑尾段(fsPathTail為只用於診斷輸出之樣式)與Received尾段(receivedTail)於對外值亦不遮
            let other = Object.assign(new Error('ENOENT: no such file or directory, open \'D:\\other\\unknown\\p.txt\''), { code: 'ENOENT' })
            let r2 = await withDecodeError(() => settle(w.select({})), () => other)
            assert.strictEqual(r2.err, other)
            let recv = new TypeError('The "path" argument must be of type string. Received \'D:\\\\q\\\\r\'')
            let r3 = await withDecodeError(() => settle(w.select({})), () => recv)
            assert.strictEqual(r3.err, recv)
        })

    })

    describe('本套件自身以固定詞彙組成之錯誤原樣交出(不經遮罩, 含回顯呼叫端自給之文字者)', function() {

        //呼叫端自給之文字恰為憑證形式(id、欄名、運算子名)時, 屬回傳契約, 不得被遮罩改寫(遮罩會使固定字串被挖空而可反推)
        it('R-DIAG-01-7 insertBulk主鍵衝突: 訊息回顯呼叫端自給之id(含token=形式)原樣, error事件同源', async function() {
            let { w, evs } = mkW()
            let id = `token=${TK}`
            await w.insert({ id })
            let { err } = await settle(w.insertBulk({ id }))
            assert.strictEqual(err.message, `can not insertBulk by existed id[${id}]`)
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['insertBulk', err.message]])
        })

        it('R-DIAG-01-7 insert與insertBulk資料良構檢核: 訊息回顯欄名(含password=形式)原樣, error事件同源', async function() {
            let { w, evs } = mkW()
            let key = `password=${PW}`
            let r1 = await settle(w.insert({ id: 'k1', [key]: '\ud800' }))
            let r2 = await settle(w.insertBulk({ id: 'k2', [key]: '\ud800' }))
            for (let r of [r1, r2]) {
                assert.strictEqual(r.err.message, `invalid data: string contains lone surrogate at [data[0].${key}]`)
            }
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['insert', r1.err.message], ['insertBulk', r2.err.message]])
        })

        it('R-DIAG-01-7 save與del逐筆: 本套件自身之err(良構檢核、無效id)原樣, 回顯之欄名不被遮', async function() {
            let { w, evs } = mkW()
            let key = `password=${PW}`
            await w.insert({ id: 's1', a: 1 })
            let r1 = await settle(w.save({ id: 's2', [key]: '\ud800' })) //新增路徑: 良構檢核
            let r2 = await settle(w.save({ id: 's1', [key]: '\ud800' })) //合併路徑(既有記錄): 交易內再檢核
            let r3 = await settle(w.del({ id: 'a\u0000b' }))
            assert.strictEqual(r1.ok[0].err, `invalid data: string contains lone surrogate at [data[0].${key}]`)
            assert.strictEqual(r2.ok[0].err, `invalid data: string contains lone surrogate at [data[0].${key}]`)
            assert.strictEqual(r3.ok[0].err, 'invalid id: string contains NUL at [data[0].id]')
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['save', r1.ok[0].err], ['save', r2.ok[0].err], ['del', r3.ok[0].err]])
        })

        it('R-DIAG-01-7 save合併路徑之交易內再檢核(繼承存取器第2次讀回孤立代理): 本套件自身之err原樣, 回顯之欄名(含password=形式)不被遮', async function() {
            let { w, evs } = mkW()
            let key = `password=${PW}`
            await w.insert({ id: 'g1', name: 'pre' })
            let n = 0
            let proto = {}
            Object.defineProperty(proto, key, {
                enumerable: true,
                get() {
                    return n++ === 0 ? 'ok' : 'bad\ud800'
                },
            })
            let d = Object.create(proto)
            d.id = 'g1'
            let r = await settle(w.save([d]))
            assert.strictEqual(r.ok[0].ok, 0)
            assert.strictEqual(r.ok[0].err, `invalid data: string contains lone surrogate at [data[0].${key}]`)
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['save', r.ok[0].err]])
        })

        it('R-DIAG-01-7 select與delAll條件檢核: 訊息回顯欄名與運算子名(含token=、password=形式)原樣, error事件同源', async function() {
            let { w, evs } = mkW()
            let r1 = await settle(w.select({ [`password=${PW}`]: { $foo: 1 } }))
            let r2 = await settle(w.delAll({ a: { [`$token=${TK}`]: 1 } }))
            assert.ok(r1.err.message.startsWith('invalid find:') && r1.err.message.indexOf(`password=${PW}`) >= 0, r1.err.message)
            assert.ok(r2.err.message.startsWith('invalid find:') && r2.err.message.indexOf(`$token=${TK}`) >= 0, r2.err.message)
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.msg]), [['select', r1.err.message], ['delAll', r2.err.message]])
        })

        it('R-DIAG-01-7 backup檢核錯誤(本套件自身)訊息原樣: 目標為非空資料夾', async function() {
            let { w, evs } = mkW()
            let dirTar = path.join(dirBase, 'tar')
            fs.mkdirSync(dirTar, { recursive: true })
            fs.writeFileSync(path.join(dirTar, 'x.txt'), 'x')
            let { err } = await settle(w.backup(dirTar))
            assert.strictEqual(err.message, 'can not backup by target folder which is not empty')
            assert.deepStrictEqual(evs.map((v) => [v.mode, v.data, v.msg]), [['backup', null, err.message]])
        })

        it('R-DIAG-01-7 已關閉: 訊息原樣', async function() {
            let { w } = mkW()
            await w.close()
            let { err } = await settle(w.insert({ id: 'late' }))
            assert.strictEqual(err.message, 'can not insert by closed client, instance can not be used after close()')
        })

    })

    describe('共用語料之接線案(wsemi _diagCases wire案中屬對外錯誤值者; 其餘見unit-diaglog之處置表)', function() {

        let ctxOf = () => D.mkCtx({ util: true, isNativeError: util.types.isNativeError })
        let caseOf = (id) => {
            let c = D.cases.find((v) => v.id === id)
            assert.ok(c !== undefined && c.wire === true, `語料應有wire案${id}`)
            return c
        }

        //T07: 通用憑證樣式(Bearer、token=、"password":、authorization: Basic)之字串 → 使用端拋出該字串, reject為其遮罩結果
        it('T07 (reject) 使用端拋出含通用憑證樣式之字串 → reject之字串依語料期望遮罩, error事件之訊息同源', async function() {
            let c = caseOf('T07')
            let ctx = ctxOf()
            let value = c.make(ctx)
            let { w, evs } = mkW()
            let { err } = await settle(w.insert(badData('t07', () => value)))
            assert.strictEqual(c.expect.check(err, ctx), true)
            assert.strictEqual(verifyCase(c, err, ctx), null)
            assert.strictEqual(evs.length, 1)
            assert.strictEqual(evs[0].msg, err)
        })

        //R01: 不含憑證之錯誤 → 原物件
        it('R01 (reject) 使用端拋出不含憑證之錯誤 → reject為同一物件', async function() {
            let c = caseOf('R01')
            let ctx = ctxOf()
            let value = c.make(ctx)
            let { w } = mkW()
            let { err } = await settle(w.insert(badData('r01', () => value)))
            assert.strictEqual(c.expect.check([value, err], ctx), true)
        })

    })

})


//verifyCase, 以語料之verify判定(憑證類秘密不含本套件宣告之路徑, 故只驗語料原有之期望)
function verifyCase(c, out, ctx) {
    return D.verify(c, out, ctx)
}
