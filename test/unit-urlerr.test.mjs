import assert from 'assert'
import fs from 'fs'
import path from 'path'
import util from 'util'
import { fileURLToPath } from 'url'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//rulebook R-DIAG-01-5: 建構函式open()失敗之錯誤(訊息會回顯opt.url組成之儲存路徑)經wsemi之diag.err後再throw(接線測試, 形態由wsemi共用語料驅動)
//規則(dspec 診斷輸出與遮罩 D3、Q3): 對外錯誤值只遮憑證(scheme://帳密@、Bearer、token=與password=等鍵值對), 不遮儲存路徑與單獨帳號;
//不含憑證者原樣交出(身分不變), 含憑證者交出同型別(內建類別、instanceof、code、errno、syscall)之副本, 憑證換成***且不出現於訊息、stack、path、JSON與util.inspect
//做法: 所有情境的儲存路徑皆位於一個一般檔案之下, 故open()必在建立任何資料夾之前失敗(不論平台); 以lmdb的open()直接取得「未經diag.err之原錯誤」為對照


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let dirBase = path.join(__dirname, '_tmp', 'urlerr')
let barrier = path.join(dirBase, 'afile.txt')
let PW = 'S3cretPw'

//BUILTINS, 內建錯誤類別(沿原型鏈找第一個)
let BUILTINS = [TypeError, RangeError, SyntaxError, ReferenceError, EvalError, URIError, Error]
let builtinOf = (e) => {
    for (let x = Object.getPrototypeOf(e); x !== null; x = Object.getPrototypeOf(x)) {
        let c = BUILTINS.find((v) => v.prototype === x)
        if (c !== undefined) {
            return c
        }
    }
    return null
}

//cases, [名稱, 建構選項, 憑證標記]: 憑證標記為空字串者原錯誤訊息無憑證形式, 原樣交出; 非空者為憑證前之文字, 訊息自頭至此標記須不變, 其後之憑證被遮
//lmdb以檔案系統路徑處理url, 路徑經正規化後`scheme://`的雙斜線消失(http:\user:pw@host), 不再是網址形式, 通用樣式不認得, 故原樣(同Q3: 儲存路徑不遮)
//NUL情境之Node參數驗證錯誤以util.inspect回顯字串, 雙斜線存留, 網址帳密可被遮
let cases = [
    ['url為一般檔案(ENOTDIR)', { url: barrier, db: 'worm', cl: 'test' }, ''],
    ['url含user:pw@host之路徑區段(非網址形式)', { url: path.join(barrier, `user:${PW}@host`), db: 'worm', cl: 'test' }, ''],
    ['url為scheme://帳密@主機形式(路徑正規化後雙斜線消失)', { url: `${barrier}/http://user:${PW}@host/x`, db: 'worm', cl: 'test' }, ''],
    ['url為相對路徑', { url: `./${path.relative(process.cwd(), barrier).split(path.sep).join('/')}/svc-${PW}@host`, db: 'worm', cl: 'test' }, ''],
    ['db與cl名稱含識別字', { url: barrier, db: `db-${PW}`, cl: `cl-${PW}` }, ''],
    ['url含NUL(Node參數驗證錯誤, TypeError)', { url: `${barrier}\u0000${PW}`, db: 'worm', cl: 'test' }, ''],
    ['url含非ASCII與空白', { url: path.join(barrier, `開源 ${PW}@host`), db: 'worm', cl: 'test' }, ''],
    ['url含NUL與換行且很長(Node以多行回顯Received)', { url: `./${'a'.repeat(80)}\nuser:${PW}@host\u0000`, db: 'worm', cl: 'test' }, ''],
    ['cl含副檔名(lmdb改對上一層資料夾建立, 錯誤路徑為url/db)', { url: path.join(barrier, `svc-${PW}@host`), db: 'worm', cl: 'users.v1' }, ''],
    //相對路徑起頭即為網址: Node以util.inspect回顯並截斷過長字串(約128字元), 憑證須在截斷點之前, 故不加絕對路徑前綴(測試資料夾深度不影響)
    ['url為含NUL之scheme://帳密@主機(Node參數驗證錯誤, 雙斜線存留)', { url: `https://user:${PW}@host/x\u0000`, db: 'worm', cl: 'test' }, 'https://'],
    ['url路徑區段為token=值', { url: path.join(barrier, `token=${PW}`), db: 'worm', cl: 'test' }, 'token='],
    ['url路徑區段為password=值', { url: path.join(barrier, `password=${PW}`), db: 'worm', cl: 'test' }, 'password='],
    ['url路徑區段為Bearer值', { url: path.join(barrier, `Bearer ${PW}`), db: 'worm', cl: 'test' }, 'Bearer '],
]


//rawErr, 以lmdb的open()取得同一儲存路徑之原錯誤(未經diag.err)
function rawErr(opt) {
    let storage = `${opt.url}/${opt.db}/${opt.cl}`
    try {
        open({ path: storage, compression: true, useVersions: true })
    }
    catch (err) {
        return err
    }
    return null
}

//ctorErr, 取建構函式拋出之錯誤; 同時擷取console
function ctorErr(opt) {
    let lines = []
    let ks = ['log', 'info', 'warn', 'error', 'debug']
    let olds = ks.map((k) => console[k])
    ks.forEach((k) => {
        console[k] = (...a) => lines.push(a)
    })
    let err = null
    try {
        WOrm(opt)
    }
    catch (e) {
        err = e
    }
    finally {
        ks.forEach((k, i) => {
            console[k] = olds[i]
        })
    }
    return { err, lines }
}


describe('urlerr', function() {

    before(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
        fs.mkdirSync(dirBase, { recursive: true })
        fs.writeFileSync(barrier, 'x')
    })

    after(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
    })

    for (let [name, opt, marker] of cases) {

        it(`R-DIAG-01-5 ${name}: ${marker !== '' ? '憑證形式被遮, 其餘訊息、型態、code、errno、syscall不變' : '無憑證形式, 原樣交出(儲存路徑不遮, Q3)'}`, function() {
            let raw = rawErr(opt)
            assert.ok(raw instanceof Error, '前提: 未經diag.err之原錯誤確實拋出')
            let { err, lines } = ctorErr(opt)
            assert.ok(err instanceof Error, '建構函式應拋出Error')
            assert.deepStrictEqual(lines, [], '建構失敗不得寫console')

            //型態與機器欄位不變
            assert.strictEqual(builtinOf(err), builtinOf(raw), '內建錯誤類別同原錯誤')
            assert.ok(err instanceof builtinOf(raw))
            assert.strictEqual(err.constructor.name, raw.constructor.name)
            if (marker === '') {
                assert.strictEqual(Object.getPrototypeOf(err), Object.getPrototypeOf(raw), '原物件交出者原型同原錯誤(含Node內部錯誤之子類別)')
            }
            assert.strictEqual(err.code, raw.code)
            assert.strictEqual(err.errno, raw.errno)
            assert.strictEqual(err.syscall, raw.syscall)

            if (marker === '') {
                //原樣: 訊息、path、stack首行、toString皆同原錯誤(儲存路徑與其內之識別字不遮)
                assert.strictEqual(err.message, raw.message)
                assert.strictEqual(err.path, raw.path)
                assert.strictEqual(String(err.stack).split('\n')[0], String(raw.stack).split('\n')[0])
                assert.strictEqual(String(err), String(raw))
                if (name.includes('多行')) {
                    assert.ok(raw.message.includes('\n'), '前提: 原錯誤訊息確為多行')
                }
                return
            }

            //含憑證: 副本, 憑證換成***, 其餘訊息不變
            assert.notStrictEqual(err.message, raw.message)
            assert.ok(raw.message.indexOf(PW) >= 0, '前提: 原錯誤訊息確含憑證')
            assert.ok(err.message.indexOf('***') >= 0)
            let head = raw.message.slice(0, raw.message.indexOf(marker) + marker.length)
            assert.ok(err.message.startsWith(head), `憑證標記以前之訊息不變: ${err.message}`)
            assert.ok(String(err.stack).includes(err.message.split('\n')[0]), 'stack首行含遮罩後之訊息')
            if (typeof raw.path === 'string') {
                assert.strictEqual(typeof err.path, 'string', '副本保留path欄位')
            }
            for (let o of [err.message, String(err.stack), String(err.path), JSON.stringify(err), util.inspect(err, { depth: 6 }), String(err)]) {
                assert.ok(o.indexOf(PW) < 0, `憑證出現於: ${o.slice(0, 400)}`)
            }
        })

    }

    it('R-DIAG-01-5 選項logDetail與funLog不影響建構錯誤之處理, 且funLog不被呼叫(建構失敗非診斷輸出)', function() {
        let calls = []
        let { err, lines } = ctorErr({ url: path.join(barrier, `token=${PW}`), db: 'worm', cl: 'test', logDetail: true, funLog: (...a) => calls.push(a) })
        assert.ok(err instanceof Error)
        assert.deepStrictEqual(calls, [])
        assert.deepStrictEqual(lines, [])
        assert.ok(err.message.indexOf(PW) < 0 && String(err.stack).indexOf(PW) < 0)
        assert.strictEqual(err.code, 'ENOTDIR')
    })

    it('R-DIAG-01-5 opt.url含孤立代理字元: 既有訊息(固定詞彙, 不回顯值)與Error型態不變, 且先於open', function() {
        for (let [k, opt] of [['url', { url: `${barrier}/a\ud800b` }], ['db', { url: barrier, db: 'a\udc00b' }], ['cl', { url: barrier, cl: '\ud800' }]]) {
            let { err } = ctorErr(opt)
            assert.strictEqual(Object.getPrototypeOf(err), Error.prototype)
            assert.strictEqual(err.message, `invalid opt.${k}: string contains lone surrogate`)
            assert.strictEqual(err.code, undefined)
        }
    })

    it('R-DIAG-01-5 建構成功之路徑不受影響(同一url可正常建構、讀寫、關閉)', async function() {
        let url = path.join(dirBase, `ok-${PW}@host`)
        let w = WOrm({ url, db: 'worm', cl: 'test' })
        await w.insert({ id: 'u1', a: 1 })
        let r = await w.selectByPk('u1')
        assert.strictEqual(r.a, 1)
        await w.close()
    })

})
