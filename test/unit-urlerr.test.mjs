import assert from 'assert'
import fs from 'fs'
import path from 'path'
import util from 'util'
import { fileURLToPath } from 'url'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//rulebook R-DIAG-01-5: 建構函式open()失敗之錯誤訊息遮罩(訊息會回顯opt.url組成之儲存路徑, 其可含帳密, 如 user:pw@host)
//做法: 所有情境的儲存路徑皆位於一個一般檔案之下, 故open()必在建立任何資料夾之前失敗(不論平台); 以lmdb的open()直接取得「未遮罩之原錯誤」為對照,
//驗證遮罩後之錯誤: 型態(內建類別)、code、errno、syscall與訊息前綴不變, 儲存路徑(含其任何前綴)不出現於訊息、stack、path、JSON與util.inspect


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

//cases, [名稱, 建構選項]
let cases = [
    ['url為一般檔案(ENOTDIR)', { url: barrier, db: 'worm', cl: 'test' }],
    ['url含user:pw@host之路徑區段', { url: path.join(barrier, `user:${PW}@host`), db: 'worm', cl: 'test' }],
    ['url為scheme://帳密@主機形式(正斜線)', { url: `${barrier}/http://user:${PW}@host/x`, db: 'worm', cl: 'test' }],
    ['url為相對路徑', { url: `./${path.relative(process.cwd(), barrier).split(path.sep).join('/')}/svc-${PW}@host`, db: 'worm', cl: 'test' }],
    ['db與cl名稱含識別字', { url: barrier, db: `db-${PW}`, cl: `cl-${PW}` }],
    ['url含NUL(Node參數驗證錯誤, TypeError)', { url: `${barrier}\u0000${PW}`, db: 'worm', cl: 'test' }],
    ['url含非ASCII與空白', { url: path.join(barrier, `開源 ${PW}@host`), db: 'worm', cl: 'test' }],
    ['url含NUL與換行且很長(Node以多行回顯Received)', { url: `./${'a'.repeat(80)}\nuser:${PW}@host\u0000`, db: 'worm', cl: 'test' }],
    ['cl含副檔名(lmdb改對上一層資料夾建立, 錯誤路徑為url/db)', { url: path.join(barrier, `svc-${PW}@host`), db: 'worm', cl: 'users.v1' }],
]


//rawErr, 以lmdb的open()取得同一儲存路徑之原錯誤(未遮罩)
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

    for (let [name, opt] of cases) {

        it(`R-DIAG-01-5 ${name}: 錯誤訊息不回顯儲存路徑(含任何前綴); 型態、code、errno、syscall與訊息前綴同未遮罩之原錯誤`, function() {
            let raw = rawErr(opt)
            assert.ok(raw instanceof Error, '前提: 未遮罩之原錯誤確實拋出')
            let { err, lines } = ctorErr(opt)
            assert.ok(err instanceof Error, '建構函式應拋出Error')
            assert.deepStrictEqual(lines, [], '建構失敗不得寫console')

            //型態與機器欄位不變
            assert.strictEqual(Object.getPrototypeOf(err), builtinOf(raw).prototype, '內建錯誤類別同原錯誤')
            assert.strictEqual(err.code, raw.code)
            assert.strictEqual(err.errno, raw.errno)
            assert.strictEqual(err.syscall, raw.syscall)

            //訊息前綴不變, 路徑段以***取代(檔案系統錯誤: 路徑段; Node參數驗證錯誤: Received尾段); stack首行同訊息
            let expMask = (s) => {
                if (typeof raw.path === 'string') {
                    return s.split(raw.path).join('***')
                }
                let cut = s.indexOf('Received ') + 'Received '.length
                assert.ok(cut > 'Received '.length)
                return `${s.slice(0, cut)}***`
            }
            assert.strictEqual(err.message, expMask(raw.message), `${err.message} <- ${raw.message}`)
            if (typeof raw.path === 'string') {
                assert.strictEqual(err.path, '***')
            }
            assert.strictEqual(String(err.stack).split('\n')[0], expMask(String(raw.stack).split('\n')[0]), 'stack首行同遮罩後之訊息')
            assert.strictEqual(String(err), expMask(String(raw)), 'toString同遮罩後之原錯誤(含Node內部錯誤之[代碼])')
            assert.ok(err.message.indexOf('***') >= 0)
            if (name.includes('多行')) {
                assert.ok(raw.message.includes('\n'), '前提: 原錯誤訊息確為多行')
            }

            //儲存路徑(原樣、絕對路徑、url、路徑前綴)與其內之識別字不得出現於任何呈現形式
            let storage = `${opt.url}/${opt.db}/${opt.cl}`
            let outs = [err.message, String(err.stack), String(err.path), JSON.stringify(err), util.inspect(err, { depth: 6 }), String(err)]
            for (let o of outs) {
                for (let bad of [PW, opt.url, storage, path.resolve(opt.url), path.resolve(storage), dirBase, barrier, 'afile.txt']) {
                    assert.ok(o.indexOf(bad) < 0, `「${bad}」出現於: ${o.slice(0, 400)}`)
                }
            }
        })

    }

    it('R-DIAG-01-5 選項logDetail與funLog不影響建構錯誤之遮罩, 且funLog不被呼叫(建構失敗非診斷輸出)', function() {
        let calls = []
        let { err, lines } = ctorErr({ url: path.join(barrier, `user:${PW}@host`), db: 'worm', cl: 'test', logDetail: true, funLog: (...a) => calls.push(a) })
        assert.ok(err instanceof Error)
        assert.deepStrictEqual(calls, [])
        assert.deepStrictEqual(lines, [])
        assert.ok(err.message.indexOf(PW) < 0 && String(err.stack).indexOf(PW) < 0)
        assert.strictEqual(err.code, 'ENOENT')
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
