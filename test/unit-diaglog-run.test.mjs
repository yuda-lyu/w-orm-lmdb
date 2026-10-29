import assert from 'assert'
import fs from 'fs'
import path from 'path'
import util from 'util'
import { fileURLToPath } from 'url'
import WOrm from '../src/WOrmLmdb.mjs'
import { scenario, ID } from './tools/diagScenario.mjs'


//rulebook R-DIAG-01: 診斷輸出(實例層, 開真實LMDB; 遮罩與出口為wsemi之getDiag, 本檔為接線測試)
//對應: 預設時console只出現標籤與不含個資之摘要(-1)、logDetail輸出標籤加遮罩後之細節且儲存位置遮罩(-2)、funLog收完整細節(-3)、輸出不得影響操作(-4)、
//回傳契約不變(-6: 各操作之resolve與reject內容、事件payload與時機, 對照修改前HEAD dfa8bb7產生之固定預期值test/tools/diagGolden.json)
//本檔只經WOrmLmdb實例, 不匯入任何診斷模組


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let golden = JSON.parse(fs.readFileSync(path.join(__dirname, 'tools', 'diagGolden.json'), 'utf8'))
let dirBase = path.join(__dirname, '_tmp', 'diaglog')
let url = path.join(dirBase, 'svc-S3cretPw@host')
let PW = 'S3cretPw'

//golden之console輸出以<URL>代替opt.url; 新版中opt.url屬儲存位置, 一律遮罩為***
let toMasked = (s) => s.split('<URL>').join('***')

let nCl = 0
let nextCl = () => `c${++nCl}`

//json, JSON往返(與golden之產生方式一致)
let json = (v) => JSON.parse(JSON.stringify(v))


//capAsync, 擷取console各方法之輸出(保留原引數)
async function capAsync(fn) {
    let calls = []
    let ks = ['log', 'info', 'warn', 'error', 'debug']
    let olds = ks.map((k) => console[k])
    ks.forEach((k) => {
        console[k] = (...a) => calls.push({ method: k, args: a })
    })
    try {
        return { r: await fn(), calls }
    }
    finally {
        ks.forEach((k, i) => {
            console[k] = olds[i]
        })
    }
}

//runSc, 於指定選項下執行情境並擷取console
let runSc = (optExtra, withBad) => capAsync(() => scenario(WOrm, { url, db: 'worm', cl: nextCl(), ...optExtra }, withBad))

//allText, 將擷取之console輸出轉為單一字串供「不得出現」比對
let allText = (calls) => calls.map((c) => c.args.map((a) => (typeof a === 'string' ? a : util.inspect(a, { depth: 5 }))).join(' ')).join('\n')


describe('diaglog-run', function() {

    before(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
        fs.mkdirSync(dirBase, { recursive: true })
    })

    after(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
    })

    describe('預設(未給logDetail與funLog)', function() {

        it('R-DIAG-01-1 一般操作與錯誤(無訂閱函數拋錯)時console完全無輸出', async function() {
            let { calls } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl() })
                await w.insert([{ id: 'q1', name: 'n' }])
                await w.insert({ id: 'q1' }).catch(() => {})
                await w.insertBulk([{ id: 'q1' }]).catch(() => {})
                await w.save({ id: 'q2' })
                await w.select({ $foo: 1 }).catch(() => {})
                await w.del([{ id: 'q1' }, { name: 'no-id' }])
                await w.delAll({})
                await w.backup('').catch(() => {})
                await w.close()
                await w.insert({ id: 'late' }).catch(() => {})
            })
            assert.deepStrictEqual(calls, [])
        })

        it('R-DIAG-01-1 訂閱函數拋錯: 每次拋錯一行, 只含固定標籤與{mode, errType}, 不含id、錯誤原文、儲存位置與資料值', async function() {
            let { r, calls } = await runSc({}, true)
            assert.strictEqual(calls.length, r.evs.length)
            assert.ok(calls.length > 0)
            calls.forEach((c, k) => {
                assert.strictEqual(c.method, 'log')
                assert.strictEqual(c.args.length, 2, `第${k}行引數數`)
                assert.strictEqual(c.args[0], r.evs[k][0] === 'change' ? 'change handler threw' : 'error handler threw', `第${k}行標籤`)
                assert.deepStrictEqual(c.args[1], { mode: r.evs[k][1], errType: 'Error' }, `第${k}行摘要`)
            })
            let labels = new Set(calls.map((c) => c.args[0]))
            assert.deepStrictEqual([...labels].sort(), ['change handler threw', 'error handler threw'], 'change與error兩個站點皆被觸發')
            let t = allText(calls)
            for (let bad of [ID, PW, url, 'listener', 'n1', 'n2', 'dup', 'a1', 'no-id', 'svc-']) {
                assert.ok(t.indexOf(bad) < 0, `預設輸出含「${bad}」: ${t.slice(0, 300)}`)
            }
            assert.strictEqual(calls.length, golden.bad.calls.length, '輸出行數同修改前(每次拋錯一行)')
        })

        it('R-DIAG-01-1 訂閱函數拋出非Error值(字串、物件、null、數字): 只列型別, 不列內容', async function() {
            let { calls } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl() })
                let thrown = [`str ${ID}`, { message: ID }, null, 42]
                let k = 0
                w.on('change', () => {
                    throw thrown[k++ % thrown.length]
                })
                await w.insert({ id: 's1' })
                await w.insert({ id: 's2' })
                await w.insert({ id: 's3' })
                await w.insert({ id: 's4' })
                await w.close()
            })
            assert.deepStrictEqual(calls.map((c) => c.args), [
                ['change handler threw', { mode: 'insert', errType: 'string' }],
                ['change handler threw', { mode: 'insert', errType: 'object' }],
                ['change handler threw', { mode: 'insert', errType: 'null' }],
                ['change handler threw', { mode: 'insert', errType: 'number' }],
            ])
            assert.ok(allText(calls).indexOf(ID) < 0)
        })

        it('R-DIAG-01-1 訂閱函數拋出帶機器代碼之Error: 摘要列errCode; 代碼非機器樣式者不列', async function() {
            let { calls } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl() })
                let es = []
                let e1 = new Error(`x ${ID}`)
                e1.code = 'ENOENT'
                let e2 = new TypeError(`y ${ID}`)
                e2.code = ID
                let e3 = new Error('z')
                e3.code = 'MDB_MAP_FULL' //lmdb之代碼集合(宣告codes: lmdb)
                let e4 = new Error('w')
                e4.code = 5 //整數不收(代碼一律明列, 不收整數)
                let e5 = new Error('v')
                e5.code = 'MDB_NOT_A_REAL_CODE' //未明列之樣式相符字串不收
                es.push(e1, e2, e3, e4, e5)
                let k = 0
                w.on('change', () => {
                    throw es[k++]
                })
                for (let i = 0; i < es.length; i++) {
                    await w.insert({ id: `k${i}` })
                }
                await w.close()
            })
            assert.deepStrictEqual(calls.map((c) => c.args), [
                ['change handler threw', { mode: 'insert', errType: 'Error', errCode: 'ENOENT' }],
                ['change handler threw', { mode: 'insert', errType: 'TypeError' }],
                ['change handler threw', { mode: 'insert', errType: 'Error', errCode: 'MDB_MAP_FULL' }],
                ['change handler threw', { mode: 'insert', errType: 'Error' }],
                ['change handler threw', { mode: 'insert', errType: 'Error' }],
            ])
        })

        it('R-DIAG-01-1 訂閱函數拋出自訂類別之錯誤: 類別名不在白名單者摘要一律列Error(使用端類別名可夾帶資料)', async function() {
            let { calls } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl() })
                class UserDataError extends Error {}
                w.on('change', () => {
                    throw new UserDataError(`c ${ID}`)
                })
                await w.insert({ id: 'c1' })
                await w.close()
            })
            assert.deepStrictEqual(calls.map((c) => c.args), [['change handler threw', { mode: 'insert', errType: 'Error' }]])
        })

    })

    describe('logDetail', function() {

        it('R-DIAG-01-2 logDetail:true 輸出標籤加遮罩後之細節(每次拋錯一次console.log(標籤, 拋出之Error之遮罩拷貝)), 儲存位置遮罩', async function() {
            let { r, calls } = await runSc({ logDetail: true }, true)
            assert.strictEqual(calls.length, golden.bad.calls.length)
            calls.forEach((c, k) => {
                let g = golden.bad.calls[k]
                assert.strictEqual(c.method, 'log', `第${k}行console方法為log(stdout)`)
                assert.strictEqual(c.args.length, 2, `第${k}行引數為(標籤, 細節)`)
                assert.strictEqual(c.args[0], r.evs[k][0] === 'change' ? 'change handler threw' : 'error handler threw', `第${k}行標籤`)
                assert.ok(c.args[1] instanceof Error, `第${k}行細節為Error物件(拷貝)`)
                assert.strictEqual(String(c.args[1]), toMasked(g.args[0].text), `第${k}行文字同修改前(僅儲存位置遮罩)`)
                assert.ok(c.args[1].message.indexOf(ID) >= 0, '細節保留非儲存位置之內容(id)')
                assert.ok(util.inspect(c.args[1]).startsWith(`Error: listener ${ID} ***`), 'util.inspect首行')
            })
            let t = allText(calls)
            assert.ok(t.indexOf(PW) < 0 && t.indexOf(url) < 0, `logDetail輸出含儲存位置: ${t.slice(0, 300)}`)
            assert.strictEqual(r.evs.length, calls.length)
        })

        it('R-DIAG-01-2 logDetail:true 時拋出非Error值亦輸出標籤加細節(字串仍經遮罩)', async function() {
            let { calls } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl(), logDetail: true })
                w.on('change', () => {
                    throw `str ${ID} ${url}`
                })
                await w.insert({ id: 's1' })
                await w.close()
            })
            assert.deepStrictEqual(calls.map((c) => c.args), [['change handler threw', `str ${ID} ***`]])
        })

        it('R-DIAG-01-2 logDetail須為布林true才生效: \'true\'、1、{}、false 皆視為預設(安全輸出, 不拋錯)', async function() {
            for (let v of ['true', 1, {}, false, null]) {
                let { calls } = await runSc({ logDetail: v }, true)
                assert.ok(calls.length > 0)
                assert.ok(calls.every((c) => c.args.length === 2 && c.args[0].endsWith('handler threw')), `logDetail=${JSON.stringify(v)}`)
            }
        })

    })

    describe('funLog', function() {

        it('R-DIAG-01-3 funLog 收(label, safe, detail): console無輸出; safe同預設摘要; detail為Error拷貝(完整細節含id, 儲存位置遮罩)', async function() {
            let got = []
            let { r, calls } = await runSc({ funLog: (...a) => got.push(a) }, true)
            assert.deepStrictEqual(calls, [], 'funLog取代console')
            assert.strictEqual(got.length, r.evs.length)
            got.forEach((a, k) => {
                assert.strictEqual(a.length, 3)
                assert.strictEqual(a[0], r.evs[k][0] === 'change' ? 'change handler threw' : 'error handler threw')
                assert.deepStrictEqual(a[1], { mode: r.evs[k][1], errType: 'Error' })
                assert.ok(a[2] instanceof Error)
                assert.strictEqual(a[2].message, `listener ${ID} ***`)
                assert.ok(String(a[2].stack).indexOf(PW) < 0)
            })
        })

        it('R-DIAG-01-3 funLog 與 logDetail 並給: 只交給funLog, console無輸出', async function() {
            let got = []
            let { calls } = await runSc({ logDetail: true, funLog: (...a) => got.push(a) }, true)
            assert.deepStrictEqual(calls, [])
            assert.ok(got.length > 0)
        })

        it('R-DIAG-01-3 detail為拷貝: 不改動訂閱函數拋出之原物件, 且不是同一參照', async function() {
            let got = []
            let thrown = null
            let { r } = await capAsync(async() => {
                let w = WOrm({ url, db: 'worm', cl: nextCl(), funLog: (...a) => got.push(a) })
                thrown = new Error(`own ${ID} ${url}`)
                thrown.extra = { loc: url }
                w.on('change', () => {
                    throw thrown
                })
                await w.insert({ id: 'o1' })
                await w.close()
                return true
            })
            assert.ok(r)
            assert.strictEqual(got.length, 1)
            assert.notStrictEqual(got[0][2], thrown)
            assert.strictEqual(got[0][2].message, `own ${ID} ***`)
            assert.strictEqual(got[0][2].extra.loc, '***')
            assert.strictEqual(thrown.message, `own ${ID} ${url}`, '原物件不得被遮罩改動')
            assert.strictEqual(thrown.extra.loc, url)
        })

        it('R-DIAG-01-2 相對url: 訂閱函數拋出之訊息含儲存位置(url、url/db/cl)之原樣、絕對路徑與斜線互換形式, 亦遮罩', async function() {
            let urlRel = `./${path.relative(process.cwd(), dirBase).split(path.sep).join('/')}/rel-${PW}@host`
            let cl = nextCl()
            let got = []
            await capAsync(async() => {
                let w = WOrm({ url: urlRel, db: 'worm', cl, funLog: (...a) => got.push(a) })
                w.on('change', () => {
                    throw new Error(`rel ${urlRel} | raw ${urlRel}/worm/${cl} | db ${urlRel}/worm | norm ${path.normalize(urlRel)} | abs ${path.resolve(urlRel)} | all ${path.resolve(`${urlRel}/worm/${cl}`)} | flip ${path.resolve(urlRel).split('\\').join('/')} | id ${ID}`)
                })
                await w.insert({ id: 'r1' })
                await w.close()
            })
            assert.strictEqual(got.length, 1)
            assert.strictEqual(got[0][2].message, `rel *** | raw *** | db *** | norm *** | abs *** | all *** | flip *** | id ${ID}`)
        })

        it('R-DIAG-01-2 url帶尾斜線: 去尾斜線之形式亦遮罩', async function() {
            let urlRel = `./${path.relative(process.cwd(), dirBase).split(path.sep).join('/')}/trail-${PW}@host`
            let got = []
            await capAsync(async() => {
                let w = WOrm({ url: `${urlRel}/`, db: 'worm', cl: nextCl(), funLog: (...a) => got.push(a) })
                w.on('change', () => {
                    throw new Error(`trail ${urlRel} end`)
                })
                await w.insert({ id: 't1' })
                await w.close()
            })
            assert.strictEqual(got.length, 1)
            assert.strictEqual(got[0][2].message, 'trail *** end')
        })

        it('R-DIAG-01-3 funLog 須為函數才生效: 字串、物件、null 視為預設(安全輸出, 不拋錯)', async function() {
            for (let v of ['x', {}, null, 1]) {
                let { calls } = await runSc({ funLog: v }, true)
                assert.ok(calls.length > 0)
                assert.ok(calls.every((c) => c.args.length === 2), `funLog=${JSON.stringify(v)}`)
            }
        })

    })

    describe('選項取值時點與健壯性', function() {

        it('R-DIAG-01-3 選項於建構時取快照: 建構後改寫opt.logDetail與opt.funLog不生效', async function() {
            let got = []
            let opt = { url, db: 'worm', cl: nextCl() }
            let { calls } = await capAsync(async() => {
                let w = WOrm(opt)
                opt.logDetail = true
                opt.funLog = (...a) => got.push(a)
                w.on('change', () => {
                    throw new Error(`snap ${ID}`)
                })
                await w.insert({ id: 'p1' })
                await w.close()
            })
            assert.deepStrictEqual(got, [])
            assert.deepStrictEqual(calls.map((c) => c.args), [['change handler threw', { mode: 'insert', errType: 'Error' }]])
        })

        it('R-DIAG-01-3 讀取選項拋錯(getter)者忽略該選項, 建構不拋錯, 另一個有效選項仍生效', async function() {
            let got = []
            let opt = {
                url,
                db: 'worm',
                cl: nextCl(),
                get logDetail() {
                    throw new Error('getter boom')
                },
                funLog: (...a) => got.push(a),
            }
            let { calls } = await capAsync(async() => {
                let w = WOrm(opt)
                w.on('change', () => {
                    throw new Error(`g ${ID}`)
                })
                await w.insert({ id: 'g1' })
                await w.close()
            })
            assert.deepStrictEqual(calls, [])
            assert.strictEqual(got.length, 1)
        })

        it('R-DIAG-01-4 funLog拋錯、回傳reject之promise: 不影響操作結果, 不成為unhandledRejection, console無輸出', async function() {
            let bad = []
            let fn = (e) => bad.push(e)
            process.on('unhandledRejection', fn)
            try {
                for (let funLog of [() => {
                    throw new Error('funLog boom')
                }, () => Promise.reject(new Error('funLog rej')), () => ({ then: () => Promise.reject(new Error('funLog rej2')) })]) {
                    let { r, calls } = await runSc({ funLog }, true)
                    assert.deepStrictEqual(calls, [])
                    assert.deepStrictEqual(json(r.out), golden.bad.out)
                }
                await new Promise((resolve) => setTimeout(resolve, 50))
            }
            finally {
                process.off('unhandledRejection', fn)
            }
            assert.deepStrictEqual(bad.map((v) => v.message), [])
        })

        it('R-DIAG-01-4 console本身拋錯: 不影響操作結果', async function() {
            let old = console.log
            let r = null
            console.log = () => {
                throw new Error('console broken')
            }
            try {
                r = await scenario(WOrm, { url, db: 'worm', cl: nextCl() }, true)
            }
            finally {
                console.log = old
            }
            assert.deepStrictEqual(json(r.out), golden.bad.out)
        })

    })

    describe('回傳契約不變(對照修改前HEAD產生之固定預期值)', function() {

        let configs = [
            ['預設', {}],
            ['logDetail', { logDetail: true }],
            ['funLog', { funLog: () => {} }],
            ['logDetail與funLog', { logDetail: true, funLog: () => {} }],
            ['型別錯誤之選項', { logDetail: 'x', funLog: 'y' }],
        ]
        for (let [name, optExtra] of configs) {

            it(`R-DIAG-01-6 ${name}: 各操作之resolve與reject內容(含是否為Error實例)、事件payload與事件/結算之時間軸同修改前(訂閱函數不拋錯)`, async function() {
                let { r } = await runSc(optExtra, false)
                assert.deepStrictEqual(json(r.out), golden.plain.out)
                assert.deepStrictEqual(json(r.evs), golden.plain.evs)
                assert.deepStrictEqual(json(r.tl), golden.plain.tl)
            })

            it(`R-DIAG-01-6 ${name}: 訂閱函數拋錯時各操作之結果、事件payload與時間軸仍同修改前`, async function() {
                let { r } = await runSc(optExtra, true)
                assert.deepStrictEqual(json(r.out), golden.bad.out)
                assert.deepStrictEqual(json(r.evs), golden.bad.evs)
                assert.deepStrictEqual(json(r.tl), golden.bad.tl)
            })

        }

        it('R-DIAG-01-6 固定預期值本身: 訂閱函數拋錯與否, 各操作結果相同; 失敗操作確實reject(預期值有鑑別力)', function() {
            assert.deepStrictEqual(golden.plain.out, golden.bad.out)
            assert.deepStrictEqual(golden.plain.tl, golden.bad.tl)
            let errs = golden.plain.out.filter((v) => v.err !== undefined)
            assert.strictEqual(errs.length, 9)
            assert.ok(errs.every((v) => v.isError === true && typeof v.ctor === 'string'), 'reject值皆為Error實例')
            let msgs = errs.map((v) => v.err)
            assert.ok(msgs.some((v) => v.startsWith('can not insertBulk by existed id')))
            assert.ok(msgs.some((v) => v.startsWith('invalid find')))
            assert.ok(msgs.some((v) => v.startsWith('can not insert by closed client')))
            assert.ok(msgs.some((v) => v.startsWith('can not backup by closed client')))
            assert.ok(golden.plain.evs.some((v) => v[0] === 'error') && golden.plain.evs.some((v) => v[0] === 'change'))
            //時間軸: 整批性錯誤之error事件先於該次reject之結算(鎖住事件時機)
            let k = golden.plain.tl.indexOf('settle:3:err')
            assert.strictEqual(golden.plain.tl[k - 1], 'ev:a:error:insertBulk')
            //save逐筆失敗(整批仍resolve): 逐筆error事件、逐筆插入事件、整批change事件依序先於該次結算
            let k9 = golden.plain.tl.indexOf('settle:9:ok')
            assert.deepStrictEqual(golden.plain.tl.slice(k9 - 3, k9), ['ev:a:error:save', 'ev:a:change:insert', 'ev:a:change:save'])
            assert.ok(golden.plain.tl.some((v) => v.startsWith('ev:c:change:')), 'useCache實例之事件亦在時間軸內')
        })

    })

})
