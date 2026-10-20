import assert from 'assert'
import fs from 'fs'
import path from 'path'
import w from 'wsemi'
import { open } from 'lmdb'
import cvStrCases from 'wsemi/src/_cvOrmStrCases.mjs'
import WOrm from '../src/WOrmLmdb.mjs'


//字串良構(帳本R-ORMSTR-01; 規則: C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md〈一之三〉9.)
//共用檢核為wsemi之chkOrmStr、chkOrmData, 契約案例表為wsemi之_cvOrmStrCases(各w-orm-*逐案對公開API執行, 每案全新資料表),
//本檔驗證各入口(insert、insertBulk、save、del、selectByPk與建構)確實呼叫共用檢核並只使用複製後之複本, 而未被後端(msgpack、lmdb鍵編碼)改寫或略過
//lmdb能力: doc=可帶任意欄位名與巢狀結構; 無GridFS(group='gfs'與need含'gfs'者略過)
describe('wellformed', function() {
    let root = './test/_tmp/wellformed'
    let { cases, forms, reasons } = cvStrCases

    //mk, 案例表之文件工廠(夾具: 字串型主鍵欄id與字串欄name)
    let mk = (id, extra) => ({ id, name: 'n', ...extra })

    //msgOf, 取reject值之訊息
    let msgOf = (err) => (err instanceof Error ? err.message : String(err))

    //assertPartial, 取回文件須含期望之各鍵且深度相等(其餘鍵不論)
    let assertPartial = (actual, part) => {
        assert.strict.ok(actual !== null && actual !== undefined, 'row should exist')
        for (let [k, v] of Object.entries(part)) {
            assert.strict.deepStrictEqual(actual[k], v, `row field [${k}]`)
        }
    }

    //withOpt, save之option改為合併指定設定(案例原已有option者合併之, 如autoInsert:false)
    let withOpt = (args, o) => {
        let a = args.slice()
        a[1] = { ...(a[1] || {}), ...o }
        return a
    }

    let seq = 0
    let nSkipGfs = 0
    let nRan = 0

    //freshBackend, 每案全新資料表; 以wo.close之包裝於關閉後刪除該資料夾(每個lmdb環境約260KB, 數千案不刪將累積數百MB)
    let freshBackend = (extraOpt = {}) => {
        seq++
        let cl = `c${seq}`
        let wo = WOrm({ url: root, db: 'worm', cl, ...extraOpt })
        let close = wo.close
        wo.close = async() => {
            await close()
            w.fsDeleteFolder(path.join(root, 'worm', cl))
        }
        return wo
    }

    before(function() {
        w.fsDeleteFolder(root)
    })

    after(function() {
        w.fsDeleteFolder(root)
        assert.strict.ok(!fs.existsSync(root), 'test folder should be removed')
    })

    //runCase, 依案例表檔頭[後端套用方式]逐案執行; caseOpt為建構之額外設定, saveOpt為save之額外option(如merge:false)
    let runCase = async function(ctx, c, caseOpt = {}, saveOpt = null) {

        //caps, lmdb: doc可、gfs無
        if (c.need.includes('gfs')) {
            nSkipGfs++
            return ctx.skip()
        }
        nRan++
        let baseOpt = { url: root, db: 'worm', cl: 'cbase', ...caseOpt }
        let e = c.expect
        let args = c.args(mk, baseOpt)
        if (saveOpt !== null && c.api === 'save') {
            args = withOpt(args, saveOpt)
        }

        //建構之案例: 同步throw, 訊息全等
        if (e.kind === 'throw') {
            assert.throws(() => WOrm(...args), (err) => msgOf(err) === e.msg)
            return
        }

        let wo = freshBackend(caseOpt)
        try {
            await wo.insertBulk(c.pre(mk))

            let before = {}
            for (let id of (e.unchanged || [])) {
                before[id] = await wo.selectByPk(id)
            }

            //evs, 記錄呼叫期間之error事件
            let evs = []
            wo.on('error', (mode, data, err) => evs.push({ mode, err }))

            let r = null
            let rej = null
            try {
                let p = wo[c.api](...args)
                if (c.afterCall) {
                    c.afterCall(args)
                }
                r = await p
            }
            catch (err) {
                rej = err
            }

            if (e.kind === 'free') {
                //值與鍵名之NUL不屬共用檢核: 可接受, 亦可因自身限制拒收, 但拒收訊息不得為共用檢核之詞彙
                let re = new RegExp(e.notMsg)
                if (rej) {
                    assert.strict.ok(!re.test(msgOf(rej)), `free case should not be rejected by shared check: ${msgOf(rej)}`)
                }
                else if (Array.isArray(r)) {
                    for (let x of r) {
                        if (x.ok === 0) {
                            assert.strict.ok(!re.test(String(x.err)), `free case item should not be rejected by shared check: ${x.err}`)
                        }
                    }
                }
            }
            else if (e.kind === 'rejectAll') {
                assert.strict.ok(rej, 'should reject')
                assert.strict.equal(msgOf(rej), e.msg)
            }
            else {
                assert.strict.ok(!rej, `must not reject: ${rej ? msgOf(rej) : ''}`)
                if (e.kind === 'null') {
                    assert.strict.equal(r, null)
                }
                if (e.kind === 'itemErr') {
                    assert.strict.ok(Array.isArray(r), 'result should be array')
                    r.forEach((x, i) => {
                        if (e.bad.includes(i)) {
                            assert.strict.deepStrictEqual([x.ok, x.err], [0, e.msgs ? e.msgs[e.bad.indexOf(i)] : e.msg], `item ${i}`)
                        }
                        else {
                            assert.strict.equal(x.ok, 1, `item ${i} should be ok`)
                        }
                    })
                }
            }
            //error事件(統一規格T10): 整批reject者於reject之前恰一次且訊息全等reject; 逐筆失敗者依序各一次且訊息全等該筆err; 回null與成功者無
            if (e.kind === 'rejectAll') {
                assert.strict.deepStrictEqual(evs, [{ mode: c.api, err: e.msg }], 'error event')
            }
            else if (e.kind === 'itemErr') {
                assert.strict.deepStrictEqual(evs, e.bad.map((v, j) => ({ mode: c.api, err: e.msgs ? e.msgs[j] : e.msg })), 'error events')
            }
            else if (e.kind === 'null' || e.kind === 'ok') {
                assert.strict.deepStrictEqual(evs, [], 'no error event')
            }
            if (e.kind === 'ok' && /^insert/.test(c.api)) {
                assert.strict.equal(r.ok, 1)
            }
            if (e.ids) {
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.id).sort(), e.ids)
            }
            for (let id of (e.unchanged || [])) {
                assert.strict.deepStrictEqual(await wo.selectByPk(id), before[id], `unchanged [${id.length}]`)
            }
            if (e.kind === 'ok' && e.result) {
                assertPartial(r, e.result)
            }
            for (let [id, part] of Object.entries(e.rows || {})) {
                assertPartial(await wo.selectByPk(id), part)
            }
        }
        finally {
            await wo.close()
        }
    }

    //案例分組: 單元層(group='str'與'data')由wsemi自身測試執行, 本套件只跑API層
    let apiCases = cases.filter((v) => v.group !== 'str' && v.group !== 'data')
    let againCases = apiCases.filter((v) => ['id', 'single', 'val', 'order'].includes(v.group))
    let mergeCases = apiCases.filter((v) => v.api === 'save')

    describe('cases(autoGenPk預設)', function() {
        for (let c of apiCases) {
            it(c.id, async function() {
                await runCase(this, c)
            })
        }
    })

    describe('cases(autoGenPk:false; 檢核不隨autoGenPk改變, 規則9.6)', function() {
        for (let c of againCases) {
            it(c.id, async function() {
                await runCase(this, c, { autoGenPk: false })
            })
        }
    })

    describe('cases(save option.merge:false; 後端特有選項路徑)', function() {
        for (let c of mergeCases) {
            it(c.id, async function() {
                await runCase(this, c, {}, { merge: false })
            })
        }
    })

    describe('案例完整度與略過計數', function() {
        it('API層案例皆被執行, 僅need含gfs者(GridFS, 本後端無)略過', function() {
            let all = [...apiCases, ...againCases, ...mergeCases]
            let nGfs = all.filter((v) => v.need.includes('gfs')).length
            assert.strict.ok(nGfs > 0 && nGfs < all.length)
            assert.strict.equal(nSkipGfs, nGfs)
            assert.strict.equal(nRan, all.length - nGfs)
            assert.strict.ok(nRan > 1500, `ran ${nRan}`)
        })
    })

    describe('lmdb特有: 文件鍵名撞鍵', function() {
        //lmdb(msgpack)把鍵名之孤立代理改寫為三個U+FFFD, 與另一鍵名恰為三個U+FFFD者撞鍵, 後寫入者無聲覆蓋前者之值(資料遺失)
        let lone = 'k\uD800'
        let fffd3 = 'k\uFFFD\uFFFD\uFFFD'

        it('insert含孤立鍵名與其改寫後同名之鍵, 整批reject且不寫入', async function() {
            let wo = freshBackend()
            try {
                let doc = { id: 'x', name: 'n', [lone]: 1, [fffd3]: 2 }
                await assert.rejects(() => wo.insert(doc), (err) => msgOf(err) === 'invalid data: string contains lone surrogate at [data[0].<invalid key>]')
                assert.strict.deepStrictEqual(await wo.select(), [])
            }
            finally {
                await wo.close()
            }
        })

        it('save含孤立鍵名與其改寫後同名之鍵, 該筆ok:0且不寫入, 同批其他筆照常', async function() {
            let wo = freshBackend()
            try {
                let r = await wo.save([{ id: 'x', name: 'n', [lone]: 1, [fffd3]: 2 }, { id: 'y', name: 'ok' }])
                assert.strict.deepStrictEqual(r.map((v) => v.ok), [0, 1])
                assert.strict.equal(r[0].err, 'invalid data: string contains lone surrogate at [data[0].<invalid key>]')
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.id), ['y'])
            }
            finally {
                await wo.close()
            }
        })

        it('僅含三個U+FFFD之鍵名(良構)照常存入與讀回, 兩鍵各自保留', async function() {
            let wo = freshBackend()
            try {
                let kFffd = 'k\uFFFD'
                await wo.insert({ id: 'x', [fffd3]: 2, [kFffd]: 3 })
                let d = await wo.selectByPk('x')
                assert.strict.equal(d[fffd3], 2)
                assert.strict.equal(d[kFffd], 3)
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('lmdb特有: name條件之三個U+FFFD不誤中孤立寫入之列', function() {
        //修正前: 孤立name寫入後讀回為三個U+FFFD, {name: FFFD3}會命中該列(靜默改寫之下游碰撞); 修正後該列不存在
        let fffd3 = '\uFFFD\uFFFD\uFFFD'

        it('孤立name被拒, 其後select與delAll之name=FFFD3僅命中良構之列', async function() {
            let wo = freshBackend()
            try {
                await wo.insertBulk([{ id: 'good', name: fffd3 }, { id: 'other', name: 'other' }])
                await assert.rejects(() => wo.insert({ id: 'bad', name: '\uD800' }), (err) => msgOf(err) === 'invalid data: string contains lone surrogate at [data[0].name]')
                let rs = await wo.select({ name: fffd3 })
                assert.strict.deepStrictEqual(rs.map((v) => v.id), ['good'])
                let r = await wo.delAll({ name: fffd3 })
                assert.strict.deepStrictEqual(r, { n: 1, nDeleted: 1, ok: 1 })
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.id), ['other'])
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('lmdb特有: 舊資料(孤立主鍵於修正前寫入, D8不遷移)', function() {
        //以裸lmdb(開啟參數同src/WOrmLmdb.mjs)造修正前寫入之舊列: 鍵為孤立代理字串(CESU-8), 值之id欄於讀回時為三個U+FFFD
        let legacyKey = 'a\uD800'
        let legacyId = 'a\uFFFD\uFFFD\uFFFD'

        //seedLegacy, 先以裸lmdb寫入舊列後關閉, 再以WOrm開啟
        let seedLegacy = async(cl) => {
            let p = path.resolve(root, 'worm', cl)
            let db = open({ path: p, compression: true, useVersions: true })
            await db.put(legacyKey, { id: legacyKey, name: 'legacy' })
            await db.put('plain', { id: 'plain', name: 'plain' })
            await db.put('n\u0000x', { id: 'n\u0000x', name: 'nul' })
            await db.close()
        }

        //closeLegacy, 關閉後刪除該資料夾
        let closeLegacy = async(wo, cl) => {
            await wo.close()
            w.fsDeleteFolder(path.join(root, 'worm', cl))
        }

        it('select仍讀得到舊列(id為三個U+FFFD, 良構), 但selectByPk(讀回之id)為null: 可據此檢出', async function() {
            seq++
            let cl = `legacy${seq}`
            await seedLegacy(cl)
            let wo = WOrm({ url: root, db: 'worm', cl })
            try {
                let rs = await wo.select()
                let row = rs.find((v) => v.name === 'legacy')
                assert.strict.ok(row, 'legacy row should be readable')
                assert.strict.equal(row.id, legacyId)
                assert.strict.equal(row.id.isWellFormed(), true)

                //README之檢出法: 讀回之id無法往返者(selectByPk回null)為孤立主鍵之舊列
                let orphans = []
                for (let r of rs) {
                    if (await wo.selectByPk(r.id) === null) {
                        orphans.push(r.name)
                    }
                }
                assert.strict.deepStrictEqual(orphans.sort(), ['legacy', 'nul'])
            }
            finally {
                await closeLegacy(wo, cl)
            }
        })

        it('以孤立id定址之selectByPk、del被拒(null、逐筆ok:0), 不誤中良構之列', async function() {
            seq++
            let cl = `legacy${seq}`
            await seedLegacy(cl)
            let wo = WOrm({ url: root, db: 'worm', cl })
            try {
                assert.strict.equal(await wo.selectByPk(legacyKey), null)
                let r = await wo.del([{ id: legacyKey }, { id: 'plain' }])
                assert.strict.deepStrictEqual(r.map((v) => v.ok), [0, 1])
                assert.strict.equal(r[0].err, 'invalid id: string contains lone surrogate at [data[0].id]')
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.name).sort(), ['legacy', 'nul'])

                //含NUL之舊主鍵同樣被拒(D2): selectByPk回null、del逐筆ok:0, 舊列留存
                assert.strict.equal(await wo.selectByPk('n\u0000x'), null)
                let rn = await wo.del({ id: 'n\u0000x' })
                assert.strict.equal(rn[0].err, 'invalid id: string contains NUL at [data[0].id]')
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.name).sort(), ['legacy', 'nul'])
            }
            finally {
                await closeLegacy(wo, cl)
            }
        })

        it('清除路徑: delAll({id:讀回之id})或依他欄之delAll可刪除舊列', async function() {
            seq++
            let cl = `legacy${seq}`
            await seedLegacy(cl)
            let wo = WOrm({ url: root, db: 'worm', cl })
            try {
                let r = await wo.delAll({ id: legacyId })
                assert.strict.deepStrictEqual(r, { n: 1, nDeleted: 1, ok: 1 })
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.name).sort(), ['nul', 'plain'])
            }
            finally {
                await closeLegacy(wo, cl)
            }
        })
    })

    describe('事件、時序與呼叫形式', function() {
        let H = 'a\uD800'

        it('insert/insertBulk整批reject: error事件於reject前恰一次, 訊息同reject, 不發change', async function() {
            for (let fn of ['insert', 'insertBulk']) {
                let wo = freshBackend()
                try {
                    let evs = []
                    wo.on('error', (mode, data, err) => evs.push({ t: 'error', mode, err }))
                    wo.on('change', (mode) => evs.push({ t: 'change', mode }))
                    let rej = null
                    try {
                        await wo[fn]([mk('g0'), mk(H)])
                    }
                    catch (err) {
                        rej = err
                    }
                    assert.strict.ok(rej)
                    assert.strict.deepStrictEqual(evs, [{ t: 'error', mode: fn, err: 'invalid id: string contains lone surrogate at [data[1].id]' }], fn)
                    assert.strict.equal(msgOf(rej), 'invalid id: string contains lone surrogate at [data[1].id]')
                    assert.strict.deepStrictEqual(await wo.select(), [])
                }
                finally {
                    await wo.close()
                }
            }
        })

        it('insert之returnList:true遇壞列: 仍整批reject(不回逐筆清單), error事件恰一次', async function() {
            let wo = freshBackend()
            try {
                let evs = []
                wo.on('error', (mode, data, err) => evs.push({ mode, err }))
                await assert.rejects(() => wo.insert([mk('g0'), mk(H)], { returnList: true }), (err) => msgOf(err) === 'invalid id: string contains lone surrogate at [data[1].id]')
                assert.strict.deepStrictEqual(evs, [{ mode: 'insert', err: 'invalid id: string contains lone surrogate at [data[1].id]' }])
                assert.strict.deepStrictEqual(await wo.select(), [])
            }
            finally {
                await wo.close()
            }
        })

        it('良構檢核先於鍵長檢核: 主鍵同時含孤立代理與超長者回報invalid id(整批reject), 單獨超長者仍回鍵長訊息', async function() {
            let wo = freshBackend()
            try {
                await assert.rejects(() => wo.insert({ id: `${H}${'x'.repeat(2500)}`, name: 'n' }), (err) => msgOf(err) === 'invalid id: string contains lone surrogate at [data[0].id]')
                await assert.rejects(() => wo.insert({ id: 'x'.repeat(2500), name: 'n' }), (err) => /Key size is larger than the maximum key size/.test(msgOf(err)))
            }
            finally {
                await wo.close()
            }
        })

        it('save/del逐筆: 各壞筆error事件一次(訊息含該筆索引), 整批定案後發出, 其餘筆照常', async function() {
            let wo = freshBackend()
            try {
                await wo.insertBulk([mk('k1')])
                let evs = []
                wo.on('error', (mode, data, err) => evs.push({ t: 'error', mode, err }))
                wo.on('change', (mode) => evs.push({ t: 'change', mode }))
                let rs = await wo.save([mk(H, { name: 'x' }), mk('k1', { name: 'y' }), mk('b\uDC00', { name: 'z' })])
                assert.strict.deepStrictEqual(rs.map((v) => v.ok), [0, 1, 0])
                assert.strict.deepStrictEqual(evs.filter((v) => v.t === 'error').map((v) => v.err), [
                    'invalid id: string contains lone surrogate at [data[0].id]',
                    'invalid id: string contains lone surrogate at [data[2].id]',
                ])
                assert.strict.deepStrictEqual(evs.map((v) => v.t), ['error', 'error', 'change'])
                evs.length = 0
                let rd = await wo.del([{ id: H }, { id: 'k1' }, { id: 'b\uDC00' }])
                assert.strict.deepStrictEqual(rd.map((v) => v.ok), [0, 1, 0])
                assert.strict.deepStrictEqual(evs.filter((v) => v.t === 'error').map((v) => v.err), [
                    'invalid id: string contains lone surrogate at [data[0].id]',
                    'invalid id: string contains lone surrogate at [data[2].id]',
                ])
            }
            finally {
                await wo.close()
            }
        })

        it('selectByPk孤立或含NUL之主鍵回null且不發error事件', async function() {
            let wo = freshBackend()
            try {
                let n = 0
                wo.on('error', () => n++)
                assert.strict.equal(await wo.selectByPk(H), null)
                assert.strict.equal(await wo.selectByPk('a\u0000b'), null)
                assert.strict.equal(n, 0)
            }
            finally {
                await wo.close()
            }
        })

        it('已關閉之實例: 孤立主鍵仍回closed訊息(isClosed先於檢核)', async function() {
            let wo = freshBackend()
            await wo.close()
            for (let fn of ['insert', 'insertBulk', 'save', 'del']) {
                await assert.rejects(() => wo[fn]([mk(H)]), (err) => /closed/.test(msgOf(err)) && !/invalid id/.test(msgOf(err)), fn)
            }
            await assert.rejects(() => wo.selectByPk(H), (err) => /closed/.test(msgOf(err)))
        })

        it('建構時孤立代理於同步throw, 且不建立任何資料夾', function() {
            let url = `${root}/ctor-nocreate`
            assert.throws(() => WOrm({ url, db: 'd\uD800', cl: 'c' }), (err) => msgOf(err) === 'invalid opt.db: string contains lone surrogate')
            assert.throws(() => WOrm({ url, db: 'd', cl: 'c\uDC00' }), (err) => msgOf(err) === 'invalid opt.cl: string contains lone surrogate')
            assert.throws(() => WOrm({ url: `${url}\uD800`, db: 'd', cl: 'c' }), (err) => msgOf(err) === 'invalid opt.url: string contains lone surrogate')
            assert.strict.equal(fs.existsSync(url), false)
        })

        it('autoGenPk為true時未帶有效id之筆先補主鍵再檢核: 照常寫入; 其資料不合格者仍拒收(訊息為資料路徑)', async function() {
            for (let fn of ['insert', 'insertBulk', 'save']) {
                let wo = freshBackend()
                try {
                    let r = await wo[fn]([{ name: 'a' }, { id: '', name: 'b' }, { id: 12345, name: 'c' }])
                    if (fn === 'save') {
                        assert.strict.deepStrictEqual(r.map((v) => v.ok), [1, 1, 1], fn)
                    }
                    else {
                        assert.strict.equal(r.ok, 1, fn)
                    }
                    let rows = await wo.select()
                    assert.strict.equal(rows.length, 3, fn)
                    assert.strict.deepStrictEqual(rows.map((v) => v.name).sort(), ['a', 'b', 'c'], fn)
                    for (let x of rows) {
                        assert.strict.ok(typeof x.id === 'string' && x.id !== '' && x.id.isWellFormed(), fn)
                    }

                    //資料不合格: 主鍵已補(良構), 故訊息為資料路徑
                    if (fn === 'save') {
                        let rs = await wo.save([{ name: 'x\uD800' }])
                        assert.strict.equal(rs[0].err, 'invalid data: string contains lone surrogate at [data[0].name]', fn)
                    }
                    else {
                        await assert.rejects(() => wo[fn]([{ name: 'ok' }, { name: 'x\uD800' }]), (err) => msgOf(err) === 'invalid data: string contains lone surrogate at [data[1].name]', fn)
                    }
                }
                finally {
                    await wo.close()
                }
            }
        })

        it('單一物件(非陣列)呼叫之批次索引為data[0]', async function() {
            let wo = freshBackend()
            try {
                await assert.rejects(() => wo.insert(mk(H)), (err) => msgOf(err) === 'invalid id: string contains lone surrogate at [data[0].id]')
                let rs = await wo.save(mk(H))
                assert.strict.equal(rs[0].err, 'invalid id: string contains lone surrogate at [data[0].id]')
                let rd = await wo.del(mk(H))
                assert.strict.equal(rd[0].err, 'invalid id: string contains lone surrogate at [data[0].id]')
            }
            finally {
                await wo.close()
            }
        })

        it('del之非字串或空主鍵之訊息不回顯值, 含批次索引', async function() {
            let wo = freshBackend()
            try {
                let rd = await wo.del([{ id: 123456 }, { id: '' }, { name: 'noid' }, { id: { secret: 'S3CR3T' } }])
                assert.strict.deepStrictEqual(rd.map((v) => v.ok), [0, 0, 0, 0])
                for (let x of rd) {
                    assert.strict.ok(!/123456|S3CR3T|noid/.test(x.err), x.err)
                    assert.strict.ok(/^invalid id: .+ at \[data\[\d\]\.id\]$/.test(x.err), x.err)
                }
            }
            finally {
                await wo.close()
            }
        })

        it('良構之emoji、CJK擴充B區、U+FFFD主鍵與鍵名照常往返, 且與孤立形態之鄰近列互不影響', async function() {
            let wo = freshBackend()
            try {
                let okIds = ['a\u{1F600}', '\u{20BB7}野', 'a\uFFFD', 'a\uFFFD\uFFFD\uFFFD']
                await wo.insertBulk(okIds.map((id) => ({ id, name: id, [id]: id })))
                for (let id of okIds) {
                    let d = await wo.selectByPk(id)
                    assert.strict.deepStrictEqual(d, { id, name: id, [id]: id })
                }
                let rs = await wo.del([{ id: 'a\uD800' }, { id: 'a\u{1F600}' }])
                assert.strict.deepStrictEqual(rs.map((v) => v.ok), [0, 1])
                assert.strict.deepStrictEqual((await wo.select()).map((v) => v.id).sort(), ['a\uFFFD', 'a\uFFFD\uFFFD\uFFFD', '\u{20BB7}野'].sort())
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('lmdb特有: save合併路徑之繼承存取器(檢查之值即寫入之值)', function() {
        //類別實例(原型非Object.prototype)上之可列舉存取器: cloneDeep保留原型故仍在複本上, lodash merge(keysIn)於合併時讀取之;
        //存取器每次讀取可回不同值, 故合併後之最終物件(實際寫入者)須再檢核一次
        let mkInh = (id, vals) => {
            let n = 0
            let proto = {
                get flip() {
                    let v = vals[Math.min(n, vals.length - 1)]
                    n++
                    return v
                }
            }
            let d = Object.create(proto)
            d.id = id
            d.name = 'n'
            return d
        }

        it('繼承存取器首次讀回良構值、其後讀回孤立代理: 該筆ok:0附err, 原記錄不變', async function() {
            let wo = freshBackend()
            try {
                await wo.insert({ id: 'g1', name: 'pre' })
                let r = await wo.save([mkInh('g1', ['ok', 'bad\uD800'])])
                assert.strict.deepStrictEqual(r.map((v) => v.ok), [0])
                assert.strict.equal(r[0].err, 'invalid data: string contains lone surrogate at [data[0].flip]')
                assert.strict.deepStrictEqual(await wo.selectByPk('g1'), { id: 'g1', name: 'pre' })
            }
            finally {
                await wo.close()
            }
        })

        it('繼承存取器自第k次讀取起回孤立代理(k=1..6): 寫入者不得為孤立代理(該筆拒收且原記錄不變, 或寫入檢核所讀之良構值)', async function() {
            let nRej = 0
            let nOk = 0
            for (let kBad = 1; kBad <= 6; kBad++) {
                let wo = freshBackend()
                try {
                    await wo.insert({ id: 'g1', name: 'pre' })
                    let n = 0
                    let proto = {
                        get flip() {
                            n++
                            return n >= kBad ? 'bad\uD800' : 'ok'
                        }
                    }
                    let d = Object.create(proto)
                    d.id = 'g1'
                    d.name = 'n'
                    let r = await wo.save([d])
                    let row = await wo.selectByPk('g1')
                    if (r[0].ok === 0) {
                        nRej++
                        assert.strict.equal(r[0].err, 'invalid data: string contains lone surrogate at [data[0].flip]', `k=${kBad}`)
                        assert.strict.deepStrictEqual(row, { id: 'g1', name: 'pre' }, `k=${kBad}`)
                    }
                    else {
                        nOk++
                        assert.strict.equal(row.flip, 'ok', `k=${kBad}`)
                    }
                }
                finally {
                    await wo.close()
                }
            }
            assert.strict.ok(nRej >= 1 && nOk >= 1, `both outcomes should occur: rejected ${nRej}, written ${nOk}`)
        })

        it('merge:false覆寫路徑: 繼承存取器之值不被編碼寫入(msgpack只編碼自有屬性, 本路徑不再檢核之前提), 寫入者不含該屬性且良構', async function() {
            let wo = freshBackend()
            try {
                await wo.insert({ id: 'g1', name: 'pre' })
                for (let vals of [['ok'], ['bad\uD800'], ['ok', 'bad\uD800']]) {
                    let r = await wo.save([mkInh('g1', vals)], { merge: false })
                    let row = await wo.selectByPk('g1')
                    if (r[0].ok === 0) {
                        //首次讀取即為孤立代理者於預先檢核被拒
                        assert.strict.equal(r[0].err, 'invalid data: string contains lone surrogate at [data[0].flip]')
                    }
                    else {
                        assert.strict.deepStrictEqual(row, { id: 'g1', name: 'n' })
                    }
                }
            }
            finally {
                await wo.close()
            }
        })

        it('繼承存取器恆回良構值: 照常合併存入(既有行為不變)', async function() {
            let wo = freshBackend()
            try {
                await wo.insert({ id: 'g1', name: 'pre' })
                let r = await wo.save([mkInh('g1', ['ok'])])
                assert.strict.deepStrictEqual(r.map((v) => v.ok), [1])
                assert.strict.deepStrictEqual(await wo.selectByPk('g1'), { id: 'g1', name: 'n', flip: 'ok' })
            }
            finally {
                await wo.close()
            }
        })

        it('繼承存取器於檢核時即回孤立代理: 該筆ok:0附err(檢核先於讀取與寫入)', async function() {
            let wo = freshBackend()
            try {
                await wo.insert({ id: 'g1', name: 'pre' })
                let r = await wo.save([mkInh('g1', ['bad\uD800'])])
                assert.strict.deepStrictEqual(r.map((v) => v.ok), [0])
                assert.strict.equal(r[0].err, 'invalid data: string contains lone surrogate at [data[0].flip]')
                assert.strict.deepStrictEqual(await wo.selectByPk('g1'), { id: 'g1', name: 'pre' })
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('已知限制(帳本記載)', function() {
        it('String包裝物件含成對代理: 本批不解包、不拒收(良構字串值), 行為如其既有', async function() {
            let wo = freshBackend()
            try {
                //良構之包裝物件不被共用檢核拒收(已知限制⑧: 讀回形態屬儲存層既有行為, 本批不改)
                let r = await wo.insert({ id: 'w', s: new String('abc') }) // eslint-disable-line no-new-wrappers
                assert.strict.equal(r.ok, 1)
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('案例表契約', function() {
        it('forms與reasons詞彙存在(案例表契約)', function() {
            assert.strict.ok(Object.keys(forms).length >= 33)
            assert.strict.deepStrictEqual(Object.keys(reasons).sort(), ['lone', 'notStr', 'nul'])
        })
    })
})
