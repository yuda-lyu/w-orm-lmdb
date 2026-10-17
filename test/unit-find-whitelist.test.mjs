import assert from 'assert'
import path from 'path'
import fs from 'fs'
import { spawnSync } from 'child_process'
import w from 'wsemi'
import { open } from 'lmdb'
import cvCases from 'wsemi/src/_cvOrmFindCases.mjs'
import cvOrmFind from 'wsemi/src/cvOrmFind.mjs'
import WOrm from '../src/WOrmLmdb.mjs'


//條件(find)之系列白名單(帳本R-FIND-01; 規則: C:\opensrcOper\dspec\w-orm系列條件語法支援盤點.md〈一之三〉)
//共用檢核為wsemi之cvOrmFind, 契約案例表為wsemi之_cvOrmFindCases(各w-orm-*逐案對公開select與delAll執行, 每案以seed()重建),
//本檔驗證select與delAll確實於入口呼叫並只使用檢核後之複本, 而未被後端(mingo)略過
//lmdb屬MongoDB式族, 命中集合取案例之ids.mongo; 本套件之$in上限即共用預設(10000), 故案例之need不使本套件reject
describe('find-whitelist', function() {
    let { seed, cases } = cvCases
    let url = './test/_tmp/find-whitelist'
    let dbName = 'worm'

    //ids, 取id集合(升冪)
    let ids = (rs) => rs.map((v) => v.id).sort()

    //tick, 讓出一個macrotask, 令延遲發出之事件有機會發出
    let tick = () => new Promise((resolve) => setTimeout(resolve, 0))

    //directMsg, 共用檢核對同一條件直接給出之訊息(後端須原樣給出, 不得改寫、包裝或另立訊息)
    let directMsg = (f) => {
        try {
            cvOrmFind(f)
        }
        catch (err) {
            return err.message
        }
        return null
    }

    //mkObserver, 依事件與結果建立一次呼叫之觀測器: 事件記錄於evs, 每筆error事件附帶發出當下呼叫端之await是否已恢復(settledAtEmit, 即呼叫端可觀察之先後)
    let mkObserver = (wo) => {
        let evs = []
        let cur = { settled: false }
        wo.on('error', function(mode, data, err) {
            evs.push({ t: 'error', mode, data, err, settledAtEmit: cur.settled })
        })
        wo.on('change', function(mode) {
            evs.push({ t: 'change', mode })
        })

        //invoke, 呼叫wo[fn](arg)並回傳{ ok, val, err, nAtSettle, evs }, nAtSettle為結果定案當下之事件數, evs為再讓出一個macrotask後之事件
        let invoke = async(fn, arg) => {
            evs.length = 0
            cur = { settled: false }
            let out = { ok: false, val: null, err: null, nAtSettle: 0, evs: [] }
            try {
                out.val = await wo[fn](arg)
                out.ok = true
            }
            catch (err) {
                out.err = err
            }
            cur.settled = true
            out.nAtSettle = evs.length
            await tick()
            out.evs = evs.slice()
            return out
        }

        //clear
        let clear = () => {
            evs.length = 0
        }

        return { invoke, clear }
    }

    //reset, 清空後以seed()重建
    let reset = async(wo) => {
        await wo.delAll()
        await wo.insertBulk(seed())
    }

    //chkRej, 整批reject之共同斷言(WL-R1、WL-R3): 訊息以invalid find開頭且逐字等於共用檢核給出之訊息(expectMsg), error事件於reject前恰發出一次(訊息同reject), change不發
    let chkRej = (out, fn, expectMsg = null) => {
        assert.strict.equal(out.ok, false, `${fn} should reject`)
        assert.strict.ok(out.err instanceof Error, `${fn} should reject with Error`)
        assert.strict.ok(out.err.message.indexOf('invalid find: ') === 0, `${fn} message should start with 'invalid find: ' but got: ${out.err.message}`)
        if (expectMsg !== null) {
            assert.strict.equal(out.err.message, expectMsg, `${fn} message should be the one of cvOrmFind`)
        }
        let evErr = out.evs.filter((v) => v.t === 'error')
        let evChg = out.evs.filter((v) => v.t === 'change' && v.mode === fn) //只計本函數之change(同時排入之他函數可能發出自己的change)
        assert.strict.equal(evErr.length, 1, `${fn} should emit error exactly once`)
        assert.strict.equal(out.nAtSettle, 1, `${fn} should emit error before reject`)
        assert.strict.equal(evErr[0].settledAtEmit, false, `${fn} should emit error before reject`)
        assert.strict.equal(evErr[0].mode, fn)
        assert.strict.equal(evErr[0].data, null)
        assert.strict.equal(evErr[0].err, out.err.message, `${fn} error event should carry the reject message`)
        assert.strict.equal(evChg.length, 0, `${fn} should not emit change`)
    }

    describe('cases', function() {
        let wo = null
        let ob = null
        let seedIds = ids(seed())
        let seedRows = null

        before(async function() {
            w.fsDeleteFolder(url)
            wo = WOrm({ url, db: dbName, cl: 'cases' })
            ob = mkObserver(wo)
            await reset(wo)
            seedRows = await wo.select()
            seedRows.sort((a, b) => (a.id < b.id ? -1 : 1))
        })

        after(async function() {
            await wo.close()
            w.fsDeleteFolder(url)
        })

        //案例表本身: 防案例表被替換為空表或缺組別而使下列逐案全綠成為假象
        it('should get nonempty case table covering all groups with seed of 6 rows', function() {
            let groups = [...new Set(cases.map((c) => c.group))].sort().join('')
            assert.strict.equal(groups, 'CEFHILNTVX')
            assert.strict.ok(cases.length >= 241)
            assert.strict.deepStrictEqual(seedIds, ['a1', 'a2', 'a3', 'a4', 'a5', 'a6'])
        })

        for (let c of cases) {

            if (c.expect === 'REJ') {

                //REJ案: select與delAll皆整批reject, 資料表不變(WL-02、WL-07等; 對應案例表expect=REJ)
                it(`should reject select and delAll without any change for ${c.id} ${c.label}`, async function() {
                    await reset(wo)
                    ob.clear()

                    let expectMsg = directMsg(c.find())
                    assert.strict.notEqual(expectMsg, null, 'cvOrmFind should reject the case')
                    let outS = await ob.invoke('select', c.find())
                    chkRej(outS, 'select', expectMsg)
                    let outD = await ob.invoke('delAll', c.find())
                    chkRej(outD, 'delAll', expectMsg)

                    //資料不變
                    let rows = await wo.select()
                    rows.sort((a, b) => (a.id < b.id ? -1 : 1))
                    assert.strict.deepStrictEqual(rows, seedRows)
                })

            }
            else {

                //ALL與OK案: select命中ids.mongo; delAll刪除之集合等於select命中之集合(系列最低要求①), n與nDeleted為實際刪除筆數
                it(`should select ${JSON.stringify(c.ids.mongo)} and delAll the same set for ${c.id} ${c.label}`, async function() {
                    await reset(wo)
                    ob.clear()

                    let outS = await ob.invoke('select', c.find())
                    assert.strict.equal(outS.ok, true, `select should resolve but got: ${outS.err}`)
                    let sel = ids(outS.val)
                    assert.strict.deepStrictEqual(sel, c.ids.mongo)
                    assert.strict.equal(outS.evs.filter((v) => v.t === 'error').length, 0)

                    await reset(wo)
                    ob.clear()
                    let outD = await ob.invoke('delAll', c.find())
                    assert.strict.equal(outD.ok, true, `delAll should resolve but got: ${outD.err}`)
                    assert.strict.deepStrictEqual(outD.val, { n: sel.length, nDeleted: sel.length, ok: 1 })
                    assert.strict.equal(outD.evs.filter((v) => v.t === 'error').length, 0)

                    let rest = ids(await wo.select())
                    let deleted = seedIds.filter((id) => rest.indexOf(id) < 0)
                    assert.strict.deepStrictEqual(deleted, sel)
                })

            }

        }

    })

    //檢查之值即使用之值(WL-H4): 檢核回傳之複本即後續查詢與刪除所用之條件, 不得再讀呼叫端之原物件
    //以Proxy令欄位g於第1次讀取回'A'(檢核所讀)、其後回'B', 若後端改讀原物件(mingo以[]取值)則命中集合改變
    describe('copy', function() {
        let wo = null
        let ob = null

        before(async function() {
            w.fsDeleteFolder(`${url}-copy`)
            wo = WOrm({ url: `${url}-copy`, db: dbName, cl: 'copy' })
            ob = mkObserver(wo)
            await reset(wo)
        })

        after(async function() {
            await wo.close()
            w.fsDeleteFolder(`${url}-copy`)
        })

        //mkFlip, 欄位g第1次被讀取(不論以屬性描述子或get)回'A', 其後回'B'; 同時回傳讀取次數
        let mkFlip = () => {
            let st = { n: 0 }
            let val = () => {
                st.n++
                return st.n === 1 ? 'A' : 'B'
            }
            let px = new Proxy({ g: 'A' }, {
                getOwnPropertyDescriptor(t, k) {
                    let d = Reflect.getOwnPropertyDescriptor(t, k)
                    if (k === 'g') {
                        d = { ...d, value: val() }
                    }
                    return d
                },
                get(t, k, r) {
                    if (k === 'g') {
                        return val()
                    }
                    return Reflect.get(t, k, r)
                },
            })
            return { px, st }
        }

        let shapes = {
            top: (px) => px,
            nested: (px) => ({ $or: [px] }),
        }

        for (let name of Object.keys(shapes)) {

            it(`should select by the checked value only for ${name} condition`, async function() {
                await reset(wo)
                let { px, st } = mkFlip()
                let r = await wo.select(shapes[name](px))
                assert.strict.deepStrictEqual(ids(r), ['a1', 'a2'])
                assert.strict.equal(st.n, 1, 'value should be read once')
            })

            it(`should delAll by the checked value only for ${name} condition`, async function() {
                await reset(wo)
                let { px, st } = mkFlip()
                let r = await wo.delAll(shapes[name](px))
                assert.strict.deepStrictEqual(r, { n: 2, nDeleted: 2, ok: 1 })
                assert.strict.deepStrictEqual(ids(await wo.select()), ['a3', 'a4', 'a5', 'a6'])
                assert.strict.equal(st.n, 1, 'value should be read once')
            })

        }

        //熱快取(useCache已填入)時亦只用檢核後之複本: 原快取測試只以壞條件與穩定條件驗快取, 守不住「合法條件改讀原物件」
        for (let fn of ['select', 'delAll']) {
            it(`should use the checked copy only with warm cache for ${fn}`, async function() {
                w.fsDeleteFolder(`${url}-warm`)
                let wc = WOrm({ url: `${url}-warm`, db: dbName, cl: `warm${fn}`, useCache: true })
                await wc.insertBulk(seed())
                assert.strict.equal((await wc.select()).length, 6) //填入快取
                let { px, st } = mkFlip()
                if (fn === 'select') {
                    assert.strict.deepStrictEqual(ids(await wc.select(px)), ['a1', 'a2'])
                }
                else {
                    assert.strict.deepStrictEqual(await wc.delAll(px), { n: 2, nDeleted: 2, ok: 1 })
                    assert.strict.deepStrictEqual(ids(await wc.select()), ['a3', 'a4', 'a5', 'a6'])
                }
                assert.strict.equal(st.n, 1, 'value should be read once')
                await wc.close()
                w.fsDeleteFolder(`${url}-warm`)
            })
        }

        //「是否有條件」之判斷亦須依檢核後之複本: ownKeys於第1次回['g']、其後回[](若改讀原物件, select會回全部、delAll會刪光)
        for (let fn of ['select', 'delAll']) {
            it(`should decide the empty condition by the checked copy only for ${fn}`, async function() {
                await reset(wo)
                let n = 0
                let px = new Proxy({ g: 'A' }, {
                    ownKeys() {
                        n++
                        return n === 1 ? ['g'] : []
                    },
                })
                if (fn === 'select') {
                    assert.strict.deepStrictEqual(ids(await wo.select(px)), ['a1', 'a2'])
                }
                else {
                    assert.strict.deepStrictEqual(await wo.delAll(px), { n: 2, nDeleted: 2, ok: 1 })
                    assert.strict.deepStrictEqual(ids(await wo.select()), ['a3', 'a4', 'a5', 'a6'])
                }
                assert.strict.equal(n, 1, 'ownKeys should be read once')
            })
        }

        //運算子物件內之值亦只讀一次並只用複本: {age:Proxy({$gt:20})}, $gt第1次被讀取回20、其後回100(若改讀原物件, 命中集合改變)
        for (let fn of ['select', 'delAll']) {
            it(`should read the value in operator object once for ${fn}`, async function() {
                await reset(wo)
                let n = 0
                let val = () => (++n === 1 ? 20 : 100)
                let op = new Proxy({ $gt: 20 }, {
                    getOwnPropertyDescriptor(t, k) {
                        let d = Reflect.getOwnPropertyDescriptor(t, k)
                        return k === '$gt' ? { ...d, value: val() } : d
                    },
                    get(t, k, r) {
                        return k === '$gt' ? val() : Reflect.get(t, k, r)
                    },
                })
                if (fn === 'select') {
                    assert.strict.deepStrictEqual(ids(await wo.select({ age: op })), ['a2', 'a3', 'a5'])
                }
                else {
                    assert.strict.deepStrictEqual(await wo.delAll({ age: op }), { n: 3, nDeleted: 3, ok: 1 })
                    assert.strict.deepStrictEqual(ids(await wo.select()), ['a1', 'a4', 'a6'])
                }
                assert.strict.equal(n, 1, 'value should be read once')
            })
        }

        //呼叫端之條件物件不被改動(複本與原物件脫勾)
        it('should not modify the find object of caller', async function() {
            await reset(wo)
            let find = { $and: [{ g: 'A' }, { age: { $in: [20, 30] } }], d: new Date(Date.UTC(2026, 0, 1)) }
            let snap = JSON.stringify(find)
            let d0 = find.d.getTime()
            let r = await wo.select(find)
            assert.strict.deepStrictEqual(ids(r), ['a1'])
            await wo.delAll(find)
            assert.strict.equal(JSON.stringify(find), snap)
            assert.strict.equal(find.d.getTime(), d0)
            assert.strict.ok(Object.getPrototypeOf(find) === Object.prototype)
        })

        //select與delAll對同一條件之判定一致: 同一不合法條件兩者皆reject; 同一合法條件兩者命中同一集合(逐案已驗, 此處以非空巢狀條件再驗一次)
        it('should delAll exactly what select returns for nested condition', async function() {
            await reset(wo)
            let find = { $or: [{ $and: [{ g: 'B' }, { age: { $gte: 40 } }] }, { id: { $in: ['a1'] } }] }
            let sel = ids(await wo.select(find))
            assert.strict.deepStrictEqual(sel, ['a1', 'a3', 'a5'])
            let r = await wo.delAll(find)
            assert.strict.deepStrictEqual(r, { n: 3, nDeleted: 3, ok: 1 })
            assert.strict.deepStrictEqual(ids(await wo.select()), ['a2', 'a4', 'a6'])
            ob.clear()
        })

    })

    //欄名同於Object.prototype成員(WL-F2: constructor、toString、hasOwnProperty、valueOf等為白名單內之合法欄名):
    //數據無此自有欄位者須與一般不存在之欄位同為缺欄(null語意), 不得取到繼承之成員(修正前mingo取到函數, {constructor:null}無命中、{constructor:{$ne:null}}命中全部而delAll刪光)
    describe('protoname', function() {
        let wo = null

        //reset, 種子6列(皆無自有constructor、toString等欄)另加p1(自有constructor與toString欄)
        let resetP = async() => {
            await reset(wo)
            await wo.insert({ id: 'p1', constructor: 'own', toString: 'ts' })
        }
        let A6 = ['a1', 'a2', 'a3', 'a4', 'a5', 'a6']

        before(async function() {
            w.fsDeleteFolder(`${url}-proto`)
            wo = WOrm({ url: `${url}-proto`, db: dbName, cl: 'proto' })
        })

        after(async function() {
            await wo.close()
            w.fsDeleteFolder(`${url}-proto`)
        })

        let finds = [
            { name: '{constructor:null}', find: () => ({ constructor: null }), ids: A6 },
            { name: '{constructor:{$eq:null}}', find: () => ({ constructor: { $eq: null } }), ids: A6 },
            { name: '{constructor:{$ne:null}}', find: () => ({ constructor: { $ne: null } }), ids: ['p1'] },
            { name: '{constructor:"own"}', find: () => ({ constructor: 'own' }), ids: ['p1'] },
            { name: '{toString:{$ne:null}}', find: () => ({ toString: { $ne: null } }), ids: ['p1'] },
            { name: '{toString:null}', find: () => ({ toString: null }), ids: A6 },
            { name: '{hasOwnProperty:{$ne:null}}', find: () => ({ hasOwnProperty: { $ne: null } }), ids: [] },
            { name: '{valueOf:null}', find: () => ({ valueOf: null }), ids: [...A6, 'p1'] },
            { name: '{valueOf:{$in:["x"]}}', find: () => ({ valueOf: { $in: ['x'] } }), ids: [] },
            { name: '{valueOf:{$nin:["x"]}}', find: () => ({ valueOf: { $nin: ['x'] } }), ids: [...A6, 'p1'] },
            { name: '$or', find: () => ({ $or: [{ constructor: 'own' }, { id: 'a1' }] }), ids: ['a1', 'p1'] },
            { name: '$and', find: () => ({ $and: [{ toString: null }, { id: 'a2' }] }), ids: ['a2'] },
            { name: 'nested $or in $and', find: () => ({ $and: [{ $or: [{ hasOwnProperty: { $ne: null } }, { id: 'a3' }] }] }), ids: ['a3'] },
            { name: 'with ordinary field', find: () => ({ g: 'A', constructor: null }), ids: ['a1', 'a2'] },
        ]
        for (let f of finds) {
            it(`should select and delAll by ${f.name} as missing-field semantics`, async function() {
                await resetP()
                assert.strict.deepStrictEqual(ids(await wo.select(f.find())), f.ids)
                await resetP()
                let r = await wo.delAll(f.find())
                assert.strict.deepStrictEqual(r, { n: f.ids.length, nDeleted: f.ids.length, ok: 1 })
                let rest = ids(await wo.select())
                assert.strict.deepStrictEqual([...A6, 'p1'].filter((id) => rest.indexOf(id) < 0), f.ids)
            })
        }

        //對照組: 一般不存在之欄位(全部列皆缺欄)之語意, 原型成員欄名須與之同(數據無自有欄位者皆缺欄)
        it('should get the same result for an ordinary missing field', async function() {
            await resetP()
            assert.strict.deepStrictEqual(ids(await wo.select({ nocolumn: null })), [...A6, 'p1'])
            assert.strict.deepStrictEqual(ids(await wo.select({ nocolumn: { $ne: null } })), [])
            assert.strict.deepStrictEqual(ids(await wo.select({ nocolumn: { $nin: ['x'] } })), [...A6, 'p1'])
            assert.strict.deepStrictEqual(ids(await wo.select({ nocolumn: { $in: ['x'] } })), [])
        })

    })

    //Date(WL-S1): lmdb原生保存Date, 條件內之Date不轉換即可比對, 於各運算子與邏輯位置皆須命中(與level、lowdb轉為ISO字串後之命中集合相同)
    describe('date', function() {
        let wo = null
        let D1 = () => new Date(Date.UTC(2026, 0, 1))
        let D2 = () => new Date(Date.UTC(2026, 1, 1))

        before(async function() {
            w.fsDeleteFolder(`${url}-date`)
            wo = WOrm({ url: `${url}-date`, db: dbName, cl: 'date' })
            await reset(wo)
        })

        after(async function() {
            await wo.close()
            w.fsDeleteFolder(`${url}-date`)
        })

        it('should get native Date in stored rows', async function() {
            await reset(wo)
            let a1 = await wo.selectByPk('a1')
            assert.strict.ok(a1.d instanceof Date)
            assert.strict.equal(a1.d.getTime(), D1().getTime())
        })

        let finds = [
            { name: '$eq', find: () => ({ d: { $eq: D1() } }), ids: ['a1'] },
            { name: '$ne', find: () => ({ d: { $ne: D1() } }), ids: ['a2', 'a3', 'a4', 'a5', 'a6'] },
            { name: '$gt', find: () => ({ d: { $gt: D1() } }), ids: ['a2'] },
            { name: '$lt', find: () => ({ d: { $lt: D2() } }), ids: ['a1'] },
            { name: '$lte', find: () => ({ d: { $lte: D2() } }), ids: ['a1', 'a2'] },
            { name: '$in', find: () => ({ d: { $in: [D2()] } }), ids: ['a2'] },
            { name: '$nin', find: () => ({ d: { $nin: [D1(), D2()] } }), ids: ['a3', 'a4', 'a5', 'a6'] },
            { name: '$or', find: () => ({ $or: [{ d: D2() }, { id: 'a3' }] }), ids: ['a2', 'a3'] },
            { name: '$and', find: () => ({ $and: [{ d: { $gte: D1() } }, { d: { $lte: D1() } }] }), ids: ['a1'] },
            { name: 'range', find: () => ({ d: { $gte: D1(), $lte: D2() } }), ids: ['a1', 'a2'] },
        ]
        for (let f of finds) {
            it(`should select and delAll by Date in ${f.name}`, async function() {
                await reset(wo)
                assert.strict.deepStrictEqual(ids(await wo.select(f.find())), f.ids)
                let r = await wo.delAll(f.find())
                assert.strict.deepStrictEqual(r, { n: f.ids.length, nDeleted: f.ids.length, ok: 1 })
                let rest = ids(await wo.select())
                assert.strict.deepStrictEqual(['a1', 'a2', 'a3', 'a4', 'a5', 'a6'].filter((id) => rest.indexOf(id) < 0), f.ids)
            })
        }

    })

    //時序(R-CLOSE-01規則1: 入口順序為closed→find檢核→讀取); 本套件無serialize佇列, 故無level、lowdb之「檢核置於佇列之外」斷言
    describe('order', function() {
        let bad = () => ({ $where: () => true })

        //已關閉先於find檢核: 以壞條件呼叫已關閉之實例, 訊息須為closed而非invalid find
        it('should reject with closed message before find check for closed instance', async function() {
            w.fsDeleteFolder(`${url}-closed`)
            let wo = WOrm({ url: `${url}-closed`, db: dbName, cl: 'closed' })
            await wo.insert({ id: 'x1', g: 'A' })
            await wo.close()
            let ob = mkObserver(wo)
            let rs = []
            for (let fn of ['select', 'delAll']) {
                let out = await ob.invoke(fn, bad())
                assert.strict.equal(out.ok, false)
                assert.strict.equal(out.err.message, `can not ${fn} by closed client, instance can not be used after close()`)
                let evErr = out.evs.filter((v) => v.t === 'error')
                assert.strict.equal(evErr.length, 1)
                assert.strict.equal(evErr[0].err, out.err.message)
                rs.push(out.err.message)
            }
            assert.strict.equal(rs.length, 2)
            w.fsDeleteFolder(`${url}-closed`)
        })

        //find檢核先於讀取: 資料庫內有無法解碼之紀錄(任何讀取皆拋解碼錯誤)時, 壞條件仍須回invalid find(未讀取), 合法條件則因讀取而回解碼錯誤
        it('should check find before reading the database', async function() {
            let urlBad = `${url}-unreadable`
            w.fsDeleteFolder(urlBad)
            let storage = path.resolve(`${urlBad}/${dbName}/un`)
            fs.mkdirSync(path.dirname(storage), { recursive: true })
            let raw = open({ path: storage, compression: true, useVersions: true, encoding: 'binary' })
            await raw.put('bad', Buffer.from([0xd9, 0x50, 0x41]))
            await raw.close()

            let wo = WOrm({ url: urlBad, db: dbName, cl: 'un' })
            let ob = mkObserver(wo)

            //對照: 合法條件須讀取資料庫, 因解碼錯誤而reject, 且訊息不是invalid find
            for (let fn of ['select', 'delAll']) {
                let out = await ob.invoke(fn, { g: 'A' })
                assert.strict.equal(out.ok, false, `${fn} with valid find should reject by unreadable record`)
                assert.strict.ok(out.err.message.indexOf('invalid find') < 0, `${fn} valid find should not get invalid find: ${out.err.message}`)
            }

            //壞條件: 於讀取之前reject
            for (let fn of ['select', 'delAll']) {
                for (let f of [bad(), null, [], { g: undefined }, { $and: [] }]) {
                    let out = await ob.invoke(fn, f)
                    chkRej(out, fn)
                }
            }

            await wo.close()
            w.fsDeleteFolder(urlBad)
        })

        //useCache: 快取已填入時壞條件仍須reject, 快取與資料不受影響
        it('should reject bad find with filled cache and keep data', async function() {
            let urlC = `${url}-cache`
            w.fsDeleteFolder(urlC)
            let wo = WOrm({ url: urlC, db: dbName, cl: 'cache', useCache: true })
            let ob = mkObserver(wo)
            await wo.insertBulk(seed())
            assert.strict.equal((await wo.select()).length, 6)
            for (let fn of ['select', 'delAll']) {
                for (let f of [bad(), null, [], { g: undefined }, { g: { $nin: [] } }]) {
                    let out = await ob.invoke(fn, f)
                    chkRej(out, fn, directMsg(f))
                }
            }
            assert.strict.deepStrictEqual(ids(await wo.select()), ['a1', 'a2', 'a3', 'a4', 'a5', 'a6'])
            assert.strict.deepStrictEqual(ids(await wo.select({ g: 'A' })), ['a1', 'a2'])
            await wo.close()
            w.fsDeleteFolder(urlC)
        })

        //關閉進行中(close()已呼叫而尚未await, status為closing)亦屬已關閉: 以壞條件呼叫須回closed訊息, 不得回invalid find
        it('should reject with closed message before find check while closing', async function() {
            let urlG = `${url}-closing`
            w.fsDeleteFolder(urlG)
            for (let fn of ['select', 'delAll']) {
                let wo = WOrm({ url: urlG, db: dbName, cl: `closing${fn}` })
                await wo.insert({ id: 'x1', g: 'A' })
                let ob = mkObserver(wo)
                let pmClose = wo.close()
                let out = await ob.invoke(fn, bad())
                assert.strict.equal(out.ok, false)
                assert.strict.equal(out.err.message, `can not ${fn} by closed client, instance can not be used after close()`)
                await pmClose
            }
            w.fsDeleteFolder(urlG)
        })

        //檢核途中實例被關閉(條件內之Proxy trap呼叫close後拋錯): 兩函數皆須保留檢核產生之invalid find, 不得被改寫為closed訊息, 且select與delAll對稱
        it('should keep invalid find when instance is closed during find check', async function() {
            let urlD = `${url}-closeduring`
            w.fsDeleteFolder(urlD)
            for (let fn of ['select', 'delAll']) {
                let wo = WOrm({ url: urlD, db: dbName, cl: `during${fn}` })
                await wo.insert({ id: 'x1', g: 'A' })
                let ob = mkObserver(wo)
                let pmClose = null
                let mk = () => new Proxy({ g: 'A' }, {
                    ownKeys() {
                        pmClose = wo.close()
                        throw new Error('PRIVATE_TRAP_ERROR')
                    },
                })
                let out = await ob.invoke(fn, mk())
                chkRej(out, fn)
                assert.strict.ok(out.err.message.indexOf('PRIVATE_TRAP_ERROR') < 0, 'message should not echo the error of caller')
                assert.strict.ok(out.err.message.indexOf('closed') < 0, 'message should not be rewritten as closed')
                await pmClose
            }
            w.fsDeleteFolder(urlD)
        })

        //delAll壞條件reject後立即close不崩潰(接續R-WRITE-02之子程序工具), 且資料不變
        it('should exit normally when close right after delAll rejected by bad find', function() {
            let urlP = `${url}-proc`
            w.fsDeleteFolder(urlP)
            let r = spawnSync(process.execPath, ['./test/tools/closeAfterReject.mjs', 'delAllBadFind', urlP], { encoding: 'utf8' })
            assert.strict.equal(r.status, 0, r.stderr)
            assert.strict.ok(r.stdout.indexOf('end') >= 0)
            assert.strict.ok(r.stdout.indexOf('rest=3') >= 0, r.stdout)
            w.fsDeleteFolder(urlP)
        })

    })

})
