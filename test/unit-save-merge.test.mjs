import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


//規格出處: 建議w-orm-lmdb修正.md §2、§3, 帳本R-SAVE-01(save之option.merge)
//merge僅嚴格等於false者為整筆覆寫(同一寫交易內完成, 不經刪除), 其餘維持現行深層合併
//標記: [鎖定]=預設(合併)路徑之既有行為, 修前即綠; 其餘為merge:false新行為, 修前紅

let url = './_db_save_merge'
let seq = 0


//withWo, 每案用獨立資料表, 結束必close
async function withWo(fn, opt = {}) {
    seq++
    let wo = WOrm({ url, db: 'worm', cl: `c${seq}`, ...opt })
    try {
        return await fn(wo)
    }
    finally {
        await wo.close()
    }
}

//settle, 把reject轉為可斷言之值
async function settle(pm) {
    return pm
        .then(function(msg) {
            return { ok: true, msg }
        })
        .catch(function(msg) {
            return { ok: false, msg: _.isString(msg) ? msg : String(_.get(msg, 'message', msg)) }
        })
}

//watch, 收集change與error事件
function watch(wo) {
    let evs = []
    wo.on('change', function(mode, data, res) {
        evs.push({ ev: 'change', mode, data, res })
    })
    wo.on('error', function(mode, data, err) {
        evs.push({ ev: 'error', mode, data, err })
    })
    return evs
}

//resOK, 逐筆結果鍵集合固定
let r = (n, nInserted, nModified) => {
    return { n, nInserted, nModified, ok: 1 }
}


describe('save-merge', function() {

    before(function() {
        w.fsDeleteFolder(url)
    })

    describe('overwrite whole record in one transaction', function() {

        //建議書§3: 刪鍵不殘留舊值
        it('should drop keys absent from the new record by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a1', a: 1, b: 2, c: 3 })
                let rs = await wo.save({ id: 'a1', a: 9 }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('a1'), { id: 'a1', a: 9 })
                assert.strict.deepStrictEqual(await wo.select(), [{ id: 'a1', a: 9 }])
            })
        })

        //建議書§3: 陣列欄位整筆替換不殘留舊元素(合併路徑依索引逐元素合併而殘留)
        it('should replace array fields as a whole by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a2', tags: ['x', 'y', 'z'], rows: [{ k: 1, j: 1 }, { k: 2 }] })
                let rs = await wo.save({ id: 'a2', tags: ['q'], rows: [{ k: 9 }] }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('a2'), { id: 'a2', tags: ['q'], rows: [{ k: 9 }] })
            })
        })

        it('should replace array field by empty array by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a3', tags: ['x', 'y'] })
                await wo.save({ id: 'a3', tags: [] }, { merge: false })
                assert.strict.deepStrictEqual(await wo.selectByPk('a3'), { id: 'a3', tags: [] })
            })
        })

        it('should not merge nested object deeply by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a4', o: { a: 1, b: 2, c: { d: 3, e: 4 } } })
                await wo.save({ id: 'a4', o: { a: 5, c: { d: 6 } } }, { merge: false })
                assert.strict.deepStrictEqual(await wo.selectByPk('a4'), { id: 'a4', o: { a: 5, c: { d: 6 } } })
            })
        })

        it('should store exactly the input with new keys and changed values by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a5', a: 1, b: 2 })
                let v = { id: 'a5', b: 20, n: 'new', o: { p: [1, 2] } }
                let rs = await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('a5'), v)
            })
        })

        it('should keep only id when input has only id by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a6', a: 1, b: [1, 2] })
                let rs = await wo.save({ id: 'a6' }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('a6'), { id: 'a6' })
            })
        })

        it('should not modify the input object by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'a7', a: 1 })
                let v = { id: 'a7', o: { p: [1, 2] } }
                let vc = _.cloneDeep(v)
                await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(v, vc)
                v.o.p.push(3)
                assert.strict.deepStrictEqual(await wo.selectByPk('a7'), { id: 'a7', o: { p: [1, 2] } })
            })
        })

        it('should overwrite every row of an array input with order kept by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert([{ id: 'm1', a: 1, b: 1 }, { id: 'm2', a: 2, b: 2 }, { id: 'm3', a: 3, b: 3 }])
                let rs = await wo.save([{ id: 'm3', c: 3 }, { id: 'm1', c: 1 }, { id: 'm2', a: 2, b: 2 }], { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1), r(1, 0, 1), r(1, 0, 0)])
                assert.strict.deepStrictEqual(_.sortBy(await wo.select(), 'id'), [{ id: 'm1', c: 1 }, { id: 'm2', a: 2, b: 2 }, { id: 'm3', c: 3 }])
            })
        })

        it('should apply rows with same id in input order and keep only the last whole content by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 's1', old: 1 })
                let rs = await wo.save([{ id: 's1', first: 1, shared: 'f' }, { id: 's1', second: 2 }], { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1), r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('s1'), { id: 's1', second: 2 })
            })
        })

        it('should insert then overwrite rows with same id when id not existed by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let rs = await wo.save([{ id: 's2', first: 1 }, { id: 's2', second: 2 }], { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0), r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('s2'), { id: 's2', second: 2 })
            })
        })

    })

    describe('same content judgement follows the merge mode', function() {

        //規則3: 覆寫以[待寫入物件與現值相同]判定, 輸入為現值之子集(刪鍵)不得被當成無須寫入
        it('should write when input is a subset of current record with same values by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'b1', a: 1, b: 2 })
                let rs = await wo.save({ id: 'b1', a: 1 }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('b1'), { id: 'b1', a: 1 })
            })
        })

        it('should not write and get nModified 0 when input equals current record by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'b2', a: 1, b: [1, 2], o: { p: 1 } })
                let rs = await wo.save({ id: 'b2', a: 1, b: [1, 2], o: { p: 1 } }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 0)])
                assert.strict.deepStrictEqual(await wo.selectByPk('b2'), { id: 'b2', a: 1, b: [1, 2], o: { p: 1 } })
            })
        })

        it('should get nModified 1 then 0 for the same input saved twice by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'b3', a: 1, b: 2 })
                let rs1 = await wo.save({ id: 'b3', a: 5 }, { merge: false })
                let rs2 = await wo.save({ id: 'b3', a: 5 }, { merge: false })
                assert.strict.deepStrictEqual(rs1, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(rs2, [r(1, 0, 0)])
                assert.strict.deepStrictEqual(await wo.selectByPk('b3'), { id: 'b3', a: 5 })
            })
        })

        //[鎖定] 預設合併: 子集且值相同合併後等同現值, 不寫入且保留其餘欄位
        it('[lock] should keep merge semantic when option is omitted', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'b4', a: 1, b: 2, tags: ['x', 'y', 'z'] })
                let rs1 = await wo.save({ id: 'b4', a: 1 })
                assert.strict.deepStrictEqual(rs1, [r(1, 0, 0)])
                let rs2 = await wo.save({ id: 'b4', tags: ['q'] })
                assert.strict.deepStrictEqual(rs2, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('b4'), { id: 'b4', a: 1, b: 2, tags: ['q', 'y', 'z'] })
            })
        })

        //[鎖定] merge:true與預設等價; 非布林值不啟用覆寫(僅嚴格false啟用)
        for (let [name, mv] of [['true', true], ['undefined', undefined], ['null', null], ['0', 0], ['empty string', ''], ['string false', 'false'], ['NaN', NaN]]) {
            it(`[lock] should keep merge semantic when merge is ${name}`, async function() {
                await withWo(async(wo) => {
                    await wo.insert({ id: 'b5', a: 1, b: 2, tags: ['x', 'y'] })
                    let rs = await wo.save({ id: 'b5', a: 9, tags: ['q'] }, { merge: mv })
                    assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                    assert.strict.deepStrictEqual(await wo.selectByPk('b5'), { id: 'b5', a: 9, b: 2, tags: ['q', 'y'] })
                })
            })
        }

        it('[lock] should keep merge semantic when option has other keys only or is empty', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'b6', a: 1, b: 2 })
                await wo.save({ id: 'b6', a: 9 }, { autoInsert: false })
                await wo.save({ id: 'b6', c: 3 }, {})
                assert.strict.deepStrictEqual(await wo.selectByPk('b6'), { id: 'b6', a: 9, b: 2, c: 3 })
            })
        })

        //option本身為null或非物件, 不得throw且維持合併(與merge:null是不同的格)
        for (let [name, ov] of [['null', null], ['string', 'x'], ['number', 5], ['boolean false', false], ['array', [false]]]) {
            it(`[lock] should keep merge semantic when option itself is ${name}`, async function() {
                await withWo(async(wo) => {
                    await wo.insert({ id: 'b7', a: 1, b: 2, tags: ['x', 'y'] })
                    let rs = await wo.save({ id: 'b7', a: 9, tags: ['q'] }, ov)
                    assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                    assert.strict.deepStrictEqual(await wo.selectByPk('b7'), { id: 'b7', a: 9, b: 2, tags: ['q', 'y'] })
                })
            })
        }

    })

    describe('combination with autoInsert', function() {

        //建議書§3: 不存在之id依autoInsert
        it('should insert whole input when id not existed by save(merge:false) and autoInsert omitted', async function() {
            await withWo(async(wo) => {
                let v = { id: 'c1', a: 1, b: [1, 2] }
                let rs = await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0)])
                assert.strict.deepStrictEqual(await wo.selectByPk('c1'), v)
            })
        })

        it('should insert whole input when id not existed by save(merge:false, autoInsert:true)', async function() {
            await withWo(async(wo) => {
                let v = { id: 'c2', a: 1 }
                let rs = await wo.save(v, { merge: false, autoInsert: true })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0)])
                assert.strict.deepStrictEqual(await wo.selectByPk('c2'), v)
            })
        })

        it('should not write when id not existed by save(merge:false, autoInsert:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'c3-other', z: 1 })
                let rs = await wo.save({ id: 'c3', a: 1 }, { merge: false, autoInsert: false })
                assert.strict.deepStrictEqual(rs, [r(0, 0, 0)])
                assert.strict.deepStrictEqual(await wo.selectByPk('c3'), null)
                assert.strict.deepStrictEqual(await wo.select(), [{ id: 'c3-other', z: 1 }])
            })
        })

        it('should still overwrite existed id by save(merge:false, autoInsert:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'c4', a: 1, b: 2 })
                let rs = await wo.save({ id: 'c4', a: 5 }, { merge: false, autoInsert: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('c4'), { id: 'c4', a: 5 })
            })
        })

        it('should keep order of per-row results for mixed existed and not existed rows by save(merge:false, autoInsert:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert([{ id: 'c5a', a: 1, b: 1 }, { id: 'c5c', a: 3, b: 3 }])
                let rs = await wo.save([{ id: 'c5a', a: 9 }, { id: 'c5b', a: 2 }, { id: 'c5c', a: 3, b: 3 }], { merge: false, autoInsert: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1), r(0, 0, 0), r(1, 0, 0)])
                assert.strict.deepStrictEqual(_.sortBy(await wo.select(), 'id'), [{ id: 'c5a', a: 9 }, { id: 'c5c', a: 3, b: 3 }])
            })
        })

        it('should keep order of per-row results for mixed rows by save(merge:false, autoInsert:true)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'c6a', a: 1, b: 1 })
                let rs = await wo.save([{ id: 'c6b', a: 2 }, { id: 'c6a', a: 9 }], { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0), r(1, 0, 1)])
                assert.strict.deepStrictEqual(_.sortBy(await wo.select(), 'id'), [{ id: 'c6a', a: 9 }, { id: 'c6b', a: 2 }])
            })
        })

        it('should treat key longer than limit as not existed by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let idLong = 'k'.repeat(2000)
                let rsT = await wo.save({ id: idLong, a: 1 }, { merge: false })
                assert.strict.deepStrictEqual(rsT.length, 1)
                assert.strict.deepStrictEqual(_.pick(rsT[0], ['n', 'nInserted', 'nModified', 'ok']), { n: 1, nInserted: 0, nModified: 0, ok: 0 })
                assert.strict.deepStrictEqual(_.isString(rsT[0].err) && _.includes(rsT[0].err, 'Key size is larger than the maximum key size'), true)
                let rsF = await wo.save({ id: idLong, a: 1 }, { merge: false, autoInsert: false })
                assert.strict.deepStrictEqual(rsF, [r(0, 0, 0)])
                assert.strict.deepStrictEqual(await wo.select(), [])
            })
        })

        it('should generate id and insert when row has no id and autoGenPk is true by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let rs = await wo.save({ a: 1 }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0)])
                let all = await wo.select()
                assert.strict.deepStrictEqual(all.length, 1)
                assert.strict.deepStrictEqual(_.omit(all[0], ['id']), { a: 1 })
                assert.strict.deepStrictEqual(_.isString(all[0].id) && all[0].id.length > 0, true)
            })
        })

    })

    describe('failure keeps the original record', function() {

        //建議書§3: 寫入失敗(無法序列化)時原記錄仍在; 無獨立之刪除步驟故不會先刪後失敗
        for (let [name, mk] of [['Symbol', () => Symbol('x')], ['BigInt over 64 bits', () => 2n ** 70n]]) {

            it(`should keep original record when new record has ${name} by save(merge:false)`, async function() {
                await withWo(async(wo) => {
                    let evs = watch(wo)
                    let orig = { id: 'd1', a: 1, b: [1, 2], o: { p: 1 } }
                    await wo.insert(orig)
                    evs.length = 0
                    let rs = await wo.save({ id: 'd1', a: 2, bad: mk() }, { merge: false })
                    assert.strict.deepStrictEqual(rs.length, 1)
                    assert.strict.deepStrictEqual(_.pick(rs[0], ['n', 'nInserted', 'nModified', 'ok']), { n: 1, nInserted: 0, nModified: 0, ok: 0 })
                    assert.strict.deepStrictEqual(_.isString(rs[0].err) && rs[0].err.length > 0, true)
                    assert.strict.deepStrictEqual(await wo.selectByPk('d1'), orig)
                    assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error', 'change'])
                    assert.strict.deepStrictEqual(evs[0].mode, 'save')
                    assert.strict.deepStrictEqual(evs[0].err, rs[0].err)
                })
            })

        }

        it('should fail only the bad row and keep order by save(merge:false) for bad row at first, middle and last', async function() {
            for (let pos of [0, 1, 2]) {
                await withWo(async(wo) => {
                    let evs = watch(wo)
                    await wo.insert([{ id: 'e0', a: 0, x: 0 }, { id: 'e1', a: 1, x: 1 }, { id: 'e2', a: 2, x: 2 }])
                    evs.length = 0
                    let rows = _.times(3, function(k) {
                        return k === pos ? { id: `e${k}`, a: 99, bad: Symbol('x') } : { id: `e${k}`, a: 99 }
                    })
                    let rs = await wo.save(rows, { merge: false })
                    assert.strict.deepStrictEqual(_.map(rs, 'ok'), _.times(3, (k) => (k === pos ? 0 : 1)))
                    assert.strict.deepStrictEqual(_.map(rs, 'nModified'), _.times(3, (k) => (k === pos ? 0 : 1)))
                    let all = _.sortBy(await wo.select(), 'id')
                    assert.strict.deepStrictEqual(all, _.times(3, (k) => (k === pos ? { id: `e${k}`, a: k, x: k } : { id: `e${k}`, a: 99 })))
                    assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error', 'change'])
                })
            }
        })

        it('should neither insert nor emit insert event when new row is unwritable by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let evs = watch(wo)
                let rs = await wo.save({ id: 'd2', bad: Symbol('x') }, { merge: false })
                assert.strict.deepStrictEqual(_.pick(rs[0], ['n', 'nInserted', 'nModified', 'ok']), { n: 1, nInserted: 0, nModified: 0, ok: 0 })
                assert.strict.deepStrictEqual(await wo.selectByPk('d2'), null)
                assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error', 'change'])
                assert.strict.deepStrictEqual(_.map(evs, 'mode'), ['save', 'save'])
            })
        })

        it('should get same results with and without error listener by save(merge:false)', async function() {
            let f = async(withListener) => {
                return withWo(async(wo) => {
                    if (withListener) {
                        watch(wo)
                    }
                    await wo.insert({ id: 'd3', a: 1 })
                    return wo.save([{ id: 'd3', a: 2, bad: Symbol('x') }, { id: 'd3', b: 2 }, { id: 'd4', c: 1 }], { merge: false })
                })
            }
            let rsA = await f(true)
            let rsB = await f(false)
            assert.strict.deepStrictEqual(rsA, rsB)
            assert.strict.deepStrictEqual(_.map(rsA, 'ok'), [0, 1, 1])
        })

    })

    describe('batch level errors are the same as merge path', function() {

        for (let [name, opt] of [['merge:false', { merge: false }], ['default', {}]]) {

            it(`should reject without writing any row when an element is not an object by save(${name})`, async function() {
                await withWo(async(wo) => {
                    let evs = watch(wo)
                    let rt = await settle(wo.save([{ id: 'f1', a: 1 }, 'str'], opt))
                    assert.strict.deepStrictEqual(rt, { ok: false, msg: 'can not save by data[1] which is not an object' })
                    assert.strict.deepStrictEqual(await wo.select(), [])
                    assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error'])
                })
            })

            it(`should reject without writing any row when id missing and autoGenPk is false by save(${name})`, async function() {
                await withWo(async(wo) => {
                    let evs = watch(wo)
                    let rt = await settle(wo.save([{ id: 'f2', a: 1 }, { a: 2 }], opt))
                    assert.strict.deepStrictEqual(rt, { ok: false, msg: 'can not save by data[1] without valid id when autoGenPk is false' })
                    assert.strict.deepStrictEqual(await wo.select(), [])
                    assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error'])
                }, { autoGenPk: false })
            })

            it(`should return empty array for invalid input by save(${name})`, async function() {
                await withWo(async(wo) => {
                    assert.strict.deepStrictEqual(await wo.save('x', opt), [])
                    assert.strict.deepStrictEqual(await wo.save(null, opt), [])
                    assert.strict.deepStrictEqual(await wo.save(123, opt), [])
                })
            })

            it(`should reject with closed message after close by save(${name})`, async function() {
                seq++
                let wo = WOrm({ url, db: 'worm', cl: `c${seq}` })
                await wo.insert({ id: 'f3', a: 1 })
                await wo.close()
                let rt = await settle(wo.save({ id: 'f3', a: 2 }, opt))
                assert.strict.deepStrictEqual(rt.ok, false)
                assert.strict.deepStrictEqual(_.includes(rt.msg, 'closed'), true)
            })

        }

    })

    describe('events', function() {

        it('should emit only one change(save) when overwriting existed record by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'g1', a: 1, b: 2 })
                let evs = watch(wo)
                let v = { id: 'g1', a: 5 }
                let rs = await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['change'])
                assert.strict.deepStrictEqual(evs[0].mode, 'save')
                assert.strict.deepStrictEqual(evs[0].data, [v])
                assert.strict.deepStrictEqual(evs[0].res, rs)
            })
        })

        it('should emit change(insert) before change(save) when inserting by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let evs = watch(wo)
                let v = { id: 'g2', a: 1 }
                let rs = await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(_.map(evs, 'mode'), ['insert', 'save'])
                assert.strict.deepStrictEqual(evs[0].data, [v])
                assert.strict.deepStrictEqual(evs[0].res, rs[0])
                assert.strict.deepStrictEqual(evs[1].data, [v])
                assert.strict.deepStrictEqual(evs[1].res, rs)
            })
        })

        it('should emit change(save) only when nothing is written by save(merge:false, autoInsert:false)', async function() {
            await withWo(async(wo) => {
                let evs = watch(wo)
                await wo.save({ id: 'g3', a: 1 }, { merge: false, autoInsert: false })
                assert.strict.deepStrictEqual(_.map(evs, 'mode'), ['save'])
            })
        })

        it('should not change returned value when listener rewrites res by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'g4', a: 1 })
                wo.on('change', function(mode, data, res) {
                    _.each(res, (v) => {
                        v.n = 999
                    })
                })
                let rs = await wo.save({ id: 'g4', a: 2 }, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
            })
        })

    })

    describe('event timing and batch level error of option read', function() {

        //逐筆事件待整批定案後才發出(T10.1第1條): 訂閱者於第1筆逐筆事件中close實例, 不得使其後各筆中止
        it('should emit per-row events only after the whole batch is settled by save(merge:false)', async function() {
            seq++
            let cl = `c${seq}`
            let wo = WOrm({ url, db: 'worm', cl })
            await wo.insert([{ id: 'ev1', a: 1, b: 1 }, { id: 'ev2', a: 2, b: 2 }])
            let evs = watch(wo)
            wo.on('error', function() {
                wo.close()
            })
            let rs = await wo.save([{ id: 'ev1', a: 9, bad: Symbol('x') }, { id: 'ev2', a: 9 }], { merge: false })
            assert.strict.deepStrictEqual(_.map(rs, 'ok'), [0, 1])
            assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error', 'change'])
            await wo.close()
            let wo2 = WOrm({ url, db: 'worm', cl })
            try {
                assert.strict.deepStrictEqual(_.sortBy(await wo2.select(), 'id'), [{ id: 'ev1', a: 1, b: 1 }, { id: 'ev2', a: 9 }])
            }
            finally {
                await wo2.close()
            }
        })

        //整批性錯誤: option.merge取值拋錯(如getter), 依T10.3先發出error事件(恰一次)再reject, 不寫入、不發change
        it('should emit error once before reject and write nothing when reading option.merge throws', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'og1', old: 1 })
                let evs = watch(wo)
                let option = {
                    get merge() {
                        throw new Error('merge getter failed')
                    },
                }
                let rt = await settle(wo.save({ id: 'og1', a: 2 }, option))
                assert.strict.deepStrictEqual(rt, { ok: false, msg: 'merge getter failed' })
                assert.strict.deepStrictEqual(_.map(evs, 'ev'), ['error'])
                assert.strict.deepStrictEqual(evs[0].mode, 'save')
                assert.strict.deepStrictEqual(evs[0].err, 'merge getter failed')
                assert.strict.deepStrictEqual(await wo.selectByPk('og1'), { id: 'og1', old: 1 })
            })
        })

        it('should apply option.merge returned by getter', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'og2', old: 1, keep: 2 })
                let option = {
                    get merge() {
                        return false
                    },
                }
                let rs = await wo.save({ id: 'og2', a: 2 }, option)
                assert.strict.deepStrictEqual(rs, [r(1, 0, 1)])
                assert.strict.deepStrictEqual(await wo.selectByPk('og2'), { id: 'og2', a: 2 })
            })
        })

    })

    describe('input isolation', function() {

        //與外部脫勾: 呼叫後、await前修改輸入, 存入者仍為呼叫當下之內容
        it('should store the content at call time when the input is modified before awaiting by save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'is1', a: 1 })
                let v = { id: 'is1', o: { p: [1, 2] }, n: 1 }
                let pm = wo.save(v, { merge: false })
                v.o.p.push(3)
                v.n = 99
                v.extra = true
                await pm
                assert.strict.deepStrictEqual(await wo.selectByPk('is1'), { id: 'is1', o: { p: [1, 2] }, n: 1 })
            })
        })

        it('should store the content at call time for rows to be inserted when the input is modified before awaiting by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let rows = [{ id: 'is2', n: 1 }, { id: 'is3', n: 2 }]
                let pm = wo.save(rows, { merge: false })
                rows[0].n = 99
                rows[1].n = 99
                await pm
                assert.strict.deepStrictEqual(_.sortBy(await wo.select(), 'id'), [{ id: 'is2', n: 1 }, { id: 'is3', n: 2 }])
            })
        })

        //補值於複本上進行, 呼叫端之輸入物件不得被加上id
        it('should not add generated id to the input object by save(merge:false)', async function() {
            await withWo(async(wo) => {
                let v = { a: 1 }
                let rs = await wo.save(v, { merge: false })
                assert.strict.deepStrictEqual(rs, [r(1, 1, 0)])
                assert.strict.deepStrictEqual(v, { a: 1 })
                assert.strict.deepStrictEqual(_.size(await wo.select()), 1)
            })
        })

    })

    describe('cache', function() {

        it('should reflect overwritten record in select after save(merge:false) with useCache', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'h1', a: 1, b: 2 })
                assert.strict.deepStrictEqual(await wo.select(), [{ id: 'h1', a: 1, b: 2 }])
                await wo.save({ id: 'h1', a: 5 }, { merge: false })
                assert.strict.deepStrictEqual(await wo.select(), [{ id: 'h1', a: 5 }])
                assert.strict.deepStrictEqual(await wo.select({ id: 'h1' }), [{ id: 'h1', a: 5 }])
            }, { useCache: true })
        })

    })

    describe('atomicity', function() {

        //20個併發整筆覆寫同一id, 終值須恰為其中一筆之完整內容, 不混入他筆之鍵
        it('should end with exactly one whole record after 20 concurrent save(merge:false) on same id', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'i1', old: 1 })
                let rss = await Promise.all(_.times(20, function(k) {
                    return wo.save({ id: 'i1', who: k, [`f${k}`]: k }, { merge: false })
                }))
                let v = await wo.selectByPk('i1')
                assert.strict.deepStrictEqual(_.keys(v).sort(), ['id', 'who', `f${v.who}`].sort())
                assert.strict.deepStrictEqual(v[`f${v.who}`], v.who)
                assert.strict.deepStrictEqual(_.every(_.flatten(rss), (x) => x.ok === 1 && x.nInserted === 0), true)
            })
        })

        //同id全新, 併發整筆覆寫之autoInsert僅一次
        it('should insert only once for 10 concurrent save(merge:false) on a new id', async function() {
            await withWo(async(wo) => {
                let rss = await Promise.all(_.times(10, function(k) {
                    return wo.save({ id: 'i2', who: k }, { merge: false })
                }))
                let rs = _.flatten(rss)
                assert.strict.deepStrictEqual(_.size(_.filter(rs, (x) => x.nInserted === 1)), 1)
                assert.strict.deepStrictEqual(_.size(await wo.select()), 1)
            })
        })

        //併發之相同整筆覆寫, 交易外之預讀皆見舊值, 皆進寫交易; 交易內讀到前者已寫入之現值, 後者與現值相同而不寫入, 僅一筆nModified為1
        it('should write once and report nModified 1 only once for 5 concurrent identical save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'i4', a: 1, b: 2 })
                let rss = await Promise.all(_.times(5, function() {
                    return wo.save({ id: 'i4', a: 1 }, { merge: false })
                }))
                let rs = _.flatten(rss)
                assert.strict.deepStrictEqual(_.sortBy(_.map(rs, 'nModified')), [0, 0, 0, 0, 1])
                assert.strict.deepStrictEqual(_.every(rs, (x) => x.n === 1 && x.nInserted === 0 && x.ok === 1), true)
                assert.strict.deepStrictEqual(await wo.selectByPk('i4'), { id: 'i4', a: 1 })
            })
        })

        //覆寫期間並行讀取, 讀者任何時刻都須見到完整記錄(無刪後插前之空窗)
        it('should never let a concurrent reader see the record missing or torn during save(merge:false)', async function() {
            await withWo(async(wo) => {
                await wo.insert({ id: 'i3', ver: 0, a: 0, b: 0 })
                let bad = []
                let reads = 0
                let done = false
                let reader = (async() => {
                    for (;;) {
                        if (done) {
                            break
                        }
                        let v = await wo.selectByPk('i3')
                        reads++
                        if (v === null || !(v.a === v.ver && v.b === v.ver)) {
                            bad.push(v)
                        }

                        //讓出macrotask, 否則讀者只佔microtask而寫入之提交回呼無從執行
                        await new Promise((resolve) => setImmediate(resolve))
                    }
                })()
                for (let k = 1; k <= 150; k++) {
                    await wo.save({ id: 'i3', ver: k, a: k, b: k }, { merge: false })
                }
                done = true
                await reader
                assert.strict.deepStrictEqual(reads > 0, true)
                assert.strict.deepStrictEqual(bad, [])
                assert.strict.deepStrictEqual(await wo.selectByPk('i3'), { id: 'i3', ver: 150, a: 150, b: 150 })
            })
        })

    })

})
