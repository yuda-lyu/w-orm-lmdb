import assert from 'assert'
import path from 'path'
import { spawnSync } from 'child_process'
import _ from 'lodash-es'
import w from 'wsemi'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//寫入之整批性錯誤於任何寫入之前檢出(帳本R-WRITE-02; 規格T4、T10.3、insertBulk之全有全無):
//insert與insertBulk遇鍵經lmdb編碼後超過上限(1978位元組)、數據無法編碼、元素非物件者整批reject, reject當下與其後皆無任何新增,
//訊息為can not <fn> by data[k], <原訊息>(元素非物件者為can not <fn> by data[k] which is not an object), error事件先於reject且訊息一致;
//save依T4判為該筆失敗(ok:0)而其餘照常; reject後立即close不得使行程崩潰; delAll依資料庫實際鍵刪除(R-DELALL-01);
//鍵超長者於讀取側一律視為未命中(R-READ-03)
describe('writeerror', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/writeerror'

    //errKey, lmdb對超長鍵之原訊息
    let errKey = 'Key size is larger than the maximum key size (1978)'

    let sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

    //bads, 各類不可寫入之列; 鍵長以lmdb之鍵編碼計: 首字元碼小於28者多1位元組跳脫, 中文每字3位元組
    let bads = {
        key1979: () => ({ id: 'k'.repeat(1979) }),
        keyEsc1979: () => ({ id: '\x01' + 'k'.repeat(1977) }),
        keyUtf8: () => ({ id: '中'.repeat(660) }),
        key70000: () => ({ id: 'k'.repeat(70000) }),
        bigint: () => ({ id: 'bad', v: 2n ** 70n }),
        symbol: () => ({ id: 'bad', v: Symbol('s') }),
        circular: () => {
            let o = { id: 'bad' }
            o.self = o
            return o
        },
    }
    let isKeyKind = (kind) => kind.indexOf('key') === 0
    let notObjs = {
        array: () => [1, 2],
        date: () => new Date(0),
        map: () => new Map([['x', 1]]),
        set: () => new Set([1]),
        number: () => 1,
        null: () => null,
        string: () => 'str',
    }
    let posIndex = { first: 0, middle: 1, last: 2 }
    let mkData = (pos, b) => {
        if (pos === 'first') {
            return [b, { id: 'b', v: 2 }, { id: 'c', v: 3 }]
        }
        if (pos === 'middle') {
            return [{ id: 'a', v: 1 }, b, { id: 'c', v: 3 }]
        }
        return [{ id: 'a', v: 1 }, { id: 'b', v: 2 }, b]
    }

    //encErr, 以與src/WOrmLmdb.mjs相同參數開啟之裸lmdb之編碼器取得原訊息, 不寫死第三方訊息
    let encErr = null

    //runBatch, 各案各用一個資料表, 全部送出後統一等待, 再核對reject訊息、reject當下與其後之筆數、事件順序與事件訊息
    let runBatch = async (prefix, cases, act) => {
        let tasks = []
        let n = 0
        for (let c of cases) {
            n++
            let wo = WOrm({ url, db: 'worm', cl: `${prefix}${n}` })
            let seq = []
            let evMsg = null
            wo.on('error', function(mode, data, err) {
                seq.push(`ev:${mode}`)
                evMsg = err
            })
            let msg = null
            let r = await act(wo, c)
                .then(() => 'resolve', (err) => {
                    seq.push('rj')
                    msg = _.get(err, 'message', String(err))
                    return 'reject'
                })
            let at = _.size(await wo.select())
            tasks.push({ c, wo, r, msg, evMsg, at, seq })
        }
        await sleep(500)
        let rs = []
        for (let t of tasks) {
            rs.push({
                key: t.c.key,
                r: t.r,
                msg: t.msg,
                evSame: t.evMsg === t.msg,
                at: t.at,
                later: _.size(await t.wo.select()),
                evBeforeReject: t.seq[0] === `ev:${t.c.mode}` && t.seq[1] === 'rj',
            })
            await t.wo.close()
        }
        return rs
    }

    before(async function() {

        w.fsDeleteFolder(url)

        //encErr
        let raw = open({ path: path.resolve(`${url}/raw`), compression: true, useVersions: true })
        encErr = (v) => {
            try {
                raw.encoder.encode(v)
                return null
            }
            catch (err) {
                return err.message
            }
        }

        //insert: 7類×3位置×returnList兩值
        let casesIns = []
        for (let kind of _.keys(bads)) {
            for (let pos of ['first', 'middle', 'last']) {
                for (let rl of [false, true]) {
                    casesIns.push({ key: `${kind}-${pos}-${rl}`, mode: 'insert', kind, pos, rl })
                }
            }
        }
        vget[1] = await runBatch('i', casesIns, (wo, c) => wo.insert(mkData(c.pos, bads[c.kind]()), { returnList: c.rl }))
        vans[1] = _.map(casesIns, (c) => {
            let k = posIndex[c.pos]
            let tail = isKeyKind(c.kind) ? errKey : encErr(bads[c.kind]())
            return { key: c.key, r: 'reject', msg: `can not insert by data[${k}], ${tail}`, evSame: true, at: 0, later: 0, evBeforeReject: true }
        })

        //insertBulk: 7類×3位置
        let casesBulk = []
        for (let kind of _.keys(bads)) {
            for (let pos of ['first', 'middle', 'last']) {
                casesBulk.push({ key: `${kind}-${pos}`, mode: 'insertBulk', kind, pos })
            }
        }
        vget[2] = await runBatch('b', casesBulk, (wo, c) => wo.insertBulk(mkData(c.pos, bads[c.kind]())))
        vans[2] = _.map(casesBulk, (c) => {
            let k = posIndex[c.pos]
            let tail = isKeyKind(c.kind) ? errKey : encErr(bads[c.kind]())
            return { key: c.key, r: 'reject', msg: `can not insertBulk by data[${k}], ${tail}`, evSame: true, at: 0, later: 0, evBeforeReject: true }
        })

        //元素非物件: insert、insertBulk、save皆整批reject且無新增
        let casesNo = []
        for (let kind of _.keys(notObjs)) {
            for (let mode of ['insert', 'insertBulk', 'save']) {
                casesNo.push({ key: `${mode}-${kind}`, mode, kind })
            }
        }
        vget[3] = await runBatch('n', casesNo, (wo, c) => wo[c.mode]([{ id: 'a', v: 1 }, notObjs[c.kind]()]))
        vans[3] = _.map(casesNo, (c) => {
            return { key: c.key, r: 'reject', msg: `can not ${c.mode} by data[1] which is not an object`, evSame: true, at: 0, later: 0, evBeforeReject: true }
        })

        //類別實例屬物件, 照常寫入且可讀回
        class Foo {
            constructor(id) {
                this.id = id
                this.x = 1
            }
        }
        let rsCls = []
        for (let mode of ['insert', 'insertBulk', 'save']) {
            let wo = WOrm({ url, db: 'worm', cl: `cls-${mode}` })
            let r = await wo[mode]([new Foo('f1')]).then(() => 'resolve', (err) => err.message)
            rsCls.push({ mode, r, v: await wo.selectByPk('f1') })
            await wo.close()
        }
        vget[4] = rsCls

        //save: 該筆ok為0並附err, 其餘筆照常寫入, 定案當下與其後之資料相同; 鍵超長之err不因長度而異(R-READ-03)
        let rsSave = []
        let m = 0
        for (let kind of ['key1979', 'key70000', 'bigint', 'symbol']) {
            m++
            let wo = WOrm({ url, db: 'worm', cl: `s${m}` })
            let r = await wo.save([{ id: 'a', v: 1 }, bads[kind](), { id: 'c', v: 3 }])
            let at = _.map(await wo.select(), 'id').join(',')
            await sleep(300)
            let later = _.map(await wo.select(), 'id').join(',')
            rsSave.push({ kind, oks: _.map(r, 'ok'), err: _.get(r, '1.err'), at, later })
            await wo.close()
        }
        vget[5] = rsSave
        vans[5] = _.map(['key1979', 'key70000', 'bigint', 'symbol'], (kind) => {
            return { kind, oks: [1, 0, 1], err: isKeyKind(kind) ? errKey : encErr(bads[kind]()), at: 'a,c', later: 'a,c' }
        })

        //鍵長邊界: 恰為1978位元組者可寫入且可讀回
        let woE = WOrm({ url, db: 'worm', cl: 'edge' })
        let edges = ['k'.repeat(1978), '\x01' + 'k'.repeat(1976), '中'.repeat(659)]
        let rE = await woE.insert(_.map(edges, (id) => ({ id })))
        let rd = []
        for (let id of edges) {
            rd.push(_.get(await woE.selectByPk(id), 'id') === id)
        }
        vget[6] = { r: rE, rd }
        await woE.close()

        //delAll依實際鍵刪除: 值不帶id之紀錄(他方以裸lmdb寫入)亦可刪除, 且條件比對與select一致
        //註: reject後先讓出再close, 行程崩潰與否由下方子程序情境delAllIdless判定, 此處只核對資料面
        let mkIdless = async (cl, rows, raws) => {
            let wo = WOrm({ url, db: 'worm', cl })
            await wo.insert(rows)
            let rw = open({ path: path.resolve(`${url}/worm/${cl}`), compression: true, useVersions: true })
            for (let [k, v] of raws) {
                await rw.put(k, v)
            }
            await rw.close()
            return wo
        }
        let woD1 = await mkIdless('d1', [{ id: 'a' }, { id: 'z' }], [['m', [1, 2]]])
        let rD1 = await woD1.delAll().catch((err) => err.message)
        await sleep(50)
        vget[7] = { r: rD1, rest: _.size(await woD1.select()) }
        await woD1.close()
        let woD2 = await mkIdless('d2', [{ id: 'a', v: 1 }, { id: 'b', v: 2 }, { id: 'z', v: 1 }], [['m', { v: 1 }], ['n', { v: 2 }]])
        let rD2 = await woD2.delAll({ v: 1 }).catch((err) => err.message)
        await sleep(50)
        vget[8] = { r: rD2, rest: _.map(await woD2.select(), 'v') }
        await woD2.close()

        //無法複製之輸入(取值拋錯之getter): 屬整批性錯誤, error事件先於reject
        let getterThrow = () => {
            let o = { id: 'g' }
            Object.defineProperty(o, 'x', {
                enumerable: true,
                get() {
                    throw new Error('getter boom')
                },
            })
            return o
        }
        let casesClone = _.map(['insert', 'insertBulk', 'save', 'del'], (mode) => ({ key: mode, mode }))
        vget[9] = await runBatch('g', casesClone, (wo, c) => wo[c.mode]([getterThrow()]))

        //鍵超長於讀取側一律視為未命中, 不因長度而回錯誤, 亦不發出error事件(R-READ-03)
        let longs = {
            b1979: 'k'.repeat(1979),
            esc1979: '\x01' + 'k'.repeat(1977),
            b4092: 'k'.repeat(4092),
            b4093: 'k'.repeat(4093),
            b70000: 'k'.repeat(70000),
        }
        let woL = WOrm({ url, db: 'worm', cl: 'long' })
        await woL.insert({ id: 'g1', v: 1 })
        let evL = []
        woL.on('error', function(mode) {
            evL.push(mode)
        })
        let st = (pm) => pm.then((r) => ({ type: 'resolve', r }), (err) => ({ type: 'reject', msg: _.get(err, 'message') }))
        let rsL = []
        for (let name of _.keys(longs)) {
            let id = longs[name]
            rsL.push({
                name,
                selectByPk: await st(woL.selectByPk(id)),
                del: await st(woL.del({ id })),
                saveNoInsert: await st(woL.save({ id, v: 1 }, { autoInsert: false })),
                save: await st(woL.save({ id, v: 1 })),
            })
        }
        vget[10] = rsL
        vget[11] = evL
        vget[12] = await woL.select()
        await woL.close()

        //reject後立即close: 行程須正常結束
        let tool = path.resolve('./test/tools/closeAfterReject.mjs')
        vget[13] = _.map(['insertBadFirst', 'insertBadFirstAfterWrites', 'insertBadFirstNoAwait', 'insertBadFirstOtherOpen', 'delAllIdless'], (sc) => {
            let u = `${url}/child-${sc}`
            let r = spawnSync(process.execPath, [tool, sc, u], { encoding: 'utf8', timeout: 60000 })
            return { sc, status: r.status, end: _.includes(r.stdout, 'end') }
        })

        await raw.close()

    })

    after(function() {
        w.fsDeleteFolder(url)
    })

    it(`should get reject with message of data index and no record for insert with unwritable row`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    it(`should get reject with message of data index and no record for insertBulk with unwritable row`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    it(`should get reject without any record for insert, insertBulk and save with non-object element`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = _.map(['insert', 'insertBulk', 'save'], (mode) => {
        return { mode, r: 'resolve', v: { id: 'f1', x: 1 } }
    })
    it(`should get ${JSON.stringify(vans[4])} for class instance as element`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    it(`should get ok:0 only on the unwritable row for save`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = { r: { n: 3, nInserted: 3, ok: 1 }, rd: [true, true, true] }
    it(`should get ${JSON.stringify(vans[6])} for keys of exactly max key size`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = { r: { n: 3, nDeleted: 3, ok: 1 }, rest: 0 }
    it(`should get ${JSON.stringify(vans[7])} for delAll with id-less record`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = { r: { n: 3, nDeleted: 3, ok: 1 }, rest: [2, 2] }
    it(`should get ${JSON.stringify(vans[8])} for delAll by find with id-less records`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    it(`should get error event before reject for input which can not be cloned`, async function() {
        let exp = _.map(vget[9], (v) => ({ key: v.key, r: 'reject', msg: 'getter boom', evSame: true, at: 0, later: 0, evBeforeReject: true }))
        assert.strict.deepStrictEqual(vget[9], exp)
    })

    vans[10] = _.map(['b1979', 'esc1979', 'b4092', 'b4093', 'b70000'], (name) => {
        return {
            name,
            selectByPk: { type: 'resolve', r: null },
            del: { type: 'resolve', r: [{ n: 0, nDeleted: 0, ok: 1 }] },
            saveNoInsert: { type: 'resolve', r: [{ n: 0, nInserted: 0, nModified: 0, ok: 1 }] },
            save: { type: 'resolve', r: [{ n: 1, nInserted: 0, nModified: 0, ok: 0, err: errKey }] },
        }
    })
    it(`should get not-found for selectByPk, del and save on keys exceeding max key size`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    //正常結果(selectByPk查無、del未命中、save未插入)不發出error事件, 僅save插入失敗之筆逐筆發出
    vans[11] = _.times(5, () => 'save')
    it(`should get ${JSON.stringify(vans[11])} for error events of reads on keys exceeding max key size`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

    vans[12] = [{ id: 'g1', v: 1 }]
    it(`should get ${JSON.stringify(vans[12])} for records after reads on keys exceeding max key size`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = _.map(['insertBadFirst', 'insertBadFirstAfterWrites', 'insertBadFirstNoAwait', 'insertBadFirstOtherOpen', 'delAllIdless'], (sc) => {
        return { sc, status: 0, end: true }
    })
    it(`should get ${JSON.stringify(vans[13])} for close right after rejected write`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

})
