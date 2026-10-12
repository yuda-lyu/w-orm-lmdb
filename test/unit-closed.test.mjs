import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('closed', function() {
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_closed')

        let wo = WOrm({ url: './_db_closed', db: 'worm', cl: 'users' })
        await wo.insert({ id: 'k1', name: 'peter' })
        await wo.close()

        //evs, close後之操作屬整批性錯誤, 依T10.3須發出error事件
        let evs = []
        wo.on('error', function(mode, data, err) {
            evs.push({ mode, isStr: _.isString(err) })
        })

        //run, close後各函數皆須立即reject(而非空轉約200秒), 且訊息明示closed而不與[查無資料]同形
        let run = async (fn) => {
            let t0 = Date.now()
            let r = await fn()
                .then(function(msg) {
                    return { type: 'resolve', msg }
                })
                .catch(function(err) {
                    return { type: 'reject', closed: String(err).indexOf('closed') >= 0 }
                })
            r.fast = (Date.now() - t0) < 2000
            return r
        }

        vget[1] = await run(() => wo.select())
        vget[2] = await run(() => wo.selectByPk('k1'))
        vget[3] = await run(() => wo.insert({ id: 'k2' }))
        vget[4] = await run(() => wo.insert({ id: 'k2' }, { returnList: true }))
        vget[5] = await run(() => wo.insertBulk({ id: 'k3' }))
        vget[6] = await run(() => wo.save({ id: 'k1', name: 'mary' }))
        vget[7] = await run(() => wo.del({ id: 'k1' }))
        vget[8] = await run(() => wo.delAll())
        vget[9] = _.map(evs, 'mode')
        vget[10] = _.uniq(_.map(evs, 'isStr'))

    })

    let rj = { type: 'reject', closed: true, fast: true }

    vans[1] = rj
    it(`should get ${JSON.stringify(vans[1])} for select after close`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    //不可回null, 否則[已關閉]與[查無資料]同形而fail-open
    vans[2] = rj
    it(`should get ${JSON.stringify(vans[2])} for selectByPk after close`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = rj
    it(`should get ${JSON.stringify(vans[3])} for insert after close`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = rj
    it(`should get ${JSON.stringify(vans[4])} for insert(returnList) after close`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    //守門須在進入childTransaction之前, 否則已關閉env上之非同步回調會拋出未捕捉例外使行程崩潰
    vans[5] = rj
    it(`should get ${JSON.stringify(vans[5])} for insertBulk after close`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    //屬整批性錯誤而reject, 不可降為逐筆ok:0之resolve
    vans[6] = rj
    it(`should get ${JSON.stringify(vans[6])} for save after close`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    //不可回[{n:0,nDeleted:0,ok:1}]之正常未命中結果
    vans[7] = rj
    it(`should get ${JSON.stringify(vans[7])} for del after close`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = rj
    it(`should get ${JSON.stringify(vans[8])} for delAll after close`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = ['select', 'selectByPk', 'insert', 'insert', 'insertBulk', 'save', 'del', 'delAll']
    it(`should get ${JSON.stringify(vans[9])} for error events after close`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = [true]
    it(`should get ${JSON.stringify(vans[10])} for err of error events being string`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

})


//關閉進行中: close()呼叫後至關閉完成前(有未完成之寫入時持續至寫入結束)同屬實例已關閉(T4),
//各函數須立即reject而不空轉, 不得回正常空結果, 且不得使lmdb-js於計時器內拋出未捕捉例外而使程序崩潰
describe('closing', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/closing'
    let msg = (mode) => `can not ${mode} by closed client, instance can not be used after close()`

    before(async function() {

        w.fsDeleteFolder(url)

        //uncaught, 記錄全程之未捕捉例外
        let uncaught = []
        let onUncaught = (err) => {
            uncaught.push(String(err))
        }
        process.on('uncaughtException', onUncaught)

        //run, 記錄型態、訊息是否為統一之已關閉訊息、是否快速
        let run = async (mode, pm) => {
            let t0 = Date.now()
            let r = await pm
                .then(function(res) {
                    return { type: 'resolve', res }
                })
                .catch(function(err) {
                    return { type: 'reject', msg: _.get(err, 'message') === msg(mode) }
                })
            r.fast = (Date.now() - t0) < 2000
            return r
        }

        //關閉進行中呼叫七函數
        let wo = WOrm({ url, db: 'worm', cl: 'c1' })
        await wo.insert([{ id: 'k1', v: 1 }, { id: 'k2', v: 2 }, { id: 'k3', v: 3 }])
        let evs = []
        wo.on('error', function(mode, data, err) {
            evs.push({ mode, isMsg: err === msg(mode) })
        })
        let pmBig = wo.insert(_.times(20000, (i) => {
            return { id: `b${i}`, s: 'y'.repeat(100) }
        })) //未完成之寫入, 令關閉進行中持續至其結束
        let pmClose = wo.close()
        let rs = await Promise.all([
            run('select', wo.select()),
            run('selectByPk', wo.selectByPk('k1')),
            run('insert', wo.insert({ id: 'n1' })),
            run('insertBulk', wo.insertBulk([{ id: 'n2' }])),
            run('save', wo.save({ id: 'k1', v: 100 })),
            run('del', wo.del({ id: 'k2' })),
            run('delAll', wo.delAll({ id: 'k3' })),
        ])
        await pmClose
        vget[1] = rs
        vget[2] = evs
        vget[3] = await pmBig.then((r) => r.nInserted) //close前已送出之寫入照常完成

        //入口通過後才close: 呼叫後同步呼叫close()
        let midop = async (mode, fn) => {
            let wo2 = WOrm({ url, db: 'worm', cl: `m-${mode}` })
            await wo2.insert([{ id: 'r1', v: 1 }, { id: 'r2', v: 2 }])
            let pm = fn(wo2)
            let pmc = wo2.close()
            let r = await run(mode, pm)
            await pmc
            return r
        }
        vget[4] = await midop('select', (o) => o.select())
        vget[5] = await midop('selectByPk', (o) => o.selectByPk('r1'))
        vget[6] = await midop('insert', (o) => o.insert([{ id: 'n1' }, { id: 'n2' }]))
        vget[7] = await midop('insertBulk', (o) => o.insertBulk([{ id: 'n1' }, { id: 'n2' }]))
        vget[8] = await midop('save', (o) => o.save([{ id: 'r1', v: 9 }, { id: 'r2', v: 9 }]))
        vget[9] = await midop('del', (o) => o.del([{ id: 'r1' }, { id: 'r2' }]))
        vget[10] = await midop('delAll', (o) => o.delAll())

        //重複close
        let wo3 = WOrm({ url, db: 'worm', cl: 'c3' })
        await wo3.insert({ id: 'k1' })
        vget[11] = await Promise.all([wo3.close(), wo3.close()])
            .then(() => wo3.close())
            .then(() => 'resolve', () => 'reject')

        //單筆小寫入未完成時關閉: close與該筆寫入須如期完成(修正前lmdb-js於寫入提交之回呼內拋錯, close與寫入皆永不完成)
        let smallTrial = async (t) => {
            let o = WOrm({ url, db: 'worm', cl: `s${t}` })
            await o.insert({ id: 'k1', v: 1 })
            let pmIns = o.insert({ id: `p${t}` })
            let pmCls = o.close()
            let pmDel = run('del', o.del({ id: 'k1' }))
            let pmSave = run('save', o.save({ id: 'k1', v: 2 }))
            let timeout = () => new Promise((resolve) => setTimeout(() => resolve('timeout'), 3000))
            let rCls = await Promise.race([pmCls.then(() => 'resolve'), timeout()])
            let rIns = await Promise.race([pmIns.then((r) => r.nInserted), timeout()])
            return { rCls, rIns, del: await pmDel, save: await pmSave }
        }
        vget[13] = [await smallTrial(1), await smallTrial(2), await smallTrial(3)]

        //曾寫入而無未完成寫入時關閉: close()未await期間同屬關閉進行中
        let woI = WOrm({ url, db: 'worm', cl: 'idle' })
        await woI.insert({ id: 'k1', v: 1 })
        await new Promise((resolve) => setTimeout(resolve, 20))
        let pmClsI = woI.close()
        vget[14] = await Promise.all([
            run('select', woI.select()),
            run('selectByPk', woI.selectByPk('k1')),
            run('save', woI.save({ id: 'k1', v: 2 })),
            run('del', woI.del({ id: 'k1' })),
            run('delAll', woI.delAll()),
        ])
        await pmClsI

        //關閉進行中之無效輸入: 已關閉之判斷先於輸入檢查, 不得回正常空結果
        let woV = WOrm({ url, db: 'worm', cl: 'inv' })
        await woV.insert({ id: 'k1' })
        let pmClsV = woV.close()
        vget[15] = await Promise.all([
            run('selectByPk', woV.selectByPk()),
            run('insert', woV.insert(null)),
            run('insert', woV.insert(null, { returnList: true })),
            run('insertBulk', woV.insertBulk(null)),
            run('save', woV.save(null)),
            run('del', woV.del(null)),
            run('del', woV.del([{ name: 'no-id' }])),
        ])
        await pmClsV

        //入口通過後才close而其後不再存取資料庫者, 於關閉前已完成全部資料庫存取, 照常回傳
        vget[16] = await midop('del', (o) => o.del([{ name: 'no-id' }]))
        vget[17] = await midop('delAll', (o) => o.delAll({ id: 'not-existed' }))

        //error事件須先於reject(T10.3)
        let woO = WOrm({ url, db: 'worm', cl: 'ord' })
        await woO.insert({ id: 'k1' })
        let seq = []
        woO.on('error', function(mode) {
            seq.push(`ev:${mode}`)
        })
        let modes = ['select', 'selectByPk', 'insert', 'insertBulk', 'save', 'del', 'delAll']
        let callers = {
            select: () => woO.select(),
            selectByPk: () => woO.selectByPk('k1'),
            insert: () => woO.insert({ id: 'n1' }),
            insertBulk: () => woO.insertBulk([{ id: 'n1' }]),
            save: () => woO.save({ id: 'k1', v: 2 }),
            del: () => woO.del({ id: 'k1' }),
            delAll: () => woO.delAll(),
        }
        let pmClsO = woO.close()
        await Promise.all(_.map(modes, (mode) => {
            return callers[mode]().catch(() => {
                seq.push(`rj:${mode}`)
            })
        }))
        await pmClsO
        vget[18] = _.every(modes, (mode) => {
            let iev = seq.indexOf(`ev:${mode}`)
            let irj = seq.indexOf(`rj:${mode}`)
            return iev >= 0 && irj >= 0 && iev < irj
        })

        //入口通過後才close之insertBulk: 整批reject且未寫入任何一筆
        let woB = WOrm({ url, db: 'worm', cl: 'mb' })
        await woB.insert({ id: 'k1' })
        let pmB = run('insertBulk', woB.insertBulk([{ id: 'n1' }, { id: 'n2' }]))
        await woB.close()
        let rB = await pmB
        let woB2 = WOrm({ url, db: 'worm', cl: 'mb' })
        vget[19] = { r: rB, n1: await woB2.selectByPk('n1'), n2: await woB2.selectByPk('n2') }
        await woB2.close()

        //入口通過後才close之del整批reject: 先前之逐筆失敗不另發逐筆error事件, 只發整批之error事件(T10.1第1條)
        let woE = WOrm({ url, db: 'worm', cl: 'me' })
        await woE.insert({ id: 'r1' })
        let evE = []
        woE.on('error', function(mode, data, err) {
            evE.push(err)
        })
        let pmE = run('del', woE.del([{ name: 'no-id' }, { id: 'r1' }]))
        let pmClsE = woE.close()
        vget[20] = { r: await pmE, evs: _.clone(evE) }
        await pmClsE

        //等待lmdb-js之計時器觸發後再核對未捕捉例外
        await new Promise((resolve) => setTimeout(resolve, 50))
        process.removeListener('uncaughtException', onUncaught)
        vget[12] = uncaught

    })

    after(function() {
        w.fsDeleteFolder(url)
    })

    //七函數皆立即reject且訊息為統一之已關閉訊息, 不空轉、不回逐筆ok:0或未命中之正常結果
    vans[1] = _.times(7, () => {
        return { type: 'reject', msg: true, fast: true }
    })
    it(`should get ${JSON.stringify(vans[1])} for 7 functions during closing`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = _.map(['select', 'selectByPk', 'insert', 'insertBulk', 'save', 'del', 'delAll'], (mode) => {
        return { mode, isMsg: true }
    })
    it(`should get ${JSON.stringify(vans[2])} for error events during closing`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = 20000
    it(`should get ${JSON.stringify(vans[3])} for nInserted of writes sent before close`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    //讀取與入口同步完成, close無從插入, 正常回傳
    vans[4] = { type: 'resolve', res: [{ id: 'r1', v: 1 }, { id: 'r2', v: 2 }], fast: true }
    it(`should get ${JSON.stringify(vans[4])} for select with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = { type: 'resolve', res: { id: 'r1', v: 1 }, fast: true }
    it(`should get ${JSON.stringify(vans[5])} for selectByPk with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    //寫入於呼叫內即已送出, lmdb於關閉前完成
    vans[6] = { type: 'resolve', res: { n: 2, nInserted: 2, ok: 1 }, fast: true }
    it(`should get ${JSON.stringify(vans[6])} for insert with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    //交易回調延後執行而遇關閉, 整批reject且訊息為統一之已關閉訊息
    vans[7] = { type: 'reject', msg: true, fast: true }
    it(`should get ${JSON.stringify(vans[7])} for insertBulk with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    //逐筆處理於其後之microtask才執行而遇關閉, 屬整批性錯誤而reject, 不降為逐筆ok:0或未命中
    vans[8] = { type: 'reject', msg: true, fast: true }
    it(`should get ${JSON.stringify(vans[8])} for save with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = { type: 'reject', msg: true, fast: true }
    it(`should get ${JSON.stringify(vans[9])} for del with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = { type: 'reject', msg: true, fast: true }
    it(`should get ${JSON.stringify(vans[10])} for delAll with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    vans[11] = 'resolve'
    it(`should get ${JSON.stringify(vans[11])} for repeated close`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

    vans[12] = []
    it(`should get ${JSON.stringify(vans[12])} for uncaught exceptions`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = _.times(3, () => {
        return {
            rCls: 'resolve',
            rIns: 1,
            del: { type: 'reject', msg: true, fast: true },
            save: { type: 'reject', msg: true, fast: true },
        }
    })
    it(`should get ${JSON.stringify(vans[13])} for closing with a small pending write`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

    vans[14] = _.times(5, () => {
        return { type: 'reject', msg: true, fast: true }
    })
    it(`should get ${JSON.stringify(vans[14])} for closing after earlier writes without pending write`, async function() {
        assert.strict.deepStrictEqual(vget[14], vans[14])
    })

    vans[15] = _.times(7, () => {
        return { type: 'reject', msg: true, fast: true }
    })
    it(`should get ${JSON.stringify(vans[15])} for invalid inputs during closing`, async function() {
        assert.strict.deepStrictEqual(vget[15], vans[15])
    })

    vans[16] = { type: 'resolve', res: [{ n: 0, nDeleted: 0, ok: 0, err: 'can not delete by invalid id[]' }], fast: true }
    it(`should get ${JSON.stringify(vans[16])} for del without valid id with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[16], vans[16])
    })

    vans[17] = { type: 'resolve', res: { n: 0, nDeleted: 0, ok: 1 }, fast: true }
    it(`should get ${JSON.stringify(vans[17])} for delAll matching none with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[17], vans[17])
    })

    vans[18] = true
    it(`should get ${JSON.stringify(vans[18])} for error events emitted before reject during closing`, async function() {
        assert.strict.deepStrictEqual(vget[18], vans[18])
    })

    vans[19] = { r: { type: 'reject', msg: true, fast: true }, n1: null, n2: null }
    it(`should get ${JSON.stringify(vans[19])} for no records written by insertBulk with close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[19], vans[19])
    })

    vans[20] = { r: { type: 'reject', msg: true, fast: true }, evs: [msg('del')] }
    it(`should get ${JSON.stringify(vans[20])} for error events of del rejected by close right after call`, async function() {
        assert.strict.deepStrictEqual(vget[20], vans[20])
    })

})
