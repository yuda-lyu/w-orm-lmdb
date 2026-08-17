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
