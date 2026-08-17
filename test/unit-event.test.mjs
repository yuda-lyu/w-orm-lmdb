import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('event', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_event')

        let url = './_db_event'

        //evs, 蒐集事件, 事件名與各參數皆記錄以供比對
        let mkEvs = function(wo) {
            let evs = []
            wo.on('change', function(mode, data, res) {
                evs.push({ ev: 'change', mode, res })
            })
            wo.on('error', function(mode, data, err) {
                evs.push({ ev: 'error', mode, data, err })
            })
            return evs
        }

        //change事件之形狀: 逐筆函數以整批為單位發出一次
        let woC = WOrm({ url, db: 'worm', cl: 'ch' })
        let evsC = mkEvs(woC)
        await woC.insert([{ id: 'c1' }, { id: 'c2' }])
        await woC.save({ id: 'c1', a: 1 })
        await woC.del({ id: 'c2' })
        await woC.delAll()
        vget[1] = _.map(evsC, function(v) {
            return `${v.ev}:${v.mode}`
        })
        await woC.close()

        //save之逐筆插入另發mode為insert之事件, 且早於整批save事件
        let woI = WOrm({ url, db: 'worm', cl: 'ins' })
        let evsI = mkEvs(woI)
        await woI.save({ id: 'i1', a: 1 })
        vget[2] = _.map(evsI, function(v) {
            return `${v.ev}:${v.mode}`
        })
        await woI.close()

        //error事件: 整批性錯誤, 須於reject之前發出且err為字串
        let woE = WOrm({ url, db: 'worm', cl: 'errb', autoGenPk: false })
        let evsE = mkEvs(woE)
        rt = null
        await woE.insert({ name: 'no-id' })
            .then(function() {
                rt = 'resolve'
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[3] = rt
        vget[4] = _.map(evsE, function(v) {
            return `${v.ev}:${v.mode}`
        })
        vget[5] = w.isestr(_.get(evsE, '0.err'))
        vget[6] = _.get(evsE, '0.data') !== null
        await woE.close()

        //error事件: 逐筆失敗, 整批仍resolve且每筆一次
        let woD = WOrm({ url, db: 'worm', cl: 'errp' })
        let evsD = mkEvs(woD)
        await woD.insert({ id: 'd1' })
        evsD.length = 0
        rt = null
        await woD.del([{ id: 'd1' }, { name: 'no-id-a' }, { name: 'no-id-b' }])
            .then(function(msg) {
                rt = _.map(msg, 'ok')
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[7] = rt
        vget[8] = _.map(evsD, function(v) {
            return `${v.ev}:${v.mode}`
        })
        vget[9] = w.isestr(_.get(evsD, '0.err'))
        await woD.close()

        //正常結果不得發出error事件
        let woN = WOrm({ url, db: 'worm', cl: 'norm' })
        let evsN = mkEvs(woN)
        await woN.insert({ id: 'n1', a: 1 })
        evsN.length = 0
        await woN.insert({ id: 'n1', a: 2 }) //已存在, nInserted為0
        await woN.save({ id: 'n1', a: 1 }) //合併後內容相同, 不寫入
        await woN.save({ id: 'n2' }, { autoInsert: false }) //不存在且不autoInsert
        await woN.del({ id: 'n-none' }) //主鍵未命中
        await woN.delAll({ zz: 'none' }) //條件無命中
        await woN.selectByPk('n-none') //查無數據
        vget[10] = _.size(_.filter(evsN, function(v) {
            return v.ev === 'error'
        }))
        await woN.close()

        //核心不變式: 同一操作於有註冊與未註冊error監聽兩種情況下, 回傳值須完全相同
        let run = async function(withListener) {
            let wo = WOrm({ url, db: 'worm', cl: `inv${withListener ? 1 : 0}`, autoGenPk: false })
            if (withListener) {
                wo.on('error', function() {})
            }
            let r = {}

            //整批性錯誤
            r.batch = await wo.insert({ name: 'no-id' })
                .then(function(msg) {
                    return { type: 'resolve', msg }
                })
                .catch(function() {
                    return { type: 'reject' }
                })

            //逐筆失敗
            await wo.insert({ id: 'x1' })
            r.each = await wo.del([{ id: 'x1' }, { name: 'no-id' }])
                .then(function(msg) {
                    return {
                        type: 'resolve',
                        msg: _.map(msg, function(v) {
                            return _.omit(v, 'err')
                        }),
                    }
                })
                .catch(function() {
                    return { type: 'reject' }
                })

            await wo.close()
            return r
        }
        let rWith = await run(true)
        let rWithout = await run(false)
        vget[11] = _.isEqual(rWith, rWithout)
        vget[12] = rWithout.batch.type
        vget[13] = rWithout.each.type

        //訂閱函數拋錯不得影響本次操作
        let woT = WOrm({ url, db: 'worm', cl: 'thr' })
        woT.on('change', function() {
            throw new Error('listener boom')
        })
        woT.on('error', function() {
            throw new Error('listener boom')
        })
        rt = null
        await woT.insert({ id: 't1', a: 1 })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[14] = rt
        await woT.close()

    })

    //逐筆函數以整批為單位發出一次change, 不逐筆發出
    vans[1] = ['change:insert', 'change:save', 'change:del', 'change:delAll']
    it(`should get ${JSON.stringify(vans[1])} for change events`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = ['change:insert', 'change:save']
    it(`should get ${JSON.stringify(vans[2])} for change events of save with autoInsert`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = 'reject'
    it(`should get ${JSON.stringify(vans[3])} for insert without id by autoGenPk=false`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = ['error:insert']
    it(`should get ${JSON.stringify(vans[4])} for error event of batch error`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = true
    it(`should get ${JSON.stringify(vans[5])} for err of error event being string`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = true
    it(`should get ${JSON.stringify(vans[6])} for data of error event being input data`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    //逐筆失敗時整批仍resolve, 且每筆失敗各發出一次error
    vans[7] = [1, 0, 0]
    it(`should get ${JSON.stringify(vans[7])} for ok of each item when items failed`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = ['error:del', 'error:del', 'change:del']
    it(`should get ${JSON.stringify(vans[8])} for error events emitted before change`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = true
    it(`should get ${JSON.stringify(vans[9])} for err of each error event being string`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    //正常結果不得發出error
    vans[10] = 0
    it(`should get ${JSON.stringify(vans[10])} for error events by normal results`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    //T10.1核心不變式
    vans[11] = true
    it(`should get ${JSON.stringify(vans[11])} for same results with and without error listener`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

    vans[12] = 'reject'
    it(`should get ${JSON.stringify(vans[12])} for batch error still rejecting without error listener`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = 'resolve'
    it(`should get ${JSON.stringify(vans[13])} for each-item failure still resolving without error listener`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

    vans[14] = { n: 1, nInserted: 1, ok: 1 }
    it(`should get ${JSON.stringify(vans[14])} for result not affected by throwing listener`, async function() {
        assert.strict.deepStrictEqual(vget[14], vans[14])
    })

})
