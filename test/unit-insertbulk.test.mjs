import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('insertBulk', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_insertbulk')

        let url = './_db_insertbulk'

        let wo = WOrm({ url, db: 'worm', cl: 'users' })

        //輸入無效
        vget[1] = await wo.insertBulk(null)

        //正常插入, nInserted須恆等於n
        rt = null
        // vans[2] = { n: 3, nInserted: 3, ok: 1 }
        await wo.insertBulk([{ id: 'a' }, { id: 'b' }, { id: 'c' }])
            .then(function(msg) {
                // console.log('insertBulk then', msg)
                // insertBulk then { n: 3, nInserted: 3, ok: 1 }
                rt = msg
            })
            .catch(function(msg) {
                // console.log('insertBulk catch', msg)
                rt = 'reject: ' + msg.toString()
            })
        vget[2] = rt
        vget[3] = _.map(_.sortBy(await wo.select(), 'id'), 'id')

        //單一物件亦須回傳單一物件而非陣列
        rt = null
        // vans[4] = { n: 1, nInserted: 1, ok: 1 }
        await wo.insertBulk({ id: 'd' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[4] = rt

        //含既有主鍵須整批reject, 且不得寫入任何一筆
        rt = null
        await wo.insertBulk([{ id: 'e' }, { id: 'a' }, { id: 'f' }])
            .then(function(msg) {
                rt = 'resolve: ' + JSON.stringify(msg)
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[5] = rt
        vget[6] = _.map(_.sortBy(await wo.select(), 'id'), 'id')

        //同批含重複主鍵亦視為衝突
        rt = null
        await wo.insertBulk([{ id: 'g' }, { id: 'g' }])
            .then(function(msg) {
                rt = 'resolve: ' + JSON.stringify(msg)
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[7] = rt
        vget[8] = _.map(_.sortBy(await wo.select(), 'id'), 'id')

        //事件: 成功發change, 衝突發error且不發change
        let evs = []
        wo.on('change', function(mode) {
            evs.push('change:' + mode)
        })
        wo.on('error', function(mode, data, err) {
            evs.push({ ev: 'error:' + mode, isStr: _.isString(err) })
        })
        await wo.insertBulk({ id: 'h' })
        vget[9] = _.map(evs, function(v) {
            return v.ev || v
        })
        evs = []
        await wo.insertBulk({ id: 'h' }).catch(function() {})
        vget[10] = _.map(evs, function(v) {
            return v.ev || v
        })
        vget[11] = _.get(evs, '0.isStr')
        await wo.close()

        //autoGenPk為true時補值
        let woGen = WOrm({ url, db: 'worm', cl: 'gen' })
        rt = null
        // vans[12] = { n: 2, nInserted: 2, ok: 1 }
        await woGen.insertBulk([{ v: 1 }, { v: 2 }])
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[12] = rt
        vget[13] = _.size(_.filter(await woGen.select(), function(v) {
            return w.isestr(v.id)
        }))
        await woGen.close()

        //autoGenPk為false而未帶有效主鍵, 須reject且不得寫入任何一筆
        let woNo = WOrm({ url, db: 'worm', cl: 'nogen', autoGenPk: false })
        rt = null
        await woNo.insertBulk([{ id: 'x1' }, { v: 1 }])
            .then(function(msg) {
                rt = 'resolve: ' + JSON.stringify(msg)
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[14] = rt
        vget[15] = _.size(await woNo.select())
        await woNo.close()

    })

    vans[1] = { n: 0, nInserted: 0, ok: 1 }
    it(`should get ${JSON.stringify(vans[1])} for insertBulk with invalid input`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = { n: 3, nInserted: 3, ok: 1 }
    it(`should get ${JSON.stringify(vans[2])} for insertBulk without conflict`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = ['a', 'b', 'c']
    it(`should get ${JSON.stringify(vans[3])} for records after insertBulk`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = { n: 1, nInserted: 1, ok: 1 }
    it(`should get ${JSON.stringify(vans[4])} for insertBulk by single object`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    //全有全無: 衝突須整批reject
    vans[5] = 'reject'
    it(`should get ${JSON.stringify(vans[5])} for insertBulk with existed id`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    //全有全無: 衝突時同批之無衝突筆數亦不得寫入
    vans[6] = ['a', 'b', 'c', 'd']
    it(`should get ${JSON.stringify(vans[6])} for no writing when insertBulk rejected`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = 'reject'
    it(`should get ${JSON.stringify(vans[7])} for insertBulk with duplicated id in same batch`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = ['a', 'b', 'c', 'd']
    it(`should get ${JSON.stringify(vans[8])} for no writing when duplicated id in same batch`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = ['change:insertBulk']
    it(`should get ${JSON.stringify(vans[9])} for change event of insertBulk`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = ['error:insertBulk']
    it(`should get ${JSON.stringify(vans[10])} for error event of insertBulk with conflict`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    vans[11] = true
    it(`should get ${JSON.stringify(vans[11])} for err of error event being string`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

    vans[12] = { n: 2, nInserted: 2, ok: 1 }
    it(`should get ${JSON.stringify(vans[12])} for insertBulk without id by autoGenPk=true`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = 2
    it(`should get ${JSON.stringify(vans[13])} for records with generated id`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

    vans[14] = 'reject'
    it(`should get ${JSON.stringify(vans[14])} for insertBulk without id by autoGenPk=false`, async function() {
        assert.strict.deepStrictEqual(vget[14], vans[14])
    })

    vans[15] = 0
    it(`should get ${JSON.stringify(vans[15])} for no writing when autoGenPk=false rejected`, async function() {
        assert.strict.deepStrictEqual(vget[15], vans[15])
    })

})
