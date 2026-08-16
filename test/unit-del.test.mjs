import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('del', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_del')

        let url = './_db_del'

        let wo = WOrm({ url, db: 'worm', cl: 'users' })
        await wo.insert([
            { id: 'd1', name: 'peter' },
            { id: 'd2', name: 'rosemary' },
        ])

        //主鍵命中並刪除
        rt = null
        // vans[1] = [{ n: 1, nDeleted: 1, ok: 1 }]
        await wo.del({ id: 'd1' })
            .then(function(msg) {
                // console.log('del then', msg)
                // del then [ { n: 1, nDeleted: 1, ok: 1 } ]
                rt = msg
            })
            .catch(function(msg) {
                // console.log('del catch', msg)
                rt = msg.toString()
            })
        vget[1] = rt

        //主鍵未命中, 非錯誤故ok為1, 未命中故n為0
        rt = null
        // vans[2] = [{ n: 0, nDeleted: 0, ok: 1 }]
        await wo.del({ id: 'd-not-existed' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[2] = rt

        //未給有效主鍵, 屬該筆數據有問題故ok為0且須附err
        rt = null
        await wo.del({ name: 'no-id' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[3] = _.map(rt, function(v) {
            return _.omit(v, 'err')
        })
        vget[4] = w.isestr(_.get(rt, '0.err'))

        //整批含[未命中]與[未給有效主鍵], 單筆問題不中斷整批, 陣列長度須與輸入等長
        rt = null
        await wo.del([{ id: 'd2' }, { id: 'd-not-existed' }, { name: 'no-id' }])
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[5] = _.map(rt, function(v) {
            return _.omit(v, 'err')
        })

        //刪除後剩餘筆數
        vget[6] = _.size(await wo.select())

        //輸入非有效物件或陣列
        vget[7] = await wo.del(null)

        await wo.close()

    })

    vans[1] = [{ n: 1, nDeleted: 1, ok: 1 }]
    it(`should get ${JSON.stringify(vans[1])} for del with id existed`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = [{ n: 0, nDeleted: 0, ok: 1 }]
    it(`should get ${JSON.stringify(vans[2])} for del with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = [{ n: 0, nDeleted: 0, ok: 0 }]
    it(`should get ${JSON.stringify(vans[3])} for del without valid id`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = true
    it(`should get ${JSON.stringify(vans[4])} for err message existed in del without valid id`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = [
        { n: 1, nDeleted: 1, ok: 1 },
        { n: 0, nDeleted: 0, ok: 1 },
        { n: 0, nDeleted: 0, ok: 0 },
    ]
    it(`should get ${JSON.stringify(vans[5])} for del by array mixing valid and invalid items`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = 0
    it(`should get ${JSON.stringify(vans[6])} for records after del`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = []
    it(`should get ${JSON.stringify(vans[7])} for del with invalid input`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

})
