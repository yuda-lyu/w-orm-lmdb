import assert from 'assert'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('delAll', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_delall')

        let url = './_db_delall'

        let wo = WOrm({ url, db: 'worm', cl: 'users' })
        await wo.insert([
            { id: 'x1', g: 'A' },
            { id: 'x2', g: 'A' },
            { id: 'x3', g: 'B' },
            { id: 'x4', g: 'B' },
            { id: 'x5', g: 'B' },
        ])

        //delAll帶find且僅部份命中, n須為實際刪除筆數而非全表筆數
        rt = null
        // vans[1] = { n: 2, nDeleted: 2, ok: 1 }
        await wo.delAll({ g: 'A' })
            .then(function(msg) {
                // console.log('delAll(find) then', msg)
                // delAll(find) then { n: 2, nDeleted: 2, ok: 1 }
                rt = msg
            })
            .catch(function(msg) {
                // console.log('delAll(find) catch', msg)
                rt = msg.toString()
            })
        vget[1] = rt

        //刪除後剩餘筆數
        vget[2] = await wo.select().then(function(msg) {
            return msg.length
        })

        //delAll帶find但無命中
        rt = null
        // vans[3] = { n: 0, nDeleted: 0, ok: 1 }
        await wo.delAll({ g: 'not-existed' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[3] = rt

        //delAll不帶find, 全部刪除
        rt = null
        // vans[4] = { n: 3, nDeleted: 3, ok: 1 }
        await wo.delAll()
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[4] = rt

        vget[5] = await wo.select().then(function(msg) {
            return msg.length
        })

        //空表再delAll
        rt = null
        // vans[6] = { n: 0, nDeleted: 0, ok: 1 }
        await wo.delAll()
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[6] = rt

        await wo.close()

    })

    //n為實際刪除筆數, 與nDeleted同值, 不受全表筆數影響
    vans[1] = { n: 2, nDeleted: 2, ok: 1 }
    it(`should get ${JSON.stringify(vans[1])} for delAll(find) matching 2 of 5`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = 3
    it(`should get ${JSON.stringify(vans[2])} for records after delAll(find)`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = { n: 0, nDeleted: 0, ok: 1 }
    it(`should get ${JSON.stringify(vans[3])} for delAll(find) matching none`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = { n: 3, nDeleted: 3, ok: 1 }
    it(`should get ${JSON.stringify(vans[4])} for delAll without find`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = 0
    it(`should get ${JSON.stringify(vans[5])} for records after delAll without find`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = { n: 0, nDeleted: 0, ok: 1 }
    it(`should get ${JSON.stringify(vans[6])} for delAll on empty collection`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

})
