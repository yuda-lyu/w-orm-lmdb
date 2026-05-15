import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('cache', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function () {

        w.fsDeleteFolder('./_db_cache')

        let opt = {
            url: './_db_cache',
            db: 'worm',
            cl: 'users',
            useCache: true,
        }

        let rs = [
            {
                id: 'id-peter',
                name: 'peter',
                value: 123,
            },
            {
                id: 'id-rosemary',
                name: 'rosemary',
                value: 123.456,
            },
            {
                id: 'id-kettle',
                name: 'kettle',
                value: 456,
            },
        ]

        let rsm = [
            {
                id: 'id-rosemary',
                name: 'rosemary(modify)',
                value: 654.321,
            },
        ]

        //wo
        let wo = WOrm(opt)

        //on
        wo.on('change', function(mode, data, res) {
            // console.log('change', mode)
        })

        //delAll
        await wo.delAll()

        //insert
        await wo.insert(rs)

        //select all (1st time, 從DB讀取並填入快取)
        rt = null
        // vans[1] = [
        //     { id: 'id-kettle', name: 'kettle', value: 456 },
        //     { id: 'id-peter', name: 'peter', value: 123 },
        //     { id: 'id-rosemary', name: 'rosemary', value: 123.456 },
        // ]
        await wo.select()
            .then(function(msg) {
                // console.log('select 1st then', msg)
                msg = _.sortBy(msg, 'name')
                rt = msg
            })
            .catch(function(msg) {
                // console.log('select 1st catch', msg)
                rt = msg.toString()
            })
        vget[1] = rt

        //select all (2nd time, 命中快取, 內容須與第一次相同)
        rt = null
        // vans[2] = [
        //     { id: 'id-kettle', name: 'kettle', value: 456 },
        //     { id: 'id-peter', name: 'peter', value: 123 },
        //     { id: 'id-rosemary', name: 'rosemary', value: 123.456 },
        // ]
        await wo.select()
            .then(function(msg) {
                // console.log('select 2nd then', msg)
                msg = _.sortBy(msg, 'name')
                rt = msg
            })
            .catch(function(msg) {
                // console.log('select 2nd catch', msg)
                rt = msg.toString()
            })
        vget[2] = rt

        //save (更新rosemary, 觸發快取重設)
        rt = null
        // vans[3] = [{ n: 1, nModified: 1, ok: 1 }]
        await wo.save(rsm, { autoInsert: false })
            .then(function(msg) {
                // console.log('save then', msg)
                rt = msg
            })
            .catch(function(msg) {
                // console.log('save catch', msg)
                rt = msg.toString()
            })
        vget[3] = rt

        //select all (3rd time, 快取已重設, 從DB重新讀取, 須反映rosemary更新)
        rt = null
        // vans[4] = [
        //     { id: 'id-kettle', name: 'kettle', value: 456 },
        //     { id: 'id-peter', name: 'peter', value: 123 },
        //     { id: 'id-rosemary', name: 'rosemary(modify)', value: 654.321 },
        // ]
        await wo.select()
            .then(function(msg) {
                // console.log('select 3rd then', msg)
                msg = _.sortBy(msg, 'name')
                rt = msg
            })
            .catch(function(msg) {
                // console.log('select 3rd catch', msg)
                rt = msg.toString()
            })
        vget[4] = rt

    })

    vans[1] = [
        { id: 'id-kettle', name: 'kettle', value: 456 },
        { id: 'id-peter', name: 'peter', value: 123 },
        { id: 'id-rosemary', name: 'rosemary', value: 123.456 },
    ]
    it(`should get ${JSON.stringify(vans[1])} for select all (1st, fill cache)`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = [
        { id: 'id-kettle', name: 'kettle', value: 456 },
        { id: 'id-peter', name: 'peter', value: 123 },
        { id: 'id-rosemary', name: 'rosemary', value: 123.456 },
    ]
    it(`should get ${JSON.stringify(vans[2])} for select all (2nd, cache hit)`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = [{ n: 1, nModified: 1, ok: 1 }]
    it(`should get ${JSON.stringify(vans[3])} for save (invalidate cache)`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = [
        { id: 'id-kettle', name: 'kettle', value: 456 },
        { id: 'id-peter', name: 'peter', value: 123 },
        { id: 'id-rosemary', name: 'rosemary(modify)', value: 654.321 },
    ]
    it(`should get ${JSON.stringify(vans[4])} for select all (3rd, reload after save)`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

})
