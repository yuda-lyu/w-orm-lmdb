import assert from 'assert'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


describe('returnList', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_returnlist')

        let url = './_db_returnlist'

        let wo = WOrm({ url, db: 'worm', cl: 'users' })

        //輸入無效, 開啟returnList時回傳空陣列(對齊save與del之T5)
        vget[1] = await wo.insert(null, { returnList: true })

        //全新3筆, 逐筆恆為已插入
        rt = null
        // vans[2] = [{ n: 1, nInserted: 1, ok: 1 }, ...]
        await wo.insert([{ id: 'a' }, { id: 'b' }, { id: 'c' }], { returnList: true })
            .then(function(msg) {
                // console.log('insert(returnList) then', msg)
                // insert(returnList) then [
                //   { n: 1, nInserted: 1, ok: 1 },
                //   { n: 1, nInserted: 1, ok: 1 },
                //   { n: 1, nInserted: 1, ok: 1 }
                // ]
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[2] = rt

        //混入既有主鍵, 逐筆結果須與輸入對位
        rt = null
        await wo.insert([{ id: 'd' }, { id: 'a' }, { id: 'e' }], { returnList: true })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[3] = rt

        //同批重複主鍵, 僅首筆nInserted為1
        rt = null
        await wo.insert([{ id: 'f' }, { id: 'f' }], { returnList: true })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[4] = rt

        //不變式: filter計數等於聚合模式之nInserted(同一情境於另一資料表以聚合模式對照)
        let woAgg = WOrm({ url, db: 'worm', cl: 'agg' })
        await woAgg.insert([{ id: 'a' }, { id: 'b' }, { id: 'c' }])
        let rAgg = await woAgg.insert([{ id: 'd' }, { id: 'a' }, { id: 'e' }])
        vget[5] = {
            fromList: _.size(_.filter(vget[3], function(v) {
                return v.nInserted === 1
            })),
            fromAgg: rAgg.nInserted,
        }
        await woAgg.close()

        //單一物件輸入, 回傳長度1之陣列
        rt = null
        await wo.insert({ id: 'g' }, { returnList: true })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[6] = rt

        //逐筆元素之鍵集合恰為n,nInserted,ok
        vget[7] = _.uniq(_.map(vget[3], function(v) {
            return _.keys(v).sort().join(',')
        }))

        //預設(未給option)仍回傳聚合物件, 鍵集合不變
        rt = null
        await wo.insert({ id: 'h' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = 'reject: ' + msg.toString()
            })
        vget[8] = rt

        //change事件之res即本次實際回傳值(陣列)
        let evs = []
        wo.on('change', function(mode, data, res) {
            evs.push({ mode, isArr: _.isArray(res), len: _.size(res) })
        })
        await wo.insert([{ id: 'i1' }, { id: 'i2' }], { returnList: true })
        vget[9] = evs

        await wo.close()

        //autoGenPk為false而未帶有效主鍵, 不受returnList影響仍整批reject
        let woNo = WOrm({ url, db: 'worm', cl: 'nogen', autoGenPk: false })
        rt = null
        await woNo.insert([{ id: 'x1' }, { zz: 1 }], { returnList: true })
            .then(function(msg) {
                rt = 'resolve: ' + JSON.stringify(msg)
            })
            .catch(function() {
                rt = 'reject'
            })
        vget[10] = rt
        vget[11] = _.size(await woNo.select())
        await woNo.close()

    })

    vans[1] = []
    it(`should get ${JSON.stringify(vans[1])} for insert(returnList) with invalid input`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = [
        { n: 1, nInserted: 1, ok: 1 },
        { n: 1, nInserted: 1, ok: 1 },
        { n: 1, nInserted: 1, ok: 1 },
    ]
    it(`should get ${JSON.stringify(vans[2])} for insert(returnList) with all new`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    //對位: 第2筆為既有主鍵故nInserted為0, 其餘為1
    vans[3] = [
        { n: 1, nInserted: 1, ok: 1 },
        { n: 1, nInserted: 0, ok: 1 },
        { n: 1, nInserted: 1, ok: 1 },
    ]
    it(`should get ${JSON.stringify(vans[3])} for insert(returnList) with existed id at position 2`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = [
        { n: 1, nInserted: 1, ok: 1 },
        { n: 1, nInserted: 0, ok: 1 },
    ]
    it(`should get ${JSON.stringify(vans[4])} for insert(returnList) with duplicated id in same batch`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = { fromList: 2, fromAgg: 2 }
    it(`should get ${JSON.stringify(vans[5])} for filter count equal to aggregate nInserted`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = [{ n: 1, nInserted: 1, ok: 1 }]
    it(`should get ${JSON.stringify(vans[6])} for insert(returnList) by single object`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = ['n,nInserted,ok']
    it(`should get ${JSON.stringify(vans[7])} for keys of each item`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = { n: 1, nInserted: 1, ok: 1 }
    it(`should get ${JSON.stringify(vans[8])} for insert without option keeping aggregate shape`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = [{ mode: 'insert', isArr: true, len: 2 }]
    it(`should get ${JSON.stringify(vans[9])} for change event res being the returned array`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = 'reject'
    it(`should get ${JSON.stringify(vans[10])} for insert(returnList) without id by autoGenPk=false`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    vans[11] = 0
    it(`should get ${JSON.stringify(vans[11])} for no writing when autoGenPk=false rejected`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

})
