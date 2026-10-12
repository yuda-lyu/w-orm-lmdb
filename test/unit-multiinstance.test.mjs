import assert from 'assert'
import path from 'path'
import _ from 'lodash-es'
import w from 'wsemi'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//同一程序內同一資料表之其他實例: 讀取須可見本次呼叫前已提交之資料, 含同一程序之其他實例(另一次open)之提交
//lmdb-js之讀交易屬於每次open所得之實例, 寫入只重設寫入者自身之讀交易, 故同一程序之其他實例與其他程序同樣須讀前重設
describe('multiinstance', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/multiinstance'
    let N = 2

    before(async function() {

        w.fsDeleteFolder(url)

        let pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

        let wa = WOrm({ url, db: 'worm', cl: 'mi' })
        await wa.insert({ id: 'seed', v: 'A' }) //先建立資料表

        //raw, 同一程序另一次open之裸lmdb, 開啟參數同src/WOrmLmdb.mjs, 以同步提交(putSync)寫入,
        //令本實例之先行讀取與其後之操作落於同一macrotask, 未於讀前重設者必讀到舊快照
        let raw = open({ path: path.resolve(`${url}/worm/mi`), compression: true, useVersions: true })

        //priorRead, 本實例先讀一次, 令讀交易處於已建立而尚未被計時器重設之狀態
        let priorRead = () => wa.selectByPk('seed')

        //rec, 逐次記錄觀察值
        let rec = (k, v) => {
            if (!vget[k]) {
                vget[k] = []
            }
            vget[k].push(v)
        }

        for (let i = 0; i < N; i++) {
            let id = (k) => `c${k}_${i}`

            //1 select x 他實例新增
            await priorRead()
            raw.putSync(id(1), { id: id(1), v: 'B' })
            rec(1, _.size(await wa.select({ id: id(1) })))

            //2 selectByPk x 他實例更新
            await wa.insert({ id: id(2), v: 'old' })
            await priorRead()
            raw.putSync(id(2), { id: id(2), v: 'new' })
            rec(2, _.get(await wa.selectByPk(id(2)), 'v', null))

            //3 save與本實例所知之現值相同 x 他實例已更新: 本次save在後, 須寫入
            await wa.save({ id: id(3), v: 'X' })
            await priorRead()
            raw.putSync(id(3), { id: id(3), v: 'Y' })
            let rs3 = await wa.save({ id: id(3), v: 'X' })
            await pause(2)
            rec(3, { rs: rs3, fv: _.get(await wa.selectByPk(id(3)), 'v', null) })

            //4 del x 他實例新增
            await priorRead()
            raw.putSync(id(4), { id: id(4), v: 'B' })
            let rs4 = await wa.del({ id: id(4) })
            await pause(2)
            rec(4, { rs: rs4, fv: await wa.selectByPk(id(4)) })

            //5 delAll(find) x 他實例把列改為不符條件: 不得誤刪
            await wa.insert({ id: id(5), tag: id(5) })
            await priorRead()
            raw.putSync(id(5), { id: id(5), tag: 'keep' })
            let rs5 = await wa.delAll({ tag: id(5) })
            await pause(2)
            rec(5, { rs: rs5, fv: _.get(await wa.selectByPk(id(5)), 'tag', null) })

        }

        //兩個WOrmLmdb實例: 他實例先save, 本實例其後save同值, 終值須為本實例之值(README之同一程序內save不遺失更新)
        let wb = WOrm({ url, db: 'worm', cl: 'mi' })
        let lost = 0
        for (let i = 0; i < 20; i++) {
            let k = `s${i}`
            await wa.save({ id: k, v: 'A' })
            await wa.selectByPk(k)
            await wb.save({ id: k, v: 'B' })
            await wa.save({ id: k, v: 'A' })
            await pause(2)
            if (_.get(await wa.selectByPk(k), 'v') !== 'A') {
                lost++
            }
        }
        vget[6] = lost

        //useCache之實例: delAll不得依可能過期之快取決定刪除對象
        let wc = WOrm({ url, db: 'worm', cl: 'mc', useCache: true })
        let wd = WOrm({ url, db: 'worm', cl: 'mc' })
        await wc.insert([{ id: 'r1', flag: 1 }, { id: 'r2', flag: 0 }])
        await wc.select() //建立快取
        await wd.save({ id: 'r1', flag: 0 }) //他實例改為不符條件
        await wd.insert({ id: 'r9', flag: 1 }) //他實例新增符合條件者
        vget[7] = await wc.delAll({ flag: 1 })
        vget[8] = _.map(await wd.select(), 'id').sort()

        await raw.close()
        await wa.close()
        await wb.close()
        await wc.close()
        await wd.close()

    })

    after(function() {
        w.fsDeleteFolder(url)
    })

    let times = (v) => _.times(N, () => _.cloneDeep(v))

    vans[1] = times(1)
    it(`should get ${JSON.stringify(vans[1])} for select after another instance inserted`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = times('new')
    it(`should get ${JSON.stringify(vans[2])} for selectByPk after another instance updated`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = times({ rs: [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }], fv: 'X' })
    it(`should get ${JSON.stringify(vans[3])} for save with value same as known after another instance updated`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = times({ rs: [{ n: 1, nDeleted: 1, ok: 1 }], fv: null })
    it(`should get ${JSON.stringify(vans[4])} for del after another instance inserted`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = times({ rs: { n: 0, nDeleted: 0, ok: 1 }, fv: 'keep' })
    it(`should get ${JSON.stringify(vans[5])} for delAll after another instance updated record to unmatch`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = 0
    it(`should get ${JSON.stringify(vans[6])} for lost updates of save after another WOrmLmdb instance saved`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = { n: 1, nDeleted: 1, ok: 1 }
    it(`should get ${JSON.stringify(vans[7])} for delAll by instance with useCache after another instance changed records`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = ['r1', 'r2']
    it(`should get ${JSON.stringify(vans[8])} for records kept after delAll by instance with useCache`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

})
