import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { fork } from 'child_process'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


//跨程序可見性: 讀取須可見本次呼叫前已提交之資料, 含其他程序之提交
//情境: 本程序先讀一次, 請他程序寫入並等其提交, 隨即操作
//lmdb-js之讀交易預設延至下一個timers階段才重設, 其他程序之提交於此前不可見
//等待提交時不讓出事件迴圈(同步等待他程序提交後建立之標記檔), 令先行讀取與其後之操作落於同一macrotask,
//重設之計時器無從觸發, 故未於讀前重設者必讀到提交前之快照, 修正前後之結果皆為確定性
//他程序以裸lmdb寫入(test/tools/rawLmdbWriter.mjs), 無讀取路徑, 故異常皆歸因於本程序之讀取
describe('crossprocess', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/crossprocess'
    let N = 2

    before(async function() {

        w.fsDeleteFolder(url)

        let wo = WOrm({ url, db: 'worm', cl: 'xp' })
        await wo.insert({ id: 'seed', v: 'A' }) //先建立資料表

        //child, 他程序
        let child = fork(path.resolve('./test/tools/rawLmdbWriter.mjs'), [path.resolve(`${url}/worm/xp`)])
        await new Promise((resolve) => {
            child.once('message', resolve)
        })
        //commit, 請他程序寫入並同步等待其提交, 期間不讓出事件迴圈; 他程序之回覆訊息不需處理
        let seq = 0
        let commit = (m) => {
            let marker = path.resolve(`${url}/marker-${seq++}`)
            child.send({ ...m, marker })
            let t0 = Date.now()
            while (!fs.existsSync(marker)) {
                if (Date.now() - t0 > 10000) {
                    throw new Error('timeout for waiting commit of another process')
                }
            }
        }
        let put = (id, value) => commit({ cmd: 'put', id, value })
        let remove = (id) => commit({ cmd: 'remove', id })

        let pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

        //priorRead, 本程序先讀一次, 令讀交易處於已建立而尚未被計時器重設之狀態
        let priorRead = () => wo.selectByPk('seed')

        //final, 讓出計時器後再讀, 取終值
        let final = async (id, key = 'v') => {
            await pause(2)
            let r = await wo.selectByPk(id)
            return r === null ? null : r[key]
        }

        //rec, 逐次記錄觀察值
        let rec = (k, v) => {
            if (!vget[k]) {
                vget[k] = []
            }
            vget[k].push(v)
        }

        for (let i = 0; i < N; i++) {
            let id = (k) => `c${k}_${i}`

            //1 select x 他程序新增
            await priorRead()
            put(id(1), { id: id(1), v: 'B' })
            rec(1, _.size(await wo.select({ id: id(1) })))

            //2 select x 他程序更新
            await wo.insert({ id: id(2), v: 'old' })
            await priorRead()
            put(id(2), { id: id(2), v: 'new' })
            rec(2, _.get(await wo.select({ id: id(2) }), '0.v'))

            //3 select x 他程序刪除
            await wo.insert({ id: id(3), v: 'x' })
            await priorRead()
            remove(id(3))
            rec(3, _.size(await wo.select({ id: id(3) })))

            //4 selectByPk x 他程序新增
            await priorRead()
            put(id(4), { id: id(4), v: 'B' })
            rec(4, _.get(await wo.selectByPk(id(4)), 'v', null))

            //5 selectByPk x 他程序更新
            await wo.insert({ id: id(5), v: 'old' })
            await priorRead()
            put(id(5), { id: id(5), v: 'new' })
            rec(5, _.get(await wo.selectByPk(id(5)), 'v', null))

            //6 selectByPk x 他程序刪除
            await wo.insert({ id: id(6), v: 'x' })
            await priorRead()
            remove(id(6))
            rec(6, await wo.selectByPk(id(6)))

            //7 save與本程序所知之現值相同 x 他程序已更新: 本次save在後, 須寫入
            await wo.save({ id: id(7), v: 'X' })
            await priorRead()
            put(id(7), { id: id(7), v: 'Y' })
            rec(7, { rs: await wo.save({ id: id(7), v: 'X' }), fv: await final(id(7)) })

            //8 save與本程序所知之現值相同 x 他程序已刪除: 須依autoInsert寫回
            await wo.save({ id: id(8), v: 'X' })
            await priorRead()
            remove(id(8))
            rec(8, { rs: await wo.save({ id: id(8), v: 'X' }), fv: await final(id(8)) })

            //9 save與他程序新增之內容相同 x 他程序新增
            await priorRead()
            put(id(9), { id: id(9), v: 'B' })
            rec(9, { rs: await wo.save({ id: id(9), v: 'B' }), fv: await final(id(9)) })

            //10 save與現值不同 x 他程序新增
            await priorRead()
            put(id(10), { id: id(10), v: 'B' })
            rec(10, { rs: await wo.save({ id: id(10), v: 'A' }), fv: await final(id(10)) })

            //11 save與現值不同 x 他程序已更新
            await wo.save({ id: id(11), v: 'X' })
            await priorRead()
            put(id(11), { id: id(11), v: 'Y' })
            rec(11, { rs: await wo.save({ id: id(11), v: 'Z' }), fv: await final(id(11)) })

            //12 save與現值不同 x 他程序已刪除
            await wo.save({ id: id(12), v: 'X' })
            await priorRead()
            remove(id(12))
            rec(12, { rs: await wo.save({ id: id(12), v: 'Z' }), fv: await final(id(12)) })

            //13 del x 他程序新增
            await priorRead()
            put(id(13), { id: id(13), v: 'B' })
            rec(13, { rs: await wo.del({ id: id(13) }), fv: await final(id(13)) })

            //14 del x 他程序更新
            await wo.insert({ id: id(14), v: 'x' })
            await priorRead()
            put(id(14), { id: id(14), v: 'y' })
            rec(14, { rs: await wo.del({ id: id(14) }), fv: await final(id(14)) })

            //15 del x 他程序已刪除: 未命中
            await wo.insert({ id: id(15), v: 'x' })
            await priorRead()
            remove(id(15))
            rec(15, await wo.del({ id: id(15) }))

            //16 delAll(find) x 他程序新增符合條件者
            await priorRead()
            put(id(16), { id: id(16), tag: id(16) })
            rec(16, { rs: await wo.delAll({ tag: id(16) }), fv: await final(id(16), 'tag') })

            //17 delAll(find) x 他程序把列改為符合條件
            await wo.insert({ id: id(17), tag: 'other' })
            await priorRead()
            put(id(17), { id: id(17), tag: id(17) })
            rec(17, { rs: await wo.delAll({ tag: id(17) }), fv: await final(id(17), 'tag') })

            //18 delAll(find) x 他程序把列改為不符條件: 不得誤刪
            await wo.insert({ id: id(18), tag: id(18) })
            await priorRead()
            put(id(18), { id: id(18), tag: 'keep' })
            rec(18, { rs: await wo.delAll({ tag: id(18) }), fv: await final(id(18), 'tag') })

            //19 delAll(find) x 他程序已刪除符合條件者: 實際刪除0筆
            await wo.insert({ id: id(19), tag: id(19) })
            await priorRead()
            remove(id(19))
            rec(19, await wo.delAll({ tag: id(19) }))

            //20 insert x 他程序新增同id: 已存在則跳過
            await priorRead()
            put(id(20), { id: id(20), v: 'B' })
            rec(20, { rs: await wo.insert({ id: id(20), v: 'A' }), fv: await final(id(20)) })

            //21 insert x 他程序更新
            await wo.insert({ id: id(21), v: 'x' })
            await priorRead()
            put(id(21), { id: id(21), v: 'B2' })
            rec(21, { rs: await wo.insert({ id: id(21), v: 'A' }), fv: await final(id(21)) })

            //22 insert x 他程序已刪除: 不存在則插入
            await wo.insert({ id: id(22), v: 'x' })
            await priorRead()
            remove(id(22))
            rec(22, { rs: await wo.insert({ id: id(22), v: 'A' }), fv: await final(id(22)) })

            //23 insertBulk x 他程序新增同id: 衝突整批reject
            await priorRead()
            put(id(23), { id: id(23), v: 'B' })
            rec(23, await wo.insertBulk([{ id: id(23), v: 'A' }]).then(() => 'resolve', () => 'reject'))

            //24 insertBulk x 他程序更新: 衝突整批reject
            await wo.insert({ id: id(24), v: 'x' })
            await priorRead()
            put(id(24), { id: id(24), v: 'B2' })
            rec(24, await wo.insertBulk([{ id: id(24), v: 'A' }]).then(() => 'resolve', () => 'reject'))

            //25 insertBulk x 他程序已刪除: 無衝突全數插入
            await wo.insert({ id: id(25), v: 'x' })
            await priorRead()
            remove(id(25))
            rec(25, { rs: await wo.insertBulk([{ id: id(25), v: 'A' }]), fv: await final(id(25)) })

            //26 save(autoInsert為false)與本程序所知之現值相同 x 他程序已刪除: 未命中, n為0
            await wo.save({ id: id(26), v: 'X' })
            await priorRead()
            remove(id(26))
            rec(26, { rs: await wo.save({ id: id(26), v: 'X' }, { autoInsert: false }), fv: await final(id(26)) })

            //27 save只給部份欄位且與本程序所知之現值相同 x 他程序已更新該欄: 本次save在後, 須寫入且保留未給之欄位
            await wo.insert({ id: id(27), a: 1, b: 2 })
            await priorRead()
            put(id(27), { id: id(27), a: 9, b: 2 })
            let rs27 = await wo.save({ id: id(27), a: 1 })
            await pause(2)
            rec(27, { rs: rs27, fv: _.pick(await wo.selectByPk(id(27)), ['a', 'b']) })

        }

        //exit
        child.send({ cmd: 'exit' })
        await new Promise((resolve) => {
            child.once('exit', resolve)
        })
        await wo.close()

    })

    after(function() {
        w.fsDeleteFolder(url)
    })

    let times = (v) => _.times(N, () => _.cloneDeep(v))

    vans[1] = times(1)
    it(`should get ${JSON.stringify(vans[1])} for select after another process inserted`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = times('new')
    it(`should get ${JSON.stringify(vans[2])} for select after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = times(0)
    it(`should get ${JSON.stringify(vans[3])} for select after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = times('B')
    it(`should get ${JSON.stringify(vans[4])} for selectByPk after another process inserted`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = times('new')
    it(`should get ${JSON.stringify(vans[5])} for selectByPk after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = times(null)
    it(`should get ${JSON.stringify(vans[6])} for selectByPk after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    //本次save在他程序之更新之後, 不得因預讀到舊值而略過寫入
    vans[7] = times({ rs: [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }], fv: 'X' })
    it(`should get ${JSON.stringify(vans[7])} for save with value same as known after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = times({ rs: [{ n: 1, nInserted: 1, nModified: 0, ok: 1 }], fv: 'X' })
    it(`should get ${JSON.stringify(vans[8])} for save with value same as known after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = times({ rs: [{ n: 1, nInserted: 0, nModified: 0, ok: 1 }], fv: 'B' })
    it(`should get ${JSON.stringify(vans[9])} for save with same value after another process inserted`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = times({ rs: [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }], fv: 'A' })
    it(`should get ${JSON.stringify(vans[10])} for save with different value after another process inserted`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    vans[11] = times({ rs: [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }], fv: 'Z' })
    it(`should get ${JSON.stringify(vans[11])} for save with different value after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

    vans[12] = times({ rs: [{ n: 1, nInserted: 1, nModified: 0, ok: 1 }], fv: 'Z' })
    it(`should get ${JSON.stringify(vans[12])} for save with different value after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = times({ rs: [{ n: 1, nDeleted: 1, ok: 1 }], fv: null })
    it(`should get ${JSON.stringify(vans[13])} for del after another process inserted`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

    vans[14] = times({ rs: [{ n: 1, nDeleted: 1, ok: 1 }], fv: null })
    it(`should get ${JSON.stringify(vans[14])} for del after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[14], vans[14])
    })

    vans[15] = times([{ n: 0, nDeleted: 0, ok: 1 }])
    it(`should get ${JSON.stringify(vans[15])} for del after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[15], vans[15])
    })

    vans[16] = times({ rs: { n: 1, nDeleted: 1, ok: 1 }, fv: null })
    it(`should get ${JSON.stringify(vans[16])} for delAll after another process inserted matched record`, async function() {
        assert.strict.deepStrictEqual(vget[16], vans[16])
    })

    vans[17] = times({ rs: { n: 1, nDeleted: 1, ok: 1 }, fv: null })
    it(`should get ${JSON.stringify(vans[17])} for delAll after another process updated record to match`, async function() {
        assert.strict.deepStrictEqual(vget[17], vans[17])
    })

    //他程序已改為不符條件之列不得被刪除
    vans[18] = times({ rs: { n: 0, nDeleted: 0, ok: 1 }, fv: 'keep' })
    it(`should get ${JSON.stringify(vans[18])} for delAll after another process updated record to unmatch`, async function() {
        assert.strict.deepStrictEqual(vget[18], vans[18])
    })

    vans[19] = times({ n: 0, nDeleted: 0, ok: 1 })
    it(`should get ${JSON.stringify(vans[19])} for delAll after another process deleted matched record`, async function() {
        assert.strict.deepStrictEqual(vget[19], vans[19])
    })

    vans[20] = times({ rs: { n: 1, nInserted: 0, ok: 1 }, fv: 'B' })
    it(`should get ${JSON.stringify(vans[20])} for insert after another process inserted same id`, async function() {
        assert.strict.deepStrictEqual(vget[20], vans[20])
    })

    vans[21] = times({ rs: { n: 1, nInserted: 0, ok: 1 }, fv: 'B2' })
    it(`should get ${JSON.stringify(vans[21])} for insert after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[21], vans[21])
    })

    vans[22] = times({ rs: { n: 1, nInserted: 1, ok: 1 }, fv: 'A' })
    it(`should get ${JSON.stringify(vans[22])} for insert after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[22], vans[22])
    })

    vans[23] = times('reject')
    it(`should get ${JSON.stringify(vans[23])} for insertBulk after another process inserted same id`, async function() {
        assert.strict.deepStrictEqual(vget[23], vans[23])
    })

    vans[24] = times('reject')
    it(`should get ${JSON.stringify(vans[24])} for insertBulk after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[24], vans[24])
    })

    vans[25] = times({ rs: { n: 1, nInserted: 1, ok: 1 }, fv: 'A' })
    it(`should get ${JSON.stringify(vans[25])} for insertBulk after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[25], vans[25])
    })

    //未命中不得因預讀到舊值而回報命中
    vans[26] = times({ rs: [{ n: 0, nInserted: 0, nModified: 0, ok: 1 }], fv: null })
    it(`should get ${JSON.stringify(vans[26])} for save(autoInsert:false) after another process deleted`, async function() {
        assert.strict.deepStrictEqual(vget[26], vans[26])
    })

    vans[27] = times({ rs: [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }], fv: { a: 1, b: 2 } })
    it(`should get ${JSON.stringify(vans[27])} for save with partial fields after another process updated`, async function() {
        assert.strict.deepStrictEqual(vget[27], vans[27])
    })

})
