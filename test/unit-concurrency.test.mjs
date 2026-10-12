import assert from 'assert'
import path from 'path'
import { fork } from 'child_process'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


//同一程序內大量併發讀取, 且其他程序同時持續寫入: 讀取不得失敗, 且讀交易不得因併發而增生
//讀取前須重設讀交易(跨程序可見性), 若select之迭代跨越讓出點(for await), 併發之select會交錯持有讀交易,
//重設遇交易使用中且已有新提交時lmdb-js即另開讀交易, reader slot(預設126, 跨程序共用)耗盡而MDB_READERS_FULL
describe('concurrency', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/concurrency'
    let C = 1000

    before(async function() {

        w.fsDeleteFolder(url)

        let wo = WOrm({ url, db: 'worm', cl: 'cc' })
        await wo.insert(_.times(1000, (i) => {
            return { id: `r${i}`, v: i, s: 'x'.repeat(50) }
        }))

        //child, 他程序持續寫入並取樣本程序所占之reader slot
        let child = fork(path.resolve('./test/tools/rawLmdbWriter.mjs'), [path.resolve(`${url}/worm/cc`)])
        await new Promise((resolve) => {
            child.once('message', resolve)
        })
        let ask = (m) => new Promise((resolve) => {
            child.once('message', resolve)
            child.send(m)
        })
        await ask({ cmd: 'loopStart', pid: process.pid })
        await new Promise((resolve) => setTimeout(resolve, 50))
        let mk = await ask({ cmd: 'mark' })

        //併發select與selectByPk
        let pms = []
        for (let i = 0; i < C; i++) {
            pms.push(wo.select())
            pms.push(wo.selectByPk(`r${i}`))
        }
        let rs = await Promise.allSettled(pms)
        let st = await ask({ cmd: 'loopStop' })

        vget[1] = _.size(_.filter(rs, { status: 'rejected' }))
        vget[2] = st.maxSlots <= 2
        vget[3] = (st.nCommit - mk.nCommit) >= 100

        //讀取結果正確: select皆為全表(含他程序寫入者, 至少1000筆), selectByPk皆為該主鍵之數據
        vget[4] = _.every(rs, function(r, k) {
            if (r.status !== 'fulfilled') {
                return false
            }
            if (k % 2 === 0) {
                return _.size(r.value) >= 1000
            }
            return _.get(r.value, 'id') === `r${(k - 1) / 2}`
        })

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

    vans[1] = 0
    it(`should get ${JSON.stringify(vans[1])} for rejected count in ${C} concurrent select and selectByPk while another process writing`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    //讀交易不因併發而增生: 本程序所占之reader slot不超過2
    vans[2] = true
    it(`should get ${JSON.stringify(vans[2])} for reader slots not exceeding 2 in concurrent reading`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    //前提核對: 他程序於併發讀取期間確有提交(至少100次), 否則上二項不具鑑別力
    vans[3] = true
    it(`should get ${JSON.stringify(vans[3])} for another process committing during concurrent reading`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = true
    it(`should get ${JSON.stringify(vans[4])} for results of concurrent select and selectByPk being correct`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

})
