import assert from 'assert'
import { spawn } from 'child_process'
import path from 'path'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


//runProc, 另起行程對同一資料表插入相同id, 用於驗證跨行程之插入原子性
function runProc(tag, url, ids) {
    return new Promise(function(resolve) {
        let code = `
import { pathToFileURL } from 'url'
let { default: WOrm } = await import(pathToFileURL(process.env.SRC).href)
let wo = WOrm({ url: process.env.URL, db: 'worm', cl: 'race' })
let ids = JSON.parse(process.env.IDS)
let n = 0
for (let id of ids) {
    let r = await wo.insert({ id, from: process.env.TAG })
    n += r.nInserted
}
console.log(JSON.stringify({ tag: process.env.TAG, nInserted: n }))
await wo.close()
`
        let out = ''
        let p = spawn(process.execPath, ['--input-type=module', '-e', code], {
            shell: false,
            env: {
                ...process.env,
                SRC: path.resolve('./src/WOrmLmdb.mjs'),
                URL: url,
                IDS: JSON.stringify(ids),
                TAG: tag,
            },
        })
        p.stdout.on('data', function(d) {
            out += d.toString()
        })
        p.stderr.on('data', function(d) {
            out += d.toString()
        })
        p.on('close', function() {
            let r = null
            try {
                r = JSON.parse(_.trim(out))
            }
            catch (err) {
                r = { tag, error: _.trim(out) }
            }
            resolve(r)
        })
    })
}


describe('insert', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_insert')

        let url = './_db_insert'

        //insert同批含重複id, 僅首筆應被插入
        rt = null
        // vans[1] = { n: 3, nInserted: 2, ok: 1 }
        let woDup = WOrm({ url, db: 'worm', cl: 'dup' })
        await woDup.insert([
            { id: 'id-dup', name: 'first' },
            { id: 'id-dup', name: 'second' },
            { id: 'id-other', name: 'other' },
        ])
            .then(function(msg) {
                // console.log('insert(含重複id) then', msg)
                // insert(含重複id) then { n: 3, nInserted: 2, ok: 1 }
                rt = msg
            })
            .catch(function(msg) {
                // console.log('insert(含重複id) catch', msg)
                rt = msg.toString()
            })
        vget[1] = rt

        //同批重複id時應保留首筆內容
        vget[2] = await woDup.selectByPk('id-dup')

        //對已存在id再insert, 應不插入且不覆寫
        rt = null
        // vans[3] = { n: 1, nInserted: 0, ok: 1 }
        await woDup.insert({ id: 'id-dup', name: 'third' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[3] = rt
        vget[4] = await woDup.selectByPk('id-dup')
        await woDup.close()

        //單行程併發: 對同一id併發insert 100次, nInserted總和須為1
        let woPar = WOrm({ url, db: 'worm', cl: 'par' })
        let rs = await Promise.all(_.times(100, function(k) {
            return woPar.insert({ id: 'id-race', k })
        }))
        vget[5] = _.sum(_.map(rs, 'nInserted'))
        vget[6] = _.size(await woPar.select())
        await woPar.close()

        //跨行程併發: 兩行程各自對同50個id插入, 資料表須恰為50筆而無重複
        //註: 此處不斷言nInserted總和為50, 因lmdb-js之條件寫入於跨行程下並非完全可靠,
        //實測3行程×50id×30回合(CPU負載下)有5回合總和多算1至2, 記錄筆數則始終正確,
        //ifNoExists、transaction、transactionSync三種機制皆有相同現象, 詳見[併發保證]說明
        let ids = _.times(50, function(k) {
            return `id-x${k}`
        })
        await Promise.all([
            runProc('P1', url, ids),
            runProc('P2', url, ids),
        ])
        let woX = WOrm({ url, db: 'worm', cl: 'race' })
        vget[7] = _.size(await woX.select())
        vget[8] = _.size(_.uniq(_.map(await woX.select(), 'id')))
        await woX.close()

    })

    vans[1] = { n: 3, nInserted: 2, ok: 1 }
    it(`should get ${JSON.stringify(vans[1])} for insert with duplicated id in same batch`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = { id: 'id-dup', name: 'first' }
    it(`should get ${JSON.stringify(vans[2])} for keeping first one of duplicated id`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = { n: 1, nInserted: 0, ok: 1 }
    it(`should get ${JSON.stringify(vans[3])} for insert existed id`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = { id: 'id-dup', name: 'first' }
    it(`should get ${JSON.stringify(vans[4])} for not overwriting existed id`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = 1
    it(`should get ${JSON.stringify(vans[5])} for sum of nInserted by 100 concurrent insert with same id`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = 1
    it(`should get ${JSON.stringify(vans[6])} for records after 100 concurrent insert with same id`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = 50
    it(`should get ${JSON.stringify(vans[7])} for records after 2 processes inserting same 50 ids`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = 50
    it(`should get ${JSON.stringify(vans[8])} for distinct ids after 2 processes inserting same 50 ids`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

})
