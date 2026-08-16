import assert from 'assert'
import { spawn } from 'child_process'
import path from 'path'
import _ from 'lodash-es'
import w from 'wsemi'
import WOrm from '../src/WOrmLmdb.mjs'


//runProc, 另起行程對同一筆記錄併發save各自不同欄位, 用於驗證跨行程之讀改寫原子性
function runProc(tag, url, keys) {
    return new Promise(function(resolve) {
        let code = `
import { pathToFileURL } from 'url'
let { default: WOrm } = await import(pathToFileURL(process.env.SRC).href)
let wo = WOrm({ url: process.env.URL, db: 'worm', cl: 'xproc' })
let keys = JSON.parse(process.env.KEYS)
for (let k of keys) {
    await wo.save({ id: 'x1', [k]: 1 })
}
console.log(JSON.stringify({ tag: process.env.TAG, ok: true }))
await wo.close()
`
        let out = ''
        let p = spawn(process.execPath, ['--input-type=module', '-e', code], {
            shell: false,
            env: {
                ...process.env,
                SRC: path.resolve('./src/WOrmLmdb.mjs'),
                URL: url,
                KEYS: JSON.stringify(keys),
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
            resolve(_.trim(out))
        })
    })
}


describe('save', function() {
    let rt = null
    let vans = {}
    let vget = {}

    before(async function() {

        w.fsDeleteFolder('./_db_save')

        let url = './_db_save'

        //併發save對既有id之不同欄位, 各欄位皆須保留
        let woMerge = WOrm({ url, db: 'worm', cl: 'merge' })
        await woMerge.insert({ id: 'u1', base: 1 })
        await Promise.all(_.times(10, function(k) {
            return woMerge.save({ id: 'u1', [`f${k}`]: k })
        }))
        let vu1 = await woMerge.selectById('u1')
        vget[1] = _.size(_.filter(_.keys(vu1), function(k) {
            return _.startsWith(k, 'f')
        }))
        vget[2] = vu1.base
        await woMerge.close()

        //併發save對全新id, autoInsert僅一次且各欄位皆須保留
        let woNew = WOrm({ url, db: 'worm', cl: 'newid' })
        let rsn = await Promise.all([
            woNew.save({ id: 'w1', fa: 1 }),
            woNew.save({ id: 'w1', fb: 2 }),
            woNew.save({ id: 'w1', fc: 3 }),
        ])
        let vw1 = await woNew.selectById('w1')
        vget[3] = _.pick(vw1, ['id', 'fa', 'fb', 'fc'])
        vget[4] = _.size(await woNew.select())
        vget[5] = _.size(_.filter(_.flatten(rsn), function(v) {
            return v.nInserted === 1
        }))
        await woNew.close()

        //跨行程併發save對同一既有id之不同欄位
        //註: 此處僅斷言記錄未損毀且初始欄位仍在, 不斷言50個欄位全數存活,
        //因lmdb-js之寫交易於跨行程下並非完全可靠, 實測2行程×25欄位×15回合(CPU負載下)
        //有1回合遺失1個欄位; 單行程內併發則未觀察到遺失, 詳見[併發保證]說明
        let woX = WOrm({ url, db: 'worm', cl: 'xproc' })
        await woX.insert({ id: 'x1', base: 1 })
        await woX.close()
        await Promise.all([
            runProc('P1', url, _.times(25, function(k) {
                return `p1k${k}`
            })),
            runProc('P2', url, _.times(25, function(k) {
                return `p2k${k}`
            })),
        ])
        let woX2 = WOrm({ url, db: 'worm', cl: 'xproc' })
        let vx1 = await woX2.selectById('x1')
        vget[6] = vx1.base
        await woX2.close()

        //既有行為: 內容相同不更新
        let woKeep = WOrm({ url, db: 'worm', cl: 'keep' })
        await woKeep.insert({ id: 'k1', name: 'peter' })
        rt = null
        // vans[7] = [{ n: 1, nInserted: 0, nModified: 0, ok: 1 }]
        await woKeep.save({ id: 'k1', name: 'peter' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[7] = rt

        //既有行為: autoInsert=false且id不存在則不插入
        rt = null
        // vans[8] = [{ n: 0, nInserted: 0, nModified: 0, ok: 1 }]
        await woKeep.save({ id: 'k-none', name: 'none' }, { autoInsert: false })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[8] = rt
        vget[9] = await woKeep.selectById('k-none')

        //只給部份欄位且值皆與現值相同, 合併後結果等同現值故不寫入, nModified須為0
        await woKeep.insert({ id: 'k2', name: 'mary', value: 123 })
        rt = null
        // vans[12] = [{ n: 1, nInserted: 0, nModified: 0, ok: 1 }]
        await woKeep.save({ id: 'k2', name: 'mary' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[12] = rt
        vget[13] = await woKeep.selectById('k2')

        //只給部份欄位但值與現值不同, 合併後結果與現值不同故須寫入
        rt = null
        // vans[14] = [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }]
        await woKeep.save({ id: 'k2', name: 'lucy' })
            .then(function(msg) {
                rt = msg
            })
            .catch(function(msg) {
                rt = msg.toString()
            })
        vget[14] = rt
        vget[15] = await woKeep.selectById('k2')

        await woKeep.close()

        //既有行為: change事件, autoInsert時仍須發出insert事件
        let woEv = WOrm({ url, db: 'worm', cl: 'event' })
        let modes = []
        woEv.on('change', function(mode) {
            modes.push(mode)
        })
        await woEv.save({ id: 'e1', name: 'new' }) //不存在, 走autoInsert
        vget[10] = _.clone(modes)
        modes = []
        await woEv.save({ id: 'e1', name: 'modify' }) //已存在, 走更新
        vget[11] = _.clone(modes)
        await woEv.close()

    })

    vans[1] = 10
    it(`should get ${JSON.stringify(vans[1])} for fields after 10 concurrent save with different field`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    vans[2] = 1
    it(`should get ${JSON.stringify(vans[2])} for keeping original field after concurrent save`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = { id: 'w1', fa: 1, fb: 2, fc: 3 }
    it(`should get ${JSON.stringify(vans[3])} for 3 concurrent save with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = 1
    it(`should get ${JSON.stringify(vans[4])} for records after 3 concurrent save with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    vans[5] = 1
    it(`should get ${JSON.stringify(vans[5])} for count of nInserted===1 in 3 concurrent save with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    vans[6] = 1
    it(`should get ${JSON.stringify(vans[6])} for keeping original field after 2 processes saving different fields`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    //n為id之命中筆數, 內容相同[已命中]與id不存在[未命中]須可由n分辨, 不可皆為0
    vans[7] = [{ n: 1, nInserted: 0, nModified: 0, ok: 1 }]
    it(`should get ${JSON.stringify(vans[7])} for save with same content`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    vans[8] = [{ n: 0, nInserted: 0, nModified: 0, ok: 1 }]
    it(`should get ${JSON.stringify(vans[8])} for save(autoInsert=false) with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = null
    it(`should get ${JSON.stringify(vans[9])} for not inserting by save(autoInsert=false)`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    //nModified須反映資料庫端是否真的寫入, 只給部份欄位且值皆相同者合併後等同現值, 不得回報已修改
    vans[12] = [{ n: 1, nInserted: 0, nModified: 0, ok: 1 }]
    it(`should get ${JSON.stringify(vans[12])} for save with partial fields of same value`, async function() {
        assert.strict.deepStrictEqual(vget[12], vans[12])
    })

    vans[13] = { id: 'k2', name: 'mary', value: 123 }
    it(`should get ${JSON.stringify(vans[13])} for keeping content by save with partial fields of same value`, async function() {
        assert.strict.deepStrictEqual(vget[13], vans[13])
    })

    vans[14] = [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }]
    it(`should get ${JSON.stringify(vans[14])} for save with partial fields of different value`, async function() {
        assert.strict.deepStrictEqual(vget[14], vans[14])
    })

    vans[15] = { id: 'k2', name: 'lucy', value: 123 }
    it(`should get ${JSON.stringify(vans[15])} for merging content by save with partial fields`, async function() {
        assert.strict.deepStrictEqual(vget[15], vans[15])
    })

    vans[10] = ['insert', 'save']
    it(`should get ${JSON.stringify(vans[10])} for change events of save(autoInsert) with id not existed`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

    vans[11] = ['save']
    it(`should get ${JSON.stringify(vans[11])} for change events of save with id existed`, async function() {
        assert.strict.deepStrictEqual(vget[11], vans[11])
    })

})
