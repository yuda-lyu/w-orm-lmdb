import assert from 'assert'
import path from 'path'
import fs from 'fs'
import { fork } from 'child_process'
import _ from 'lodash-es'
import w from 'wsemi'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//讀取錯誤不得回報為查無: 錯誤時selectByPk須reject, save與del該筆須ok為0並附err(T4、各函數規格)
//以裸lmdb之binary編碼寫入無法解碼之值(截斷之msgpack字串), 令讀取該筆時拋出解碼錯誤
describe('readerror', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/readerror'

    before(async function() {

        w.fsDeleteFolder(url)

        //寫入無法解碼之值, 開啟參數除編碼外同src/WOrmLmdb.mjs
        let storage = path.resolve(`${url}/worm/re`)
        fs.mkdirSync(path.dirname(storage), { recursive: true })
        let raw = open({ path: storage, compression: true, useVersions: true, encoding: 'binary' })
        await raw.put('bad', Buffer.from([0xd9, 0x50, 0x41]))
        await raw.close()

        let wo = WOrm({ url, db: 'worm', cl: 're' })
        await wo.insert({ id: 'g1', v: 1 })
        let evs = []
        wo.on('error', function(mode) {
            evs.push(mode)
        })

        //st, 只取結果之型態與形狀, err欄位只核對為非空字串
        let st = (pm) => pm
            .then(function(r) {
                if (_.isArray(r)) {
                    return {
                        type: 'resolve',
                        rs: _.map(r, function(v) {
                            return _.omit(v, 'err')
                        }),
                        errs: _.map(r, function(v) {
                            return w.isestr(v.err)
                        }),
                    }
                }
                return { type: 'resolve', r }
            })
            .catch(function(err) {
                return { type: 'reject', hasMsg: w.isestr(_.get(err, 'message')) }
            })

        vget[1] = await st(wo.select())
        vget[2] = await st(wo.selectByPk('bad'))
        vget[3] = await st(wo.selectByPk('g1'))
        vget[4] = await st(wo.save({ id: 'bad', v: 2 }))
        vget[5] = await st(wo.del({ id: 'bad' }))
        vget[6] = await st(wo.del([{ id: 'bad' }, { id: 'g1' }]))
        vget[7] = await st(wo.delAll())

        //insert與insertBulk之存在判定皆為鍵層而不解碼數據: insert視為已存在而跳過, insertBulk視為主鍵衝突
        vget[9] = await st(wo.insert({ id: 'bad', v: 3 }))
        vget[10] = await wo.insertBulk([{ id: 'bad', v: 3 }]).then(() => 'resolve', (err) => err.message)

        vget[8] = evs

        await wo.close()

    })

    after(function() {
        w.fsDeleteFolder(url)
    })

    vans[1] = { type: 'reject', hasMsg: true }
    it(`should get ${JSON.stringify(vans[1])} for select with undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    //不可回null, 否則讀取失敗與[查無資料]同形
    vans[2] = { type: 'reject', hasMsg: true }
    it(`should get ${JSON.stringify(vans[2])} for selectByPk on undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = { type: 'resolve', r: { id: 'g1', v: 1 } }
    it(`should get ${JSON.stringify(vans[3])} for selectByPk on other record`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = { type: 'resolve', rs: [{ n: 1, nInserted: 0, nModified: 0, ok: 0 }], errs: [true] }
    it(`should get ${JSON.stringify(vans[4])} for save on undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

    //不可回[{n:0,nDeleted:0,ok:1}]之正常未命中結果
    vans[5] = { type: 'resolve', rs: [{ n: 1, nDeleted: 0, ok: 0 }], errs: [true] }
    it(`should get ${JSON.stringify(vans[5])} for del on undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[5], vans[5])
    })

    //單筆失敗不中斷整批
    vans[6] = { type: 'resolve', rs: [{ n: 1, nDeleted: 0, ok: 0 }, { n: 1, nDeleted: 1, ok: 1 }], errs: [true, false] }
    it(`should get ${JSON.stringify(vans[6])} for del by array mixing undecodable and normal records`, async function() {
        assert.strict.deepStrictEqual(vget[6], vans[6])
    })

    vans[7] = { type: 'reject', hasMsg: true }
    it(`should get ${JSON.stringify(vans[7])} for delAll with undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[7], vans[7])
    })

    //整批性錯誤與逐筆失敗皆發出error事件
    vans[8] = ['select', 'selectByPk', 'save', 'del', 'del', 'delAll', 'insertBulk']
    it(`should get ${JSON.stringify(vans[8])} for error events`, async function() {
        assert.strict.deepStrictEqual(vget[8], vans[8])
    })

    vans[9] = { type: 'resolve', r: { n: 1, nInserted: 0, ok: 1 } }
    it(`should get ${JSON.stringify(vans[9])} for insert on undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[9], vans[9])
    })

    vans[10] = 'can not insertBulk by existed id[bad]'
    it(`should get ${JSON.stringify(vans[10])} for insertBulk on undecodable record`, async function() {
        assert.strict.deepStrictEqual(vget[10], vans[10])
    })

})


//無法解碼之數據使select反覆失敗, 且他程序持續提交: 每次失敗皆須歸還讀交易, 不得累積占用reader slot,
//否則約126次後本程序與其他程序之讀取皆為MDB_READERS_FULL(迭代之next()拋錯時for...of不會呼叫return())
describe('readerror reader slot', function() {
    let vans = {}
    let vget = {}
    let url = './test/_tmp/readerror-slot'
    let N = 150

    before(async function() {

        w.fsDeleteFolder(url)

        let storage = path.resolve(`${url}/worm/rs`)
        fs.mkdirSync(path.dirname(storage), { recursive: true })
        let raw = open({ path: storage, compression: true, useVersions: true, encoding: 'binary' })
        await raw.put('bad', Buffer.from([0xd9, 0x50, 0x41]))
        await raw.close()

        let wo = WOrm({ url, db: 'worm', cl: 'rs' })
        await wo.insert({ id: 'g1', v: 1 })

        //child, 他程序持續寫入並取樣本程序所占之reader slot
        let child = fork(path.resolve('./test/tools/rawLmdbWriter.mjs'), [storage])
        await new Promise((resolve) => {
            child.once('message', resolve)
        })
        let ask = (m) => new Promise((resolve) => {
            child.once('message', resolve)
            child.send(m)
        })
        await ask({ cmd: 'loopStart', pid: process.pid })

        //反覆select, 每次之間讓出計時器令他程序之提交介入
        let msgs = []
        for (let i = 0; i < N; i++) {
            await wo.select()
                .then(() => {
                    msgs.push('resolve')
                })
                .catch((err) => {
                    msgs.push(String(_.get(err, 'message', err)))
                })
            await new Promise((resolve) => setTimeout(resolve, 2))
        }
        let stat = await ask({ cmd: 'loopStop' })

        vget[1] = _.size(_.filter(msgs, (m) => m.indexOf('MDB_READERS_FULL') >= 0))
        vget[2] = _.size(_.filter(msgs, (m) => m !== 'resolve'))
        vget[3] = stat.maxSlots <= 2
        vget[4] = await wo.selectByPk('g1').catch((err) => `reject: ${_.get(err, 'message', err)}`)

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
    it(`should get ${JSON.stringify(vans[1])} for MDB_READERS_FULL in ${N} failed select`, async function() {
        assert.strict.deepStrictEqual(vget[1], vans[1])
    })

    //前提核對: 每次select皆因無法解碼之數據而reject
    vans[2] = N
    it(`should get ${JSON.stringify(vans[2])} for rejected count of select`, async function() {
        assert.strict.deepStrictEqual(vget[2], vans[2])
    })

    vans[3] = true
    it(`should get ${JSON.stringify(vans[3])} for reader slots not exceeding 2 after repeated failures`, async function() {
        assert.strict.deepStrictEqual(vget[3], vans[3])
    })

    vans[4] = { id: 'g1', v: 1 }
    it(`should get ${JSON.stringify(vans[4])} for selectByPk on normal record after repeated failures`, async function() {
        assert.strict.deepStrictEqual(vget[4], vans[4])
    })

})
