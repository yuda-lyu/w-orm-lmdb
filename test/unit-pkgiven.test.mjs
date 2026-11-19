import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import WOrm from '../src/WOrmLmdb.mjs'
import { genRevoked, genTagThrow, genOwnKeysThrow, secrets } from './tools/hostile.mjs'


//主鍵(id)之「未帶」與「給了而不合格」之判定, 規則見sgud/rule/rulebook.md R-PKGIVEN-01(排程第557項規則一)
//規格(主控裁示; wsemi 1.9.69起判斷函數不收String包裝物件, 原procPk以isestr判定未帶而把非字串之主鍵靜默換成新主鍵):
//  S1 只有undefined、null與空字串(含鍵不存在)視為未帶主鍵: autoGenPk為true自動產生, 為false依既有整批性錯誤(can not <fn> by data[k] without valid id when autoGenPk is false)
//  S2 給了而非字串(數字、布林、物件、陣列、String包裝物件、Symbol、BigInt、函數、Date、讀取即拋錯之物件等)一律拒絕invalid id: not a string at [data[k].id], 不回顯值, 不得靜默換成新主鍵
//  S3 涵蓋所有收主鍵之入口(insert、insertBulk、save、del、selectByPk), 依各入口既有之失敗通道: insert與insertBulk整批reject(error事件恰一次, 不寫入)、
//     save與del該筆ok為0附err(逐筆)、selectByPk回null; 失敗通道與S2之訊息皆不隨autoGenPk改變
//每案使用全新資料表(含一筆種子), 以select核對資料庫(使用者面之唯一讀取管道)


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let root = path.join(__dirname, '_tmp', 'pkgiven')
let seq = 0


//withWo, 每案全新資料表並預先寫入種子(種子之事件不計), 案後關閉並刪除該資料夾
let withWo = async (fn, extraOpt = {}) => {
    seq++
    let cl = `c${seq}`
    let wo = WOrm({ url: root, db: 'worm', cl, ...extraOpt })
    try {
        await wo.insert({ id: 'seed', v: 1 })
        let evs = []
        wo.on('error', (mode, data, err) => {
            evs.push({ type: 'error', mode, err })
        })
        wo.on('change', (mode) => {
            evs.push({ type: 'change', mode })
        })
        await fn(wo, evs)
    }
    finally {
        await wo.close()
        fs.rmSync(path.join(root, 'worm', cl), { recursive: true, force: true })
    }
}

let settle = async (p) => {
    try {
        return { ok: true, v: await p }
    }
    catch (err) {
        return { ok: false, err }
    }
}

let msgOf = (err) => (err instanceof Error ? err.message : String(err))

let ids = async (wo) => (await wo.select()).map((v) => v.id).sort()

let nErr = (evs, mode) => evs.filter((v) => v.type === 'error' && (mode === undefined || v.mode === mode)).length

let noSecret = (...vals) => {
    let s = JSON.stringify(vals.map((v) => (v instanceof Error ? v.message : v)), (k, v) => (typeof v === 'bigint' ? String(v) : v))
    for (let x of secrets) {
        assert.ok(s.indexOf(x) < 0, `must not echo ${x}`)
    }
}


//給了而非字串之id(S2); 以工廠函數取值使每案皆為全新值
let badIds = [
    { name: 'number 5', make: () => 5 },
    { name: 'number 0', make: () => 0 },
    { name: 'NaN', make: () => NaN },
    { name: 'boolean true', make: () => true },
    { name: 'boolean false', make: () => false },
    { name: 'plain object', make: () => ({ a: 'SECRET-OBJ' }) },
    {
        name: 'object with toString',
        make: () => ({
            toString() {
                return 'SECRET-TS'
            }
        })
    },
    { name: 'array', make: () => ['SECRET-ARR'] },
    { name: 'empty array', make: () => [] },
    { name: 'String wrapper', make: () => new String('wrap') }, // eslint-disable-line no-new-wrappers
    { name: 'Number wrapper', make: () => new Number(5) }, // eslint-disable-line no-new-wrappers
    { name: 'Boolean wrapper', make: () => new Boolean(true) }, // eslint-disable-line no-new-wrappers
    { name: 'Symbol', make: () => Symbol('SYM-ID') },
    { name: 'BigInt', make: () => 10n },
    { name: 'function', make: () => () => 'f' },
    { name: 'Date', make: () => new Date(0) },
]

//讀取即拋錯之id: 以類別實例之原型存取器回傳(經cloneDeep仍保留原型, 走到主鍵判定); 值本身為不可讀者
let unreadableIds = [
    { name: 'revoked Proxy via prototype accessor', make: genRevoked },
    { name: 'Symbol.toStringTag throws via prototype accessor', make: genTagThrow },
    { name: 'ownKeys throws via prototype accessor', make: genOwnKeysThrow },
]
let mkInh = (make) => {
    class C {
        constructor() {
            this.name = 'n'
        }

        get id() {
            return make()
        }
    }
    return new C()
}

let reMsg = 'invalid id: not a string at [data[0].id]'


describe('pkgiven', function() {

    before(function() {
        fs.rmSync(root, { recursive: true, force: true })
        fs.mkdirSync(root, { recursive: true })
    })

    after(function() {
        fs.rmSync(root, { recursive: true, force: true })
    })


    for (let autoGenPk of [true, false]) {

        describe(`given non-string id is rejected, never replaced (autoGenPk: ${autoGenPk})`, function() {

            //S2、S3: insert、insertBulk整批reject, 不寫入, error事件恰一次且先於reject, 不發change
            for (let api of ['insert', 'insertBulk']) {
                for (let c of badIds) {
                    it(`${api}: ${c.name}`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api]({ id: c.make(), name: 'n' }))
                            assert.strictEqual(r.ok, false, 'must reject, not write a replaced id') //S2
                            assert.strictEqual(msgOf(r.err), reMsg) //S2
                            assert.deepStrictEqual(await ids(wo), ['seed']) //不得以新id寫入
                            assert.deepStrictEqual(evs, [{ type: 'error', mode: api, err: reMsg }]) //S3: error恰一次, 無change
                            noSecret(r.err, evs)
                        }, { autoGenPk })
                    })
                }
            }

            //S2、S3: save該筆ok為0附err(逐筆通道, 不隨autoGenPk改變為整批reject), 不寫入
            for (let c of badIds) {
                it(`save: ${c.name}`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo.save({ id: c.make(), name: 'n' })
                        assert.deepStrictEqual(r, [{ n: 1, nInserted: 0, nModified: 0, ok: 0, err: reMsg }]) //S2
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, 'save'), 1)
                        assert.strictEqual(evs.filter((v) => v.type === 'change' && v.mode === 'insert').length, 0) //未插入
                        noSecret(r, evs)
                    }, { autoGenPk })
                })
            }

            //S3: del既有逐筆通道(核實不改)
            for (let c of badIds) {
                it(`del: ${c.name} (existing per-item channel, unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo.del({ id: c.make() })
                        assert.deepStrictEqual(r, [{ n: 0, nDeleted: 0, ok: 0, err: reMsg }])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, 'del'), 1)
                        noSecret(r, evs)
                    }, { autoGenPk })
                })
            }

            //S3: selectByPk既有通道(核實不改): 非字串視為查無, 回null, 不發事件
            for (let c of badIds) {
                it(`selectByPk: ${c.name} (existing null channel, unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        assert.strictEqual(await wo.selectByPk(c.make()), null)
                        assert.deepStrictEqual(evs, [])
                    }, { autoGenPk })
                })
            }

            //S2: 讀取即拋錯之id(經原型存取器走到主鍵判定)同為給了而非字串, 依固定詞彙拒絕, 不替換
            for (let c of unreadableIds) {
                for (let api of ['insert', 'insertBulk']) {
                    it(`${api}: ${c.name}`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api](mkInh(c.make)))
                            assert.strictEqual(r.ok, false)
                            assert.strictEqual(msgOf(r.err), reMsg)
                            assert.deepStrictEqual(await ids(wo), ['seed'])
                            assert.deepStrictEqual(evs, [{ type: 'error', mode: api, err: reMsg }])
                            noSecret(r.err, evs)
                        }, { autoGenPk })
                    })
                }
                it(`save: ${c.name}`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo.save(mkInh(c.make))
                        assert.deepStrictEqual(r, [{ n: 1, nInserted: 0, nModified: 0, ok: 0, err: reMsg }])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, 'save'), 1)
                        noSecret(r, evs)
                    }, { autoGenPk })
                })
            }

            //S2: id值本身(自有屬性)為不可讀者於資料複製階段(cloneDeep)即拋錯: 屬既有行為(整批reject, error事件恰一次, 不寫入, 不補值), 訊息為該物件所拋之值
            for (let c of [{ name: 'revoked Proxy', make: genRevoked }, { name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                    it(`${api}: own property id is ${c.name}, rejected at the copy stage and never replaced`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api]({ id: c.make(), name: 'n' }))
                            assert.strictEqual(r.ok, false)
                            assert.deepStrictEqual(await ids(wo), ['seed'])
                            assert.strictEqual(nErr(evs, api), 1)
                            assert.strictEqual(evs.filter((v) => v.type === 'change').length, 0)
                        }, { autoGenPk })
                    })
                }
            }

        })

    }


    describe('not given id (undefined, null, empty string, absent key)', function() {

        let notGiven = [
            { name: 'absent key', make: () => ({ name: 'n' }) },
            { name: 'undefined', make: () => ({ id: undefined, name: 'n' }) },
            { name: 'null', make: () => ({ id: null, name: 'n' }) },
            { name: 'empty string', make: () => ({ id: '', name: 'n' }) },
        ]

        //S1: autoGenPk為true自動產生
        for (let api of ['insert', 'insertBulk', 'save']) {
            for (let c of notGiven) {
                it(`${api}: ${c.name} gets a generated string id (autoGenPk: true)`, async function() {
                    await withWo(async(wo) => {
                        await wo[api](c.make())
                        let rows = await wo.select()
                        assert.strictEqual(rows.length, 2)
                        let row = rows.find((v) => v.id !== 'seed')
                        assert.ok(typeof row.id === 'string' && row.id !== '' && row.name === 'n')
                    }, { autoGenPk: true })
                })
            }
        }

        //S1、S3: del與selectByPk之未帶(既有通道, 核實不改, 不隨autoGenPk改變): del該筆ok為0附err(空字串為string is empty, 其餘為not a string), selectByPk回null、不發事件
        for (let autoGenPk of [true, false]) {
            for (let c of notGiven) {
                it(`del: ${c.name} fails that row, not generated (existing per-item channel, unchanged; autoGenPk: ${autoGenPk})`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo.del(c.make())
                        let why = c.name === 'empty string' ? 'string is empty' : 'not a string'
                        assert.deepStrictEqual(r, [{ n: 0, nDeleted: 0, ok: 0, err: `invalid id: ${why} at [data[0].id]` }])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, 'del'), 1)
                    }, { autoGenPk })
                })
            }
            for (let [name, v] of [['undefined', undefined], ['null', null], ['empty string', '']]) {
                it(`selectByPk: ${name} -> null, no event (existing channel, unchanged; autoGenPk: ${autoGenPk})`, async function() {
                    await withWo(async(wo, evs) => {
                        assert.strictEqual(await wo.selectByPk(v), null)
                        assert.deepStrictEqual(evs, [])
                    }, { autoGenPk })
                })
            }
        }

        //S1: autoGenPk為false依既有整批性錯誤(三個寫入入口皆整批reject, 含save)
        for (let api of ['insert', 'insertBulk', 'save']) {
            for (let c of notGiven) {
                it(`${api}: ${c.name} rejects the whole batch (autoGenPk: false, unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api](c.make()))
                        assert.strictEqual(r.ok, false)
                        assert.strictEqual(msgOf(r.err), `can not ${api} by data[0] without valid id when autoGenPk is false`)
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, api), 1)
                    }, { autoGenPk: false })
                })
            }
        }

    })


    describe('batch and channel details', function() {

        for (let autoGenPk of [true, false]) {

            //S2、S3: 索引為該筆於批次之位置; insert與insertBulk整批不寫入(含同批有效筆)
            for (let api of ['insert', 'insertBulk']) {
                it(`${api}: a non-string id at index 1 rejects the whole batch at index 1, the valid row is not written (autoGenPk: ${autoGenPk})`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api]([{ id: 'ok1', name: 'a' }, { id: 5, name: 'b' }, { id: 'ok2', name: 'c' }]))
                        assert.strictEqual(r.ok, false)
                        assert.strictEqual(msgOf(r.err), 'invalid id: not a string at [data[1].id]')
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        assert.strictEqual(nErr(evs, api), 1)
                    }, { autoGenPk })
                })
            }

            //S3: save逐筆, 同批其他筆照常
            it(`save: a non-string id at index 1 fails only that row, the others are written (autoGenPk: ${autoGenPk})`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await wo.save([{ id: 'ok1', name: 'a' }, { id: 5, name: 'b' }, { id: 'ok2', name: 'c' }])
                    assert.deepStrictEqual(r.map((v) => v.ok), [1, 0, 1])
                    assert.strictEqual(r[1].err, 'invalid id: not a string at [data[1].id]')
                    assert.deepStrictEqual(await ids(wo), ['ok1', 'ok2', 'seed'])
                    assert.strictEqual(nErr(evs, 'save'), 1)
                }, { autoGenPk })
            })

            //S3: 同批含「未帶」與「給了不合格」: 後者之拒絕不因前者補值而改變
            it(`insert: not given and given non-string in one batch, the batch is rejected and nothing is written (autoGenPk: ${autoGenPk})`, async function() {
                await withWo(async(wo) => {
                    let r = await settle(wo.insert([{ name: 'a' }, { id: 5, name: 'b' }]))
                    assert.strictEqual(r.ok, false)
                    assert.strictEqual(msgOf(r.err), autoGenPk ? 'invalid id: not a string at [data[1].id]' : 'can not insert by data[0] without valid id when autoGenPk is false')
                    assert.deepStrictEqual(await ids(wo), ['seed'])
                }, { autoGenPk })
            })

        }

        //S3: option.returnList為true時整批性錯誤仍為reject, 不回逐筆陣列
        it('insert with returnList true still rejects the whole batch for a non-string id', async function() {
            await withWo(async(wo) => {
                let r = await settle(wo.insert({ id: 5, name: 'n' }, { returnList: true }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), reMsg)
                assert.deepStrictEqual(await ids(wo), ['seed'])
            })
        })

        //對照: 有效字串主鍵照常寫入(不因本規則過度拒絕)
        for (let autoGenPk of [true, false]) {
            for (let api of ['insert', 'insertBulk', 'save']) {
                it(`${api}: a valid string id is written as given (autoGenPk: ${autoGenPk})`, async function() {
                    await withWo(async(wo, evs) => {
                        await wo[api]({ id: 'k1', name: 'n' })
                        assert.deepStrictEqual(await ids(wo), ['k1', 'seed'])
                        assert.strictEqual(nErr(evs), 0)
                    }, { autoGenPk })
                })
            }
        }

    })

})
