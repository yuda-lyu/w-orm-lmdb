import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import WOrm from '../src/WOrmLmdb.mjs'
import { genRevoked, genTagThrow, genOwnKeysThrow, genGetterElem0, secrets } from './tools/hostile.mjs'


//讀取即拋錯之輸入依原失敗通道拒絕(wsemi chkReadable), 規則見sgud/rule/rulebook.md R-READABLE-01(排程第557項規則二)
//規格(主控裁示; wsemi 1.9.69起判斷函數對讀取即拋錯之值一律回false而不拋錯, 原站點因此把「不可讀」當成「型別不符/輸入無效/未給」而靜默忽略或改用預設):
//  R1 呼叫端給的資料容器(data)、選項(建構opt與各方法之option)、主鍵參數(selectByPk之pk): 判斷函數(iseobj、isearr、isestr、isbol、isobj)回false之分支內,
//     以chkReadable分辨「不可讀」與「可讀而型別不符」; 探針置於判斷函數回false之後(有效輸入不多讀)
//  R2 不可讀者走原失敗通道: 寫入入口與selectByPk為error事件恰一次後reject、資料庫不變, 建構為同步throw; 訊息為固定詞彙invalid <名稱>: must be a readable value(不含原例外文字)
//  R3 可讀而型別不符者行為不變(輸入無效之通道): insert與insertBulk回{n:0,nInserted:0,ok:1}(returnList為true時insert回[])、save與del回[]、selectByPk回null、建構忽略並用預設
//  R4 chkReadable不呼叫存取器: 陣列[0]為存取器而拋錯者由呼叫端自讀; 其餘存取器拋錯(無從偵測)維持原錯誤, 惟入口階段之拋錯亦走error事件恰一次後reject
//每案使用全新資料表(含一筆種子), 以select核對資料庫


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let root = path.join(__dirname, '_tmp', 'readable')
let dirSrc = path.join(__dirname, '..', 'src')
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
            evs.push({ type: 'error', mode, data, err })
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

let noSecret = (...vals) => {
    let s = JSON.stringify(vals.map((v) => (v instanceof Error ? v.message : v)))
    for (let x of secrets) {
        assert.ok(s.indexOf(x) < 0, `must not echo ${x}`)
    }
}

//assertRejectedAsUnreadable, 不可讀輸入之失敗通道(R2): reject固定詞彙, error事件恰一次且先於reject(data為null), 不發change, 資料庫不變
let assertRejectedAsUnreadable = async (wo, evs, r, mode, msg) => {
    assert.strictEqual(r.ok, false, 'must reject')
    assert.ok(r.err instanceof Error)
    assert.strictEqual(msgOf(r.err), msg)
    assert.deepStrictEqual(evs, [{ type: 'error', mode, data: null, err: msg }])
    assert.deepStrictEqual(await ids(wo), ['seed'])
    noSecret(r.err, evs)
}


//不可讀值(R2之對象): 工廠使每案皆為全新值
let unreadables = [
    { name: 'revoked Proxy', make: genRevoked },
    { name: 'Symbol.toStringTag throws', make: genTagThrow },
]

//可讀而型別不符之值(R3): 含僅ownKeys拋錯者(標籤可讀, 該站點之判斷函數不讀其內容)
let readableWrongs = [
    { name: 'number 5', make: () => 5 },
    { name: 'String wrapper', make: () => new String('a') }, // eslint-disable-line no-new-wrappers
    { name: 'ownKeys throws', make: genOwnKeysThrow },
]


describe('readable', function() {

    before(function() {
        fs.rmSync(root, { recursive: true, force: true })
        fs.mkdirSync(root, { recursive: true })
    })

    after(function() {
        fs.rmSync(root, { recursive: true, force: true })
    })


    describe('data container (insert, insertBulk, save, del)', function() {

        //data之不可讀值: 已撤銷之Proxy、Symbol.toStringTag拋錯者、ownKeys陷阱拋錯者(iseobj讀自有鍵)、長度為1而[0]為存取器且拋錯者
        let dataUnreadables = [
            { name: 'revoked Proxy', make: genRevoked },
            { name: 'Symbol.toStringTag throws', make: genTagThrow },
            { name: 'ownKeys throws', make: genOwnKeysThrow },
            { name: 'array whose [0] accessor throws', make: genGetterElem0 },
        ]

        for (let api of ['insert', 'insertBulk', 'save', 'del']) {
            for (let c of dataUnreadables) {
                it(`${api}: data is ${c.name} -> rejected as unreadable (R2), nothing written`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api](c.make()))
                        await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                    })
                })
            }
        }

        //returnList為true時不可讀之data仍reject, 不回[]
        it('insert: data is unreadable with returnList true -> still rejected', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.insert(genRevoked(), { returnList: true }))
                await assertRejectedAsUnreadable(wo, evs, r, 'insert', 'invalid data: must be a readable value')
            })
        })

        //R3: 可讀而型別不符(含空物件、空陣列、僅含空值之陣列): 回值與事件不變
        let readableInvalids = [
            { name: 'undefined', make: () => undefined },
            { name: 'null', make: () => null },
            { name: 'empty string', make: () => '' },
            { name: 'string', make: () => 'str' },
            { name: 'number 5', make: () => 5 },
            { name: 'true', make: () => true },
            { name: 'String wrapper', make: () => new String('a') }, // eslint-disable-line no-new-wrappers
            { name: 'BigInt', make: () => 10n },
            { name: 'function', make: () => () => 1 },
            { name: 'empty object', make: () => ({}) },
            { name: 'empty array', make: () => [] },
            { name: 'array of null', make: () => [null] },
            { name: 'array of undefined', make: () => [undefined] },
            { name: 'array of empty string', make: () => [''] },
            {
                name: 'array whose [0] accessor returns null',
                make: () => {
                    let a = [0]; Object.defineProperty(a, '0', {
                        get() {
                            return null
                        },
                        enumerable: true
                    }); return a
                }
            },
            { name: 'Date', make: () => new Date(0) },
            { name: 'Map', make: () => new Map() },
            { name: 'Set', make: () => new Set() },
        ]
        let ansInvalid = {
            insert: { n: 0, nInserted: 0, ok: 1 },
            insertBulk: { n: 0, nInserted: 0, ok: 1 },
            save: [],
            del: [],
        }
        for (let api of ['insert', 'insertBulk', 'save', 'del']) {
            for (let c of readableInvalids) {
                it(`${api}: data is ${c.name} -> treated as invalid input (R3 unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo[api](c.make())
                        assert.deepStrictEqual(r, ansInvalid[api])
                        assert.deepStrictEqual(evs, [])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                    })
                })
            }
        }
        //型別陣列與Buffer: 標籤可讀且非[object Object], iseobj不列舉其鍵, 探針亦不列舉全部索引(2百萬個索引); 依輸入無效之通道回覆
        for (let api of ['insert', 'insertBulk', 'save', 'del']) {
            it(`${api}: data is a large typed array and a Buffer -> treated as invalid input, no index enumeration (R3 unchanged)`, async function() {
                await withWo(async(wo, evs) => {
                    for (let big of [new Uint8Array(2000000), Buffer.alloc(2000000)]) {
                        assert.deepStrictEqual(await wo[api](big), ansInvalid[api])
                    }
                    assert.deepStrictEqual(evs, [])
                    assert.deepStrictEqual(await ids(wo), ['seed'])
                })
            })
        }
        it('insert: readable invalid data with returnList true -> [] (R3 unchanged)', async function() {
            await withWo(async(wo, evs) => {
                assert.deepStrictEqual(await wo.insert(5, { returnList: true }), [])
                assert.deepStrictEqual(evs, [])
            })
        })

    })


    describe('data elements', function() {

        //元素為不可讀值: 於資料複製階段(cloneDeep)即拋錯, 屬既有失敗通道(整批reject, error事件恰一次, 不寫入; 訊息為該物件所拋之值, 本規則不改)
        for (let api of ['insert', 'insertBulk', 'save', 'del']) {
            for (let c of [{ name: 'revoked Proxy', make: genRevoked }, { name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                it(`${api}: [${c.name}] is rejected at the copy stage, nothing written (existing channel)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api]([{ id: 'ok1', name: 'a' }, c.make()]))
                        assert.strictEqual(r.ok, false)
                        assert.deepStrictEqual(evs.map((v) => [v.type, v.mode]), [['error', api]])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                    })
                })
            }
        }

        //元素讀取後才變為不可讀(複製後之型別標籤讀取拋錯): procPk之isobj回false, 以chkReadable分辨, 不誤報為「非物件」
        for (let api of ['insert', 'insertBulk', 'save']) {
            it(`${api}: an element whose type tag becomes unreadable after the copy is reported as unreadable, not as non-object`, async function() {
                await withWo(async(wo, evs) => {
                    let armed = false
                    class E {
                        constructor() {
                            this.id = 'e1'
                            Object.defineProperty(this, 'b', {
                                get() {
                                    armed = true
                                    return 2
                                },
                                enumerable: true,
                                configurable: true,
                            })
                        }

                        get [Symbol.toStringTag]() {
                            if (armed) {
                                throw new Error('TAG-SECRET')
                            }
                            return 'Object'
                        }
                    }
                    let r = await settle(wo[api]([new E()]))
                    assert.strictEqual(r.ok, false)
                    assert.strictEqual(msgOf(r.err), 'invalid data[0]: must be a readable value')
                    assert.deepStrictEqual(evs.map((v) => [v.type, v.mode, v.err]), [['error', api, 'invalid data[0]: must be a readable value']])
                    assert.deepStrictEqual(await ids(wo), ['seed'])
                    noSecret(r.err, evs)
                })
            })
        }

        //可讀而非物件者(含String包裝物件): 既有訊息不變
        for (let api of ['insert', 'insertBulk', 'save']) {
            for (let c of [{ name: 'number 5', make: () => 5 }, { name: 'string', make: () => 'str' }, { name: 'String wrapper', make: () => new String('a') }, { name: 'array', make: () => [1] }]) { // eslint-disable-line no-new-wrappers
                it(`${api}: element ${c.name} -> can not ${api} by data[1] which is not an object (R3 unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api]([{ id: 'ok1', name: 'a' }, c.make()]))
                        assert.strictEqual(r.ok, false)
                        assert.strictEqual(msgOf(r.err), `can not ${api} by data[1] which is not an object`)
                        assert.deepStrictEqual(evs.map((v) => [v.type, v.mode]), [['error', api]])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                    })
                })
            }
        }

    })


    describe('option container and members (insert, save, backup)', function() {

        //option本身不可讀(已撤銷之Proxy): 讀取其成員即拋錯; 依原失敗通道拒絕, 不寫入
        for (let api of ['insert', 'save']) {
            it(`${api}: option is a revoked Proxy -> rejected as unreadable (R2), nothing written`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await settle(wo[api]({ id: 'a1', name: 'n' }, genRevoked()))
                    await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid option: must be a readable value')
                })
            })
        }

        //option本身之標籤拋錯或ownKeys拋錯: 成員讀取不受影響, 視為可讀(依既有處理: 取不到成員值而用預設)
        for (let api of ['insert', 'save']) {
            for (let c of [{ name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                it(`${api}: option is ${c.name} but its members are readable -> used as an empty option (unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await wo[api]({ id: 'a1', name: 'n' }, c.make())
                        assert.strictEqual(r.ok === 1 || r.n === 1 || (Array.isArray(r) && r[0].ok === 1), true)
                        assert.deepStrictEqual(await ids(wo), ['a1', 'seed'])
                        assert.strictEqual(evs.filter((v) => v.type === 'error').length, 0)
                    })
                })
            }
        }

        //option.returnList之值: isbol回false後以chkReadable分辨
        for (let c of unreadables) {
            it(`insert: option.returnList is ${c.name} -> rejected as unreadable (R2), nothing written`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await settle(wo.insert({ id: 'a1', name: 'n' }, { returnList: c.make() }))
                    await assertRejectedAsUnreadable(wo, evs, r, 'insert', 'invalid option.returnList: must be a readable value')
                })
            })
        }
        for (let c of readableWrongs) {
            it(`insert: option.returnList is ${c.name} -> default false (R3 unchanged)`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await wo.insert({ id: 'a1', name: 'n' }, { returnList: c.make() })
                    assert.deepStrictEqual(r, { n: 1, nInserted: 1, ok: 1 })
                    assert.deepStrictEqual(await ids(wo), ['a1', 'seed'])
                    assert.strictEqual(evs.filter((v) => v.type === 'error').length, 0)
                })
            })
        }
        it('insert: option.returnList true/false are honoured (valid, no probe)', async function() {
            await withWo(async(wo) => {
                assert.deepStrictEqual(await wo.insert({ id: 'a1', name: 'n' }, { returnList: true }), [{ n: 1, nInserted: 1, ok: 1 }])
                assert.deepStrictEqual(await wo.insert({ id: 'a2', name: 'n' }, { returnList: false }), { n: 1, nInserted: 1, ok: 1 })
            })
        })

        //存取器拋錯(chkReadable不呼叫存取器, 無從偵測): 維持原錯誤文字, 惟入口階段之拋錯亦走error事件恰一次後reject, 不寫入(R4)
        it('insert: option.returnList accessor throws -> original error, error event once, nothing written (R4)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.insert({ id: 'a1', name: 'n' }, {
                    get returnList() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'OPT-SECRET')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'insert', data: null, err: 'OPT-SECRET' }])
                assert.deepStrictEqual(await ids(wo), ['seed'])
            })
        })
        it('save: option.autoInsert accessor throws -> original error, error event once, nothing written (R4)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.save({ id: 'a1', name: 'n' }, {
                    get autoInsert() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'OPT-SECRET')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'save', data: null, err: 'OPT-SECRET' }])
                assert.deepStrictEqual(await ids(wo), ['seed'])
            })
        })
        it('save: option.merge accessor throws -> original error, error event once with the data, nothing written (existing T10.3 channel)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.save({ id: 'seed', name: 'n' }, {
                    get merge() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'OPT-SECRET')
                assert.strictEqual(evs.filter((v) => v.type === 'error').length, 1)
                assert.deepStrictEqual(await wo.select(), [{ id: 'seed', v: 1 }])
            })
        })

        //save之option成員(autoInsert、merge)不經判斷函數而以真假值使用, 不讀取其內容: 不可讀之值視同一般值(autoInsert為真、merge非false), 不拒絕
        for (let c of unreadables) {
            it(`save: option.autoInsert and option.merge are ${c.name} -> used as plain values, never read (unchanged)`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await wo.save({ id: 'a1', name: 'n' }, { autoInsert: c.make(), merge: c.make() })
                    assert.deepStrictEqual(r, [{ n: 1, nInserted: 1, nModified: 0, ok: 1 }])
                    assert.deepStrictEqual(await ids(wo), ['a1', 'seed'])
                    assert.strictEqual(evs.filter((v) => v.type === 'error').length, 0)
                })
            })
        }

        //backup: option與option.compact
        let nBk = 0
        let bkDir = () => {
            nBk++
            return path.join(root, `bk${nBk}`)
        }
        it('backup: option is a revoked Proxy -> rejected as unreadable (R2), no target created', async function() {
            await withWo(async(wo, evs) => {
                let dir = bkDir()
                let r = await settle(wo.backup(dir, genRevoked()))
                await assertRejectedAsUnreadable(wo, evs, r, 'backup', 'invalid option: must be a readable value')
                assert.strictEqual(fs.existsSync(dir), false)
            })
        })
        for (let c of unreadables) {
            it(`backup: option.compact is ${c.name} -> rejected as unreadable (R2), no target created`, async function() {
                await withWo(async(wo, evs) => {
                    let dir = bkDir()
                    let r = await settle(wo.backup(dir, { compact: c.make() }))
                    await assertRejectedAsUnreadable(wo, evs, r, 'backup', 'invalid option.compact: must be a readable value')
                    assert.strictEqual(fs.existsSync(dir), false)
                })
            })
        }
        for (let c of readableWrongs) {
            it(`backup: option.compact is ${c.name} -> default true (R3 unchanged)`, async function() {
                await withWo(async(wo) => {
                    let dir = bkDir()
                    let r = await wo.backup(dir, { compact: c.make() })
                    assert.strictEqual(r.ok, 1)
                    assert.strictEqual(r.compact, true)
                    assert.strictEqual(fs.existsSync(path.join(dir, 'data.mdb')), true)
                })
            })
        }
        it('backup: option.compact false is honoured (valid, no probe)', async function() {
            await withWo(async(wo) => {
                let r = await wo.backup(bkDir(), { compact: false })
                assert.strictEqual(r.compact, false)
            })
        })
        it('backup: option.compact accessor throws -> original text under the backup prefix, error event once (R4, unchanged)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.backup(bkDir(), {
                    get compact() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'can not backup, OPT-SECRET')
                assert.strictEqual(evs.filter((v) => v.type === 'error').length, 1)
            })
        })

    })


    describe('pk of selectByPk', function() {

        for (let c of unreadables) {
            it(`selectByPk: pk is ${c.name} -> rejected as unreadable (R2), not treated as a missing key`, async function() {
                await withWo(async(wo, evs) => {
                    let r = await settle(wo.selectByPk(c.make()))
                    await assertRejectedAsUnreadable(wo, evs, r, 'selectByPk', 'invalid pk: must be a readable value')
                })
            })
        }

        //R3: 可讀而非字串(含ownKeys拋錯者, 該站點之判斷函數只看型別)回null, 不發事件
        for (let c of [...readableWrongs, { name: 'undefined', make: () => undefined }, { name: 'null', make: () => null }, { name: 'empty string', make: () => '' }]) {
            it(`selectByPk: pk is ${c.name} -> null, no event (R3 unchanged)`, async function() {
                await withWo(async(wo, evs) => {
                    assert.strictEqual(await wo.selectByPk(c.make()), null)
                    assert.deepStrictEqual(evs, [])
                })
            })
        }

        it('selectByPk: a valid pk is found (no probe)', async function() {
            await withWo(async(wo) => {
                assert.deepStrictEqual(await wo.selectByPk('seed'), { id: 'seed', v: 1 })
            })
        })

    })


    describe('find of select and delAll (checked by wsemi cvOrmFind, unchanged)', function() {

        for (let api of ['select', 'delAll']) {
            for (let c of [{ name: 'revoked Proxy', make: genRevoked }, { name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                it(`${api}: find is ${c.name} -> rejected as invalid find, nothing deleted`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api](c.make()))
                        assert.strictEqual(r.ok, false)
                        assert.ok(/^invalid find: /.test(msgOf(r.err)), msgOf(r.err))
                        assert.deepStrictEqual(evs.map((v) => [v.type, v.mode]), [['error', api]])
                        assert.deepStrictEqual(await ids(wo), ['seed'])
                        noSecret(r.err, evs)
                    })
                })
            }
        }

    })


    describe('constructor opt', function() {

        let nCtor = 0
        let mkUrl = () => {
            nCtor++
            return path.join(root, `ctor${nCtor}`)
        }

        //opt本身不可讀: 同步throw固定詞彙, 先於open, 不建立資料夾
        it('opt is a revoked Proxy -> throws synchronously, no folder created', function() {
            assert.throws(() => WOrm(genRevoked()), (err) => err instanceof Error && err.message === 'invalid opt: must be a readable value')
        })

        //opt成員不可讀: 同步throw固定詞彙, 先於open, 不建立資料夾
        for (let key of ['url', 'db', 'cl', 'useCache', 'autoGenPk']) {
            for (let c of unreadables) {
                it(`opt.${key} is ${c.name} -> throws synchronously (R2), no folder created`, function() {
                    let url = mkUrl()
                    let opt = { url, db: 'worm', cl: 'c' }
                    opt[key] = c.make()
                    assert.throws(() => WOrm(opt), (err) => err instanceof Error && err.message === `invalid opt.${key}: must be a readable value` && err.message.indexOf('SECRET') < 0)
                    if (key !== 'url') {
                        assert.strictEqual(fs.existsSync(url), false)
                    }
                })
            }
        }

        //R3: 可讀而型別不符者忽略並用預設(本套件慣例)
        for (let key of ['db', 'cl', 'useCache', 'autoGenPk']) {
            for (let c of readableWrongs) {
                it(`opt.${key} is ${c.name} -> ignored and default used (R3 unchanged)`, async function() {
                    let url = mkUrl()
                    let opt = { url, db: 'worm', cl: 'c' }
                    opt[key] = c.make()
                    let wo = WOrm(opt)
                    try {
                        await wo.insert({ name: 'n' }) //autoGenPk預設true: 未帶id者補值
                        assert.strictEqual((await wo.select()).length, 1)
                        if (key === 'db') {
                            assert.strictEqual(fs.existsSync(path.join(url, 'worm', 'c')), true)
                        }
                        if (key === 'cl') {
                            assert.strictEqual(fs.existsSync(path.join(url, 'worm', 'test')), true)
                        }
                    }
                    finally {
                        await wo.close()
                    }
                })
            }
        }

        //存取器拋錯(無從偵測)維持原錯誤, 同步throw
        it('opt.url accessor throws -> original error thrown synchronously (R4, unchanged)', function() {
            assert.throws(() => WOrm({
                get url() {
                    throw new Error('OPT-SECRET')
                }
            }), (err) => err.message === 'OPT-SECRET')
        })

        it('opt readable members used as given (valid, no probe)', async function() {
            let url = mkUrl()
            let wo = WOrm({ url, db: 'd1', cl: 'c1', useCache: true, autoGenPk: false })
            try {
                await wo.insert({ id: 'x', name: 'n' })
                assert.deepStrictEqual((await wo.select()).map((v) => v.id), ['x'])
                assert.ok(await settle(wo.insert({ name: 'n' })).then((r) => !r.ok)) //autoGenPk為false
            }
            finally {
                await wo.close()
            }
        })

    })


    describe('site inventory (source scan)', function() {

        //codeOf, 去除註解後的程式碼行
        let codeOf = (fp) => {
            let r = []
            for (let line of fs.readFileSync(fp, 'utf8').split(/\r?\n/)) {
                let t = line.trim()
                if (t.startsWith('//') || t.startsWith('*') || t.startsWith('/*')) {
                    continue
                }
                r.push(line.replace(/\s\/\/.*$/, ''))
            }
            return r
        }
        let lines = codeOf(path.join(dirSrc, 'WOrmLmdb.mjs'))
        let text = lines.join('\n')
        let count = (re) => (text.match(re) || []).length

        //R1: 以wsemi判斷函數判定之全集(依函數名計數); 新增或移除站點時此表須同步更新並依下方分類: 呼叫端給的值須有chkReadable探針, 內部值(資料庫列、已複製之複本、本套件自身之值)不須
        it('judge-function call sites are the classified set (R1: any new site must be classified)', function() {
            let names = ['isestr', 'isbol', 'iseobj', 'isearr', 'isobj', 'isarr']
            let got = {}
            for (let n of names) {
                got[n] = count(new RegExp(`\\b${n}\\(`, 'g'))
            }
            //呼叫端給的值(12個站點、16次呼叫): isestr 4(opt.url、db、cl、selectByPk之pk)、isbol 3(opt.useCache、autoGenPk、getOptBol)、iseobj與isearr各4(data四入口)、isobj 1(procPk之元素)
            //內部值(14次呼叫): isestr 3(getErrMsg之訊息、pkConflict 2處)、iseobj 4(資料庫列)、isobj 1(mkMatch之資料庫列)、isarr 6(快取、結果、複製後之data 4處)
            assert.deepStrictEqual(got, { isestr: 7, isbol: 3, iseobj: 8, isearr: 4, isobj: 2, isarr: 6 })
        })

        it('chkReadable is imported from wsemi and called only at the classified sites (R1: after the judge function returned false)', function() {
            assert.ok(/import chkReadable from 'wsemi\/src\/chkReadable\.mjs'/.test(fs.readFileSync(path.join(dirSrc, 'WOrmLmdb.mjs'), 'utf8')))
            //呼叫點: getOptVal(catch)、chkOptReadable、getOpt(catch)、getOptBol、isUnreadableData(標籤與陣列、自有鍵各1處; 陣列[0]之自讀不算)、procPk、selectByPk
            assert.strictEqual(count(/\bchkReadable\(/g), 8)
            //自有鍵之讀取只於標籤為[object Object]時進行(iseobj之for...in之範圍), 不為型別陣列、Buffer等列舉全部索引
            assert.ok(/Object\.prototype\.toString\.call\(data\) === '\[object Object\]' && chkReadable\(data, \{ ownKeys: true \}\) !== ''/.test(text))
            assert.strictEqual(count(/ownKeys: true/g), 1)
            //建構: 五個成員皆於判斷函數回false之分支內呼叫chkOptReadable
            let ctor = [...text.matchAll(/if \(!(isestr|isbol)\((\w+)\)\) \{\s*chkOptReadable\('(\w+)', (\w+)\)/g)].map((m) => `${m[1]}:${m[2]}:${m[3]}:${m[4]}`)
            assert.deepStrictEqual(ctor, ['isestr:url:url:url', 'isestr:db:db:db', 'isestr:cl:cl:cl', 'isbol:useCache:useCache:useCache', 'isbol:autoGenPk:autoGenPk:autoGenPk'])
            //data: 四個入口皆於iseobj與isearr皆回false之分支內呼叫isUnreadableData, 且各以固定之mode拒絕
            let modes = [...text.matchAll(/if \(!iseobj\(data\) && !isearr\(data\)\) \{\s*if \(isUnreadableData\(data\)\) \{\s*return procUnreadableData\('(\w+)'\)/g)].map((m) => m[1])
            assert.deepStrictEqual(modes, ['insert', 'insertBulk', 'save', 'del'])
            //option: getOptBol於isbol回false之後呼叫chkReadable; procPk之元素、selectByPk之pk亦於判斷函數回false之分支內
            assert.ok(/if \(isbol\(v\)\) \{\s*return v\s*\}\s*if \(chkReadable\(v\) !== ''\)/.test(text))
            assert.ok(/if \(!isobj\(v\)\) \{\s*if \(chkReadable\(v\) !== ''\)/.test(text))
            assert.ok(/if \(!isestr\(pk\)\) \{\s*if \(chkReadable\(pk\) !== ''\)/.test(text))
        })

        it('option reads go through getOpt/getOptBol only (no raw get(option, ...) outside the helpers)', function() {
            assert.strictEqual(count(/\bget\(option,/g), 1) //getOpt內
            assert.strictEqual(count(/\bget\(opt,/g), 1) //getOptVal內
            assert.strictEqual(count(/\bgetOpt\(option, '/g) + count(/\bgetOptBol\(option, '/g), 4) //save: autoInsert與merge, insert: returnList, backup: compact
        })

    })

})
