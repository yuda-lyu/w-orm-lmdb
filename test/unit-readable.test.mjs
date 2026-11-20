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
    //比對布林而非事件物件: 修前或變異版之事件data可為不可讀之值, 以其入deepStrictEqual之差異輸出會使執行器崩潰而無法判紅
    assert.deepStrictEqual(evs.map((v) => [v.type, v.mode, v.data === null, v.err]), [['error', mode, true, msg]])
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

        //元素為不可讀值: 於資料複製階段(cloneDeep)即拋錯, 全族口徑A3(2026-11-20改釘; 原維持複製處拋出之原訊息與error事件之data為輸入):
        //轉固定詞彙invalid data: must be a readable value, 走與頂層不可讀相同之通道(error事件恰一次且data為null後reject, 不寫入)
        for (let api of ['insert', 'insertBulk', 'save', 'del']) {
            for (let c of [{ name: 'revoked Proxy', make: genRevoked }, { name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                it(`${api}: [${c.name}] is rejected at the copy stage as unreadable, nothing written (A3)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api]([{ id: 'ok1', name: 'a' }, c.make()]))
                        await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
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
                    //全族口徑A3與A4(2026-11-20改釘; 原為invalid data[0]且error事件之data為輸入): 同頂層不可讀之固定詞彙與通道
                    await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
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

        //存取器拋錯(chkReadable不呼叫存取器, 於讀取處捕捉): 全族口徑A2(2026-11-20改釘; 原維持原錯誤文字), 轉固定詞彙invalid option.<鍵>, 走入口階段通道(error事件恰一次且data為null後reject, 不寫入), 不轉出存取器拋出之文字(R4)
        it('insert: option.returnList accessor throws -> fixed vocabulary, error event once, nothing written (R4, A2)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.insert({ id: 'a1', name: 'n' }, {
                    get returnList() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'invalid option.returnList: must be a readable value')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'insert', data: null, err: 'invalid option.returnList: must be a readable value' }])
                assert.deepStrictEqual(await ids(wo), ['seed'])
                noSecret(r.err, evs)
            })
        })
        it('save: option.autoInsert accessor throws -> fixed vocabulary, error event once, nothing written (R4, A2)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.save({ id: 'a1', name: 'n' }, {
                    get autoInsert() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'invalid option.autoInsert: must be a readable value')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'save', data: null, err: 'invalid option.autoInsert: must be a readable value' }])
                assert.deepStrictEqual(await ids(wo), ['seed'])
                noSecret(r.err, evs)
            })
        })
        it('save: option.merge accessor throws -> fixed vocabulary, error event once with data null, nothing written (T10.3 channel, A2 and A4)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.save({ id: 'seed', name: 'n' }, {
                    get merge() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'invalid option.merge: must be a readable value')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'save', data: null, err: 'invalid option.merge: must be a readable value' }])
                assert.deepStrictEqual(await wo.select(), [{ id: 'seed', v: 1 }])
                noSecret(r.err, evs)
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
        it('backup: option.compact accessor throws -> fixed vocabulary, error event once (R4, A2; was the original text under the backup prefix)', async function() {
            await withWo(async(wo, evs) => {
                let r = await settle(wo.backup(bkDir(), {
                    get compact() {
                        throw new Error('OPT-SECRET')
                    }
                }))
                assert.strictEqual(r.ok, false)
                assert.strictEqual(msgOf(r.err), 'invalid option.compact: must be a readable value')
                assert.deepStrictEqual(evs, [{ type: 'error', mode: 'backup', data: null, err: 'invalid option.compact: must be a readable value' }])
                noSecret(r.err, evs)
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

        //存取器拋錯(chkReadable不呼叫存取器, 於讀取處捕捉): 全族口徑A2(2026-11-20改釘; 原維持原錯誤), 同步throw固定詞彙invalid opt.<名>, 不轉出存取器拋出之文字
        it('opt.url accessor throws -> fixed vocabulary thrown synchronously (R4, A2; was the original error)', function() {
            assert.throws(() => WOrm({
                get url() {
                    throw new Error('OPT-SECRET')
                }
            }), (err) => err instanceof Error && err.message === 'invalid opt.url: must be a readable value')
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


    //第557項全族對齊輪(2026-11-20, 條款A1~A6, 見sgud/rule/rulebook.md R-READABLE-01規則6~9與R-PKGIVEN-01): 逐代號釘住
    //  A1 選項容器本身不可讀(已撤銷之Proxy、get陷阱拋錯): invalid option(建構為invalid opt), 走原通道
    //  A2 存取器拋錯(容器可讀而其成員為拋錯之getter): 於讀取處捕捉, 轉invalid <名稱>: must be a readable value, 不轉出拋出之文字
    //  A3 複製階段之巢狀不可讀值: 轉invalid data: must be a readable value, 走與頂層不可讀相同之通道(error事件恰一次且data為null後reject, 資料庫不變)
    //  A4 入口階段拒絕之error事件之data為null(含已關閉之實例收到不可讀之資料容器)
    //  A5 selectByPk之不可讀pk: reject(error事件恰一次); 可讀而非字串者回null(其餘見'pk of selectByPk')
    //  A6 主鍵給了而無效: 見unit-pkgiven
    describe('family alignment (round 557: A1-A6)', function() {

        //withGetter, 於base上定義拋錯之存取器(拋出含標記字串之錯誤)
        let withGetter = (base, key, mk = () => new Error('OPT-SECRET')) => {
            Object.defineProperty(base, key, {
                get() {
                    throw mk()
                },
                enumerable: true,
                configurable: true,
            })
            return base
        }

        //proxyKeyThrow, 僅特定鍵之get陷阱拋錯之Proxy(容器之標籤與其餘成員可讀)
        let proxyKeyThrow = (base, key) => new Proxy(base, {
            get(t, k) {
                if (k === key) {
                    throw new Error('OPT-SECRET')
                }
                return t[k]
            }
        })

        //proxyGetThrowAll, 全部get皆拋錯之Proxy(容器本身不可讀: 連Symbol.toStringTag之讀取皆拋錯)
        let proxyGetThrowAll = () => new Proxy({}, {
            get() {
                throw new Error('OPT-SECRET')
            }
        })

        let nAlign = 0
        let mkUrlAlign = () => {
            nAlign++
            return path.join(root, `align${nAlign}`)
        }


        describe('A1 option container itself unreadable', function() {

            let containers = [
                { name: 'revoked Proxy', make: genRevoked },
                { name: 'Proxy whose get trap always throws', make: proxyGetThrowAll },
            ]

            for (let c of containers) {
                it(`constructor: opt is ${c.name} -> throws synchronously invalid opt (A1)`, function() {
                    let opt = c.make()
                    assert.throws(() => WOrm(opt), (err) => err instanceof Error && err.message === 'invalid opt: must be a readable value' && err.message.indexOf('SECRET') < 0)
                })

                for (let api of ['insert', 'save']) {
                    it(`${api}: option is ${c.name} -> invalid option, error event once with data null, nothing written (A1)`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api]({ id: 'a1', name: 'n' }, c.make()))
                            await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid option: must be a readable value')
                        })
                    })
                }

                it(`backup: option is ${c.name} -> invalid option, error event once with data null (A1)`, async function() {
                    await withWo(async(wo, evs) => {
                        let fp = path.join(root, `alignbk${++nAlign}`)
                        let r = await settle(wo.backup(fp, c.make()))
                        await assertRejectedAsUnreadable(wo, evs, r, 'backup', 'invalid option: must be a readable value')
                        assert.strictEqual(fs.existsSync(fp), false)
                    })
                })
            }

        })


        describe('A2 accessor that throws (container readable, member throws)', function() {

            //建構: 五個成員 x {自有存取器, 僅該鍵之get陷阱拋錯之Proxy}
            for (let key of ['url', 'db', 'cl', 'useCache', 'autoGenPk']) {
                for (let [kind, mk] of [
                    ['own accessor', (base) => withGetter(base, key)],
                    ['Proxy key trap', (base) => proxyKeyThrow(base, key)],
                ]) {
                    it(`constructor: opt.${key} is a ${kind} that throws -> throws synchronously invalid opt.${key} (A2), original text not transferred`, function() {
                        let url = mkUrlAlign()
                        let base = { url, db: 'worm', cl: 'c' }
                        delete base[key]
                        let opt = mk(base)
                        assert.throws(() => WOrm(opt), (err) => err instanceof Error && err.message === `invalid opt.${key}: must be a readable value` && err.message.indexOf('SECRET') < 0)
                        if (key !== 'url') {
                            assert.strictEqual(fs.existsSync(url), false)
                        }
                    })
                }
            }

            //方法: option成員x{自有存取器, Proxy key trap}
            let members = [
                { api: 'insert', key: 'returnList', call: (wo, option) => wo.insert({ id: 'a1', name: 'n' }, option) },
                { api: 'save', key: 'autoInsert', call: (wo, option) => wo.save({ id: 'a1', name: 'n' }, option) },
                { api: 'save', key: 'merge', call: (wo, option) => wo.save({ id: 'seed', name: 'n' }, option) },
                { api: 'backup', key: 'compact', call: (wo, option) => wo.backup(path.join(root, `alignbk${++nAlign}`), option) },
            ]
            for (let m of members) {
                for (let [kind, mk] of [
                    ['own accessor', () => withGetter({}, m.key)],
                    ['Proxy key trap', () => proxyKeyThrow({}, m.key)],
                    ['own accessor throwing a non-Error value', () => withGetter({}, m.key, () => 'OPT-SECRET-STRING')],
                ]) {
                    it(`${m.api}: option.${m.key} is a ${kind} that throws -> invalid option.${m.key}, error event once with data null, nothing written (A2 and A4)`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(m.call(wo, mk()))
                            await assertRejectedAsUnreadable(wo, evs, r, m.api, `invalid option.${m.key}: must be a readable value`)
                            assert.deepStrictEqual(await wo.select(), [{ id: 'seed', v: 1 }])
                            assert.ok(JSON.stringify([msgOf(r.err), evs]).indexOf('OPT-SECRET') < 0)
                        })
                    })
                }
            }

            //長度為1而[0]為拋錯之存取器之陣列(本庫自讀之元素): invalid data
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                it(`${api}: data is a one-element array whose [0] accessor throws -> invalid data, error event once with data null (A2)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo[api](genGetterElem0()))
                        await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                    })
                })
            }

            //類別實例之原型上之id存取器拋錯(cloneDeep只複製自有鍵, 原型上之getter留在複本而於本庫讀取id時才拋): 屬本庫自讀之成員為不可讀, 整批拒絕
            //(含del: 先於任何刪除之整批性拒絕, 不降為逐筆之原錯誤文字)
            let mkProtoIdThrow = (mk = () => new Error('OPT-SECRET')) => {
                class C {
                    get id() {
                        throw mk()
                    }
                }
                return [Object.assign(new C(), { name: 'n' })]
            }
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                for (let autoGenPk of [true, false]) {
                    it(`${api}: the id accessor on the prototype throws -> invalid data, error event once with data null, whole batch rejected, nothing changed (A2, autoGenPk: ${autoGenPk})`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api](mkProtoIdThrow()))
                            await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                            assert.deepStrictEqual(await wo.select(), [{ id: 'seed', v: 1 }])
                        }, { autoGenPk })
                    })
                }
            }
            it('insert: the id accessor on the prototype returns a valid string -> accepted as before (unchanged)', async function() {
                await withWo(async(wo, evs) => {
                    class C {
                        get id() {
                            return 'p1'
                        }
                    }
                    let r = await wo.insert([Object.assign(new C(), { name: 'n' })])
                    assert.deepStrictEqual(r, { n: 1, nInserted: 1, ok: 1 })
                    assert.deepStrictEqual(evs.filter((v) => v.type === 'error'), [])
                })
            })

            //有狀態之標籤存取器(前幾次讀取可讀、其後才拋錯): 無論於哪一次讀取起拋錯, 結果皆為invalid data(error事件恰一次且data為null)或可讀而無效之既有回傳, 不得逸出存取器拋出之文字
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                it(`${api}: a tag accessor that starts throwing after n reads (n = 1..8) never lets its text escape (A2)`, async function() {
                    for (let n = 1; n <= 8; n++) {
                        await withWo(async(wo, evs) => {
                            let k = 0
                            let data = {
                                get [Symbol.toStringTag]() {
                                    if (++k > n) {
                                        throw new Error('TAG-SECRET')
                                    }
                                    return 'Foo'
                                }
                            }
                            let r = await settle(wo[api](data))
                            if (r.ok) {
                                assert.deepStrictEqual(evs.filter((v) => v.type === 'error'), [], `n=${n}`)
                            }
                            else {
                                await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                            }
                            noSecret(r.ok ? r.v : r.err, evs.map((v) => v.err))
                        })
                    }
                })
            }

            //可讀之值不受影響: 存取器回傳有效值者照用
            it('insert and save: option accessors that return valid values are used as given (unchanged)', async function() {
                await withWo(async(wo) => {
                    let r1 = await wo.insert({ id: 'v1', name: 'n' }, Object.defineProperty({}, 'returnList', { get: () => true, enumerable: true }))
                    assert.deepStrictEqual(r1, [{ n: 1, nInserted: 1, ok: 1 }])
                    let r2 = await wo.save({ id: 'seed', w: 2 }, Object.defineProperty({}, 'merge', { get: () => false, enumerable: true }))
                    assert.deepStrictEqual(r2, [{ n: 1, nInserted: 0, nModified: 1, ok: 1 }])
                    assert.deepStrictEqual(await wo.selectByPk('seed'), { id: 'seed', w: 2 })
                })
            })

        })


        describe('A3 nested unreadable value found at the copy stage', function() {

            let nested = [
                { name: '{ id: revoked Proxy }', make: () => ({ id: genRevoked() }) },
                { name: '{ id, u8a: revoked Proxy }', make: () => ({ id: 'n1', u8a: genRevoked() }) },
                { name: '{ id, x: Symbol.toStringTag throws }', make: () => ({ id: 'n1', x: genTagThrow() }) },
                { name: '{ id, x: ownKeys throws }', make: () => ({ id: 'n1', x: genOwnKeysThrow() }) },
                { name: '[row, revoked Proxy]', make: () => [{ id: 'n1' }, genRevoked()] },
                { name: '[row, Symbol.toStringTag throws]', make: () => [{ id: 'n1' }, genTagThrow()] },
                { name: '[row, ownKeys throws]', make: () => [{ id: 'n1' }, genOwnKeysThrow()] },
                { name: '{ get id() throws }', make: () => withGetter({}, 'id') },
                { name: '{ id, get x() throws }', make: () => withGetter({ id: 'n1' }, 'x') },
                { name: '[row, { id, get x() throws }]', make: () => [{ id: 'n1' }, withGetter({ id: 'n2' }, 'x')] },
                { name: '[row, { id: revoked Proxy }]', make: () => [{ id: 'n1' }, { id: genRevoked() }] },
                { name: '{ id, deep: { deeper: [revoked Proxy] } }', make: () => ({ id: 'n1', deep: { deeper: [genRevoked()] } }) },
            ]

            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                for (let c of nested) {
                    it(`${api}: ${c.name} -> invalid data, error event once with data null, whole batch rejected, nothing changed (A3)`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api](c.make()))
                            await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                            assert.deepStrictEqual(await wo.select(), [{ id: 'seed', v: 1 }])
                        })
                    })
                }
            }

            //複製處之catch不分辨原因(條款: 可讀而無法複製者與過深之巢狀亦同): 過深之巢狀(引擎之堆疊溢位)亦為固定詞彙, 與其他複製處拋錯走同一通道
            //(error事件恰一次且data為null, 不寫入, 不轉出引擎之RangeError文字)
            let deepOf = () => {
                let o = { id: 'deep' }
                let cur = o
                for (let i = 0; i < 20000; i++) {
                    cur.n = {}
                    cur = cur.n
                }
                return o
            }
            let deepArrOf = () => {
                let o = { id: 'deepArr', n: [] }
                let cur = o.n
                for (let i = 0; i < 20000; i++) {
                    let next = []
                    cur.push(next)
                    cur = next
                }
                return o
            }
            let deeps = [
                { name: '{ id, n: { n: ... } } (20000 levels)', make: deepOf },
                { name: '{ id, n: [[[...]]] } (20000 levels of arrays)', make: deepArrOf },
                { name: '[row, over-deep row]', make: () => [{ id: 'n1' }, deepOf()] },
            ]
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                for (let c of deeps) {
                    it(`${api}: over-deep nesting ${c.name} -> invalid data, error event once with data null, not the engine RangeError, nothing changed (A3)`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api](c.make()))
                            await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                            //固定詞彙而非引擎之RangeError(型別為本套件之Error, 不含堆疊溢位之字樣)
                            assert.ok(!(r.err instanceof RangeError))
                            assert.ok(!/call stack/i.test(msgOf(r.err)))
                        })
                    })
                }
            }

            //過深之巢狀之拒絕為入口階段之拒絕(同其他複製處拋錯): 不寫入、不影響其後之使用(快取與資料庫一致), 同一實例其後之有效寫入照常
            it('an instance stays usable after an over-deep rejection (select and insert behave as before)', async function() {
                await withWo(async(wo, evs) => {
                    assert.deepStrictEqual(await ids(wo), ['seed'])
                    let r = await settle(wo.insert(deepOf()))
                    assert.strictEqual(r.ok, false)
                    assert.deepStrictEqual(await ids(wo), ['seed'])
                    let r2 = await wo.insert({ id: 'after', v: 2 })
                    assert.deepStrictEqual(r2, { n: 1, nInserted: 1, ok: 1 })
                    assert.deepStrictEqual(await ids(wo), ['after', 'seed'])
                    assert.deepStrictEqual(evs.filter((v) => v.type === 'error').length, 1)
                })
            })

            //使用端自拋之RangeError(含自拋與引擎同文之堆疊溢位字樣者)與其他拋錯同: 不分辨原因, 不轉出其文字
            let ownRangeErrs = [
                { name: 'RangeError of its own', make: () => new RangeError('OPT-SECRET') },
                { name: 'RangeError with the engine\'s stack overflow text', make: () => new RangeError('Maximum call stack size exceeded') },
            ]
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                for (let c of ownRangeErrs) {
                    it(`${api}: an accessor throwing a ${c.name} is unreadable like any other throw, its text is not transferred (A3)`, async function() {
                        await withWo(async(wo, evs) => {
                            let r = await settle(wo[api](withGetter({ id: 'n1' }, 'x', c.make)))
                            await assertRejectedAsUnreadable(wo, evs, r, api, 'invalid data: must be a readable value')
                        })
                    })
                }
            }

            //有效資料不受影響(含巢狀物件、陣列、Date、型別陣列)
            it('valid nested data is copied and written as before (unchanged)', async function() {
                await withWo(async(wo, evs) => {
                    let r = await wo.insert([{ id: 'ok1', deep: { a: [1, 2, { b: 3 }] }, d: new Date(0) }, { id: 'ok2', u8a: new Uint8Array([1, 2, 3]) }])
                    assert.deepStrictEqual(r, { n: 2, nInserted: 2, ok: 1 })
                    assert.deepStrictEqual(evs.filter((v) => v.type === 'error'), [])
                    assert.deepStrictEqual(await ids(wo), ['ok1', 'ok2', 'seed'])
                })
            })

        })


        describe('A4 error event data of entry-stage rejections is null', function() {

            //已關閉之實例: 資料容器本身不可讀者傳null(不把不可讀之值交給訂閱函數); 可讀者仍為輸入數據(不變)
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                for (let c of [{ name: 'revoked Proxy', make: genRevoked }, { name: 'Symbol.toStringTag throws', make: genTagThrow }, { name: 'ownKeys throws', make: genOwnKeysThrow }]) {
                    it(`${api}: closed instance with unreadable data (${c.name}) -> reject closed, error event data is null (A4)`, async function() {
                        await withWo(async(wo, evs) => {
                            await wo.close()
                            let r = await settle(wo[api](c.make()))
                            assert.strictEqual(r.ok, false)
                            assert.strictEqual(msgOf(r.err), `can not ${api} by closed client, instance can not be used after close()`)
                            //比對布林而非事件物件: 修前之事件data為不可讀之值, 以其入deepStrictEqual之差異輸出會使執行器崩潰
                            assert.deepStrictEqual(evs.map((v) => [v.type, v.mode, v.data === null, v.err]), [['error', api, true, msgOf(r.err)]])
                        })
                    })
                }

                it(`${api}: closed instance with readable data -> error event data is the input (unchanged)`, async function() {
                    await withWo(async(wo, evs) => {
                        await wo.close()
                        let input = { id: 'z1', name: 'n' }
                        let r = await settle(wo[api](input))
                        assert.strictEqual(r.ok, false)
                        assert.strictEqual(evs.length, 1)
                        assert.strictEqual(evs[0].data, input)
                    })
                })
            }

        })


        describe('A5 pk of selectByPk', function() {

            //不可讀: reject(error事件恰一次, data為null); 可讀而非字串: null, 不發事件
            for (let c of unreadables) {
                it(`selectByPk: pk is ${c.name} -> reject, error event once with data null (A5)`, async function() {
                    await withWo(async(wo, evs) => {
                        let r = await settle(wo.selectByPk(c.make()))
                        await assertRejectedAsUnreadable(wo, evs, r, 'selectByPk', 'invalid pk: must be a readable value')
                    })
                })
            }
            for (let [name, make] of [['Symbol', () => Symbol('s')], ['BigInt', () => 10n], ['plain object', () => ({})], ['array', () => ['seed']], ['function', () => () => 'seed'], ['Date', () => new Date(0)]]) {
                it(`selectByPk: pk is ${name} -> null, no event (A5, readable but not a string)`, async function() {
                    await withWo(async(wo, evs) => {
                        assert.strictEqual(await wo.selectByPk(make()), null)
                        assert.deepStrictEqual(evs, [])
                    })
                })
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

        //A3與A4: 呼叫端資料之深複製只經cloneIn(四入口各一處, 於主try之前), 已關閉之入口階段拒絕不把不可讀之資料容器交給訂閱函數
        it('data copy goes through cloneIn only (A3) and procClosed hides an unreadable data container (A4)', function() {
            assert.strictEqual(count(/\bcloneDeep\(data\)/g), 1) //cloneIn內
            let modes = [...text.matchAll(/let dc = cloneIn\('(\w+)', data\)/g)].map((m) => m[1])
            assert.deepStrictEqual(modes, ['insert', 'insertBulk', 'save', 'del'])
            assert.ok(/emitError\(mode, isUnreadableData\(data\) \? null : data, msg\)/.test(text))
            assert.strictEqual(count(/return rejectIn\('/g), 2, 'insert之returnList與save之autoInsert、merge讀取失敗')
            //主try內之入口階段拒絕(mkInErr: procPk之元素、getIdIn之原型上id存取器)與其四個catch(insert、insertBulk、save、del)之data清為null
            assert.strictEqual(count(/\bmkInErr\(/g), 2)
            assert.strictEqual(count(/\binErrs\.has\(err\)/g), 4)
            assert.strictEqual(count(/\bgetIdIn\(/g), 4, 'procPk 2、getWellFormedMsg 1、del之預讀 1')
        })

        it('option reads go through getOpt/getOptBol only (no raw get(option, ...) outside the helpers)', function() {
            assert.strictEqual(count(/\bget\(option,/g), 1) //getOpt內
            assert.strictEqual(count(/\bget\(opt,/g), 1) //getOptVal內
            assert.strictEqual(count(/\bgetOpt\(option, '/g) + count(/\bgetOptBol\(option, '/g), 4) //save: autoInsert與merge, insert: returnList, backup: compact
        })

    })

})
