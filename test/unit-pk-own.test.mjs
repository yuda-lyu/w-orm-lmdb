import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import WOrm from '../src/WOrmLmdb.mjs'


//主鍵(id)只認「自有可列舉之資料屬性」, 規則見sgud/rule/rulebook.md R-PKOWN-01(排程第569項, 承第557項R-PKGIVEN-01與olw569之lowdb修正)
//缺陷(修前, 1.0.77): 呼叫端之列其id為繼承(原型鏈上, 如Object.create({ id: 'a9' })或類別實例原型上之getter/資料屬性)時, cloneDeep保留原型,
//  procPk以get(v, 'id')讀得原型上之字串而判為「已帶」, lmdb之值編碼只存自有屬性, 故寫入無id欄之列(主鍵遺失);
//  或與資料庫內同值之既有列衝突(insert靜默跳過、insertBulk整批reject、save覆寫該既有列、del刪除該既有列)
//規格(主控裁示, rules-569):
//  O1 主鍵只認自有可列舉之資料屬性: id為繼承(原型鏈上)或自有不可列舉者視為「未帶」(與undefined、null、空字串同); 不論繼承值為字串、數字或物件
//  O2 autoGenPk為true: insert、insertBulk、save補新主鍵(自有可列舉之UUIDv7字串), 資料庫內該列帶此id欄; 繼承值不得被當成主鍵(不跳過、不衝突、不覆寫、不刪除同值之既有列)
//  O3 autoGenPk為false: 依既有「未帶」之拒絕(can not <入口> by data[k] without valid id when autoGenPk is false), 整批reject, error事件恰一次, 資料庫不變
//  O4 del: 繼承之id視為未給, 該筆ok為0並附invalid id: not a string at [data[k].id], 同值之既有列不被刪除; 其餘既有處置不變(繼承之空字串、數字照原訊息)
//  O5 selectByPk收純量pk, 無「列」之繼承問題; 傳入物件(帶繼承id)者回null且不發事件(既有契約)
//  O6 自有可列舉之id(含自有遮蔽繼承者、自有存取器經複製成資料屬性)行為不變: 以自有值存入
//  O7 寫入之列只含自有可列舉之欄位(insert、insertBulk、save之插入路徑); 任何寫入後資料庫內每一列皆有字串id欄(不得出現「判為已帶之id未被存入」之列)
//  O8 判定不多讀有效輸入: 自有存取器id之getter恆只被讀1次(複製時); 繼承存取器id之getter於insert類入口亦只被讀1次(既有路徑取值), del為既有之2次(事先讀取與逐筆讀取), 不因判定而多讀
//每案使用全新資料庫; 驗證一律經select取資料庫內之列(使用者面之讀取管道, 不經被測之判定); 各案之繼承值與列內容逐案相異


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let root = path.join(__dirname, '_tmp', 'pk-own')

describe('pk own enumerable only', function() {
    let nFile = 0
    let nVal = 0
    let opened = []

    //reUuid, genIDSeq產生之主鍵(UUIDv7)
    let reUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

    let msgOf = (err) => (err instanceof Error ? err.message : String(err))

    before(function() {
        fs.rmSync(root, { recursive: true, force: true })
        fs.mkdirSync(root, { recursive: true })
    })
    afterEach(async function() {
        for (let wo of opened) {
            await wo.close()
        }
        opened = []
    })
    after(function() {
        fs.rmSync(root, { recursive: true, force: true })
    })

    //mk, 每案全新資料庫
    let mk = (opt = {}) => {
        nFile++
        let wo = WOrm({ url: path.join(root, `c${nFile}`), db: 'worm', cl: 'c', ...opt })
        opened.push(wo)
        let evs = []
        wo.on('error', (mode, data, err) => {
            evs.push({ type: 'error', mode, data, err })
        })
        wo.on('change', (mode, data, res) => {
            evs.push({ type: 'change', mode, data, res })
        })
        return { wo, evs }
    }

    //snap, 取資料庫內全部列之快照(JSON字串, 排序後可逐列比對, 與列之順序無關)
    let snap = async (c) => {
        let rows = await c.wo.select()
        return rows.map((v) => JSON.stringify(v)).sort()
    }

    let nType = (evs, type, mode) => evs.filter((v) => v.type === type && (mode === undefined || v.mode === mode)).length

    //uniq, 逐案相異之值(繼承id、列名、數值皆取自此, 看值即知來自哪一案)
    let uniq = () => {
        nVal++
        return nVal
    }

    //seed, 先寫入一筆與兩筆其他列: 一筆與繼承值同id(collide時)與兩筆其他列; 回傳seed後資料庫內容之快照
    let seed = async (c, idv, collide) => {
        let k = uniq()
        await c.wo.insert({ id: `seed-a-${k}`, name: `seed-name-${k}`, v: k * 10 + 1 })
        await c.wo.insert({ id: `seed-b-${k}`, name: `seed-name-b-${k}`, v: k * 10 + 2 })
        if (collide) {
            await c.wo.insert({ id: idv, name: `seed-collide-${k}`, v: k * 10 + 3 })
        }
        c.evs.length = 0
        return snap(c)
    }

    //allHaveId, O7: 資料庫內每一列皆有字串id欄(不得出現「判為已帶之id未被存入」之列)
    let allHaveId = async (c) => {
        for (let v of await c.wo.select()) {
            assert.ok(Object.prototype.hasOwnProperty.call(v, 'id') && typeof v.id === 'string' && v.id !== '', `row without id: ${JSON.stringify(v)}`)
        }
    }

    //列形態: make(idv, nm)回傳一筆列; inhStr為true者其繼承值為字串(可與資料庫內同值之既有列衝突), 其餘為非字串之繼承值
    let mkClassGetter = (idv, nm) => {
        class C {
            get id() {
                return idv
            }
        }
        let o = new C()
        o.name = nm
        return o
    }
    let shapes = [
        {
            name: 'inherited string (plain prototype)',
            inhStr: true,
            make: (idv, nm) => Object.assign(Object.create({ id: idv }), { name: nm }),
        },
        {
            name: 'inherited string via class getter (getter-only on prototype)',
            inhStr: true,
            make: mkClassGetter,
        },
        {
            name: 'inherited string from frozen prototype',
            inhStr: true,
            make: (idv, nm) => Object.assign(Object.create(Object.freeze({ id: idv })), { name: nm }),
        },
        {
            name: 'own non-enumerable id shadowing inherited string',
            inhStr: true,
            make: (idv, nm) => {
                let o = Object.create({ id: idv })
                o.name = nm
                Object.defineProperty(o, 'id', { value: `ne-${idv}`, enumerable: false })
                return o
            },
        },
        {
            name: 'inherited string via a Proxy prototype',
            inhStr: true,
            make: (idv, nm) => Object.assign(Object.create(new Proxy({ id: idv }, {})), { name: nm }),
        },
        {
            name: 'own non-enumerable id, nothing inherited below',
            inhStr: false,
            make: (idv, nm) => {
                let o = { name: nm }
                Object.defineProperty(o, 'id', { value: `ne-${idv}`, enumerable: false })
                return o
            },
        },
        {
            name: 'inherited number',
            inhStr: false,
            make: (idv, nm) => Object.assign(Object.create({ id: 1000 + Number(nm.slice(4)) }), { name: nm }), //nm為row-<k>, 各案之數字相異
        },
        {
            name: 'inherited object',
            inhStr: false,
            make: (idv, nm) => Object.assign(Object.create({ id: { a: `OBJ-${nm}` } }), { name: nm }),
        },
    ]

    for (let autoGenPk of [true, false]) {

        describe(`autoGenPk: ${autoGenPk}`, function() {

            //O2、O3、O7: insert、insertBulk、save對繼承/自有不可列舉之id, 於autoGenPk為true補新主鍵、為false依「未帶」拒絕
            for (let api of ['insert', 'insertBulk', 'save']) {
                for (let sh of shapes) {
                    for (let collide of (sh.inhStr ? [false, true] : [false])) {
                        it(`${api}: ${sh.name}${collide ? ', existing row with same value' : ''}`, async function() {
                            let t = mk({ autoGenPk })
                            let k = uniq()
                            let idv = `inh-${k}`
                            let nm = `row-${k}`
                            let before = await seed(t, idv, collide)
                            let rej = null
                            let r = null
                            try {
                                r = await t.wo[api]([sh.make(idv, nm)])
                            }
                            catch (err) {
                                rej = err
                            }
                            if (autoGenPk) {
                                assert.ok(rej === null, `must resolve, rejected ${rej === null ? '' : msgOf(rej)}`)
                                //O2: 回傳為一筆新插入
                                if (api === 'save') {
                                    assert.deepStrictEqual(r, [{ n: 1, nInserted: 1, nModified: 0, ok: 1 }])
                                }
                                else {
                                    assert.deepStrictEqual(r, { n: 1, nInserted: 1, ok: 1 })
                                }
                                //O2: 既有列(含與繼承值同id者)原樣, 另多出一筆帶新主鍵之列
                                let after = await snap(t)
                                assert.strictEqual(after.length, before.length + 1)
                                for (let b of before) {
                                    assert.ok(after.includes(b), `existing row must stay intact: ${b}`)
                                }
                                let added = JSON.parse(after.filter((x) => !before.includes(x))[0])
                                assert.ok(reUuid.test(added.id), `id must be generated, got ${JSON.stringify(added.id)}`)
                                assert.notStrictEqual(added.id, idv)
                                assert.strictEqual(added.name, nm)
                                assert.deepStrictEqual(Object.keys(added).sort(), ['id', 'name']) //O7: 只含自有可列舉之欄位
                                await allHaveId(t)
                                assert.strictEqual(nType(t.evs, 'error'), 0)
                            }
                            else {
                                //O3: 依「未帶」之既有拒絕, 整批reject, 不寫入
                                let msg = `can not ${api} by data[0] without valid id when autoGenPk is false`
                                assert.strictEqual(rej === null ? `resolved ${JSON.stringify(r)}` : msgOf(rej), msg)
                                assert.deepStrictEqual(await snap(t), before)
                                assert.strictEqual(nType(t.evs, 'error', api), 1)
                                assert.strictEqual(t.evs[0].err, msg)
                                assert.strictEqual(nType(t.evs, 'change'), 0)
                            }
                        })
                    }
                }
            }

            //O2、O3: 批次內之索引與混合(自有者原樣、繼承者補值); 同批相同之繼承值不互相衝突
            for (let api of ['insert', 'insertBulk', 'save']) {
                it(`${api}: batch mixing own and inherited ids`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    let idv = `inh-${k}`
                    let rows = [
                        { id: `own-a-${k}`, name: `a-${k}` },
                        Object.assign(Object.create({ id: idv }), { name: `b-${k}` }),
                        { id: `own-c-${k}`, name: `c-${k}` },
                        Object.assign(Object.create({ id: idv }), { name: `d-${k}` }),
                    ]
                    let rej = null
                    try {
                        await t.wo[api](rows)
                    }
                    catch (err) {
                        rej = err
                    }
                    if (autoGenPk) {
                        assert.ok(rej === null, `must resolve, rejected ${rej === null ? '' : msgOf(rej)}`)
                        let after = await t.wo.select()
                        assert.strictEqual(after.length, 4)
                        let byName = {}
                        for (let v of after) {
                            byName[v.name] = v.id
                        }
                        assert.deepStrictEqual(Object.keys(byName).sort(), [`a-${k}`, `b-${k}`, `c-${k}`, `d-${k}`])
                        assert.strictEqual(byName[`a-${k}`], `own-a-${k}`)
                        assert.strictEqual(byName[`c-${k}`], `own-c-${k}`)
                        assert.ok(reUuid.test(byName[`b-${k}`]) && reUuid.test(byName[`d-${k}`]))
                        assert.notStrictEqual(byName[`b-${k}`], byName[`d-${k}`]) //同批相同之繼承值不得互相衝突, 各得新主鍵
                        await allHaveId(t)
                    }
                    else {
                        //索引為第一筆未帶者(data[1])
                        assert.strictEqual(rej === null ? 'resolved' : msgOf(rej), `can not ${api} by data[1] without valid id when autoGenPk is false`)
                        assert.deepStrictEqual(await snap(t), [])
                        assert.strictEqual(nType(t.evs, 'error', api), 1)
                    }
                })
            }

            //O2、O3: save之option(merge: false整筆覆寫、autoInsert: false不存在不插入)搭繼承之id, 繼承值不得命中同值之既有列(不覆寫、不更新)
            for (let opt of [{ merge: false }, { autoInsert: false }, { merge: false, autoInsert: false }]) {
                for (let collide of [false, true]) {
                    it(`save with option ${JSON.stringify(opt)}: inherited id${collide ? ', existing row with same value' : ''}`, async function() {
                        let t = mk({ autoGenPk })
                        let k = uniq()
                        let idv = `inh-${k}`
                        let nm = `row-${k}`
                        let before = await seed(t, idv, collide)
                        let rej = null
                        let r = null
                        try {
                            r = await t.wo.save([Object.assign(Object.create({ id: idv }), { name: nm })], opt)
                        }
                        catch (err) {
                            rej = err
                        }
                        if (!autoGenPk) {
                            //O3: 依「未帶」之既有拒絕(先於option之處理), 資料庫不變
                            assert.strictEqual(rej === null ? `resolved ${JSON.stringify(r)}` : msgOf(rej), 'can not save by data[0] without valid id when autoGenPk is false')
                            assert.deepStrictEqual(await snap(t), before)
                        }
                        else if (opt.autoInsert === false) {
                            //O2: 新補之主鍵必不存在, autoInsert為false故不插入; 同值之既有列原封不動(修前為被覆寫/更新)
                            assert.ok(rej === null, `must resolve, rejected ${rej === null ? '' : msgOf(rej)}`)
                            assert.deepStrictEqual(r, [{ n: 0, nInserted: 0, nModified: 0, ok: 1 }])
                            assert.deepStrictEqual(await snap(t), before)
                        }
                        else {
                            //O2: 新補主鍵之一筆新插入, 既有列原封不動
                            assert.ok(rej === null, `must resolve, rejected ${rej === null ? '' : msgOf(rej)}`)
                            assert.deepStrictEqual(r, [{ n: 1, nInserted: 1, nModified: 0, ok: 1 }])
                            let after = await snap(t)
                            assert.strictEqual(after.length, before.length + 1)
                            for (let b of before) {
                                assert.ok(after.includes(b), `existing row must stay intact: ${b}`)
                            }
                            let added = JSON.parse(after.filter((x) => !before.includes(x))[0])
                            assert.ok(reUuid.test(added.id))
                            assert.strictEqual(added.name, nm)
                            await allHaveId(t)
                        }
                    })
                }
            }

            //O4: del之批次內自有與繼承混合, 訊息之索引為批次索引; 自有者(含不存在者)照既有處置, 繼承者不刪同值之既有列
            it('del: batch mixing own and inherited ids reports the batch index and deletes only the own one', async function() {
                let t = mk({ autoGenPk })
                let k = uniq()
                let idv = `inh-${k}`
                await seed(t, idv, true)
                await t.wo.insert({ id: `own-${k}`, name: `own-name-${k}` })
                let before = await snap(t)
                let r = await t.wo.del([
                    { id: `own-${k}` },
                    { id: `no-such-${k}` },
                    Object.assign(Object.create({ id: idv }), { name: `row-${k}` }),
                ])
                assert.deepStrictEqual(r, [
                    { n: 1, nDeleted: 1, ok: 1 },
                    { n: 0, nDeleted: 0, ok: 1 }, //自有而不存在: 照既有(未命中, ok為1)
                    { n: 0, nDeleted: 0, ok: 0, err: 'invalid id: not a string at [data[2].id]' },
                ])
                let after = await snap(t)
                assert.strictEqual(after.length, before.length - 1)
                assert.ok(!after.some((x) => JSON.parse(x).id === `own-${k}`))
                assert.ok(after.some((x) => JSON.parse(x).id === idv)) //與繼承值同id之既有列仍在
            })

            //O4: del對繼承之id視為未給, 同值之既有列不被刪除(不論autoGenPk)
            for (let sh of shapes) {
                for (let collide of (sh.inhStr ? [false, true] : [false])) {
                    it(`del: ${sh.name}${collide ? ', existing row with same value' : ''}`, async function() {
                        let t = mk({ autoGenPk })
                        let k = uniq()
                        let idv = `inh-${k}`
                        let before = await seed(t, idv, collide)
                        let r = await t.wo.del([sh.make(idv, `row-${k}`)])
                        assert.deepStrictEqual(r, [{ n: 0, nDeleted: 0, ok: 0, err: 'invalid id: not a string at [data[0].id]' }])
                        assert.deepStrictEqual(await snap(t), before) //既有列(含與繼承值同id者)皆在
                        assert.strictEqual(nType(t.evs, 'error', 'del'), 1)
                    })
                }
            }

        })
    }

    describe('controls: own enumerable id unchanged (O6)', function() {

        for (let autoGenPk of [true, false]) {
            for (let api of ['insert', 'insertBulk', 'save']) {
                it(`${api}: own string id stored as given (autoGenPk: ${autoGenPk})`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    await t.wo[api]([{ id: `own-${k}`, name: `n-${k}` }])
                    assert.deepStrictEqual(await t.wo.select(), [{ id: `own-${k}`, name: `n-${k}` }])
                })

                it(`${api}: own id shadowing an inherited id is stored (autoGenPk: ${autoGenPk})`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    let row = Object.assign(Object.create({ id: `inh-${k}` }), { id: `own-${k}`, name: `n-${k}` })
                    await t.wo[api]([row])
                    assert.deepStrictEqual(await t.wo.select(), [{ id: `own-${k}`, name: `n-${k}` }])
                })

                it(`${api}: own accessor id is copied as a data property (autoGenPk: ${autoGenPk})`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    let row = { name: `n-${k}` }
                    Object.defineProperty(row, 'id', {
                        get() {
                            return `acc-${k}`
                        },
                        enumerable: true,
                        configurable: true,
                    })
                    await t.wo[api]([row])
                    assert.deepStrictEqual(await t.wo.select(), [{ name: `n-${k}`, id: `acc-${k}` }])
                })
            }

            it(`del: own string id deletes the row (autoGenPk: ${autoGenPk})`, async function() {
                let t = mk({ autoGenPk })
                let k = uniq()
                await t.wo.insert({ id: `own-${k}`, name: `n-${k}` })
                await t.wo.insert({ id: `keep-${k}`, name: `keep-${k}` })
                let row = Object.assign(Object.create({ id: `inh-${k}` }), { id: `own-${k}` }) //自有遮蔽繼承
                let r = await t.wo.del([row])
                assert.deepStrictEqual(r, [{ n: 1, nDeleted: 1, ok: 1 }])
                assert.deepStrictEqual((await t.wo.select()).map((v) => v.id), [`keep-${k}`])
            })
        }

        //O4 不變者: 繼承之空字串、數字(非有效主鍵)照原訊息, 不因改動而變
        it('del: inherited empty string keeps its message (string is empty)', async function() {
            let t = mk()
            let k = uniq()
            let r = await t.wo.del([Object.assign(Object.create({ id: '' }), { name: `n-${k}` })])
            assert.deepStrictEqual(r, [{ n: 0, nDeleted: 0, ok: 0, err: 'invalid id: string is empty at [data[0].id]' }])
        })
        it('del: inherited number keeps its message (not a string)', async function() {
            let t = mk()
            let k = uniq()
            let r = await t.wo.del([Object.assign(Object.create({ id: 123 }), { name: `n-${k}` })])
            assert.deepStrictEqual(r, [{ n: 0, nDeleted: 0, ok: 0, err: 'invalid id: not a string at [data[0].id]' }])
        })

        //O7: 繼承之可列舉非id欄位不存入(insert、insertBulk、save之插入路徑)
        for (let api of ['insert', 'insertBulk', 'save']) {
            it(`${api}: inherited enumerable non-id field is not stored`, async function() {
                let t = mk()
                let k = uniq()
                let row = Object.assign(Object.create({ extra: `INH-${k}` }), { id: `own-${k}`, name: `n-${k}` })
                await t.wo[api]([row])
                assert.deepStrictEqual(await t.wo.select(), [{ id: `own-${k}`, name: `n-${k}` }])
            })
        }

        //對照(既有契約不變): 單一物件而全部鍵皆在原型上(無任何自有可列舉鍵)者為輸入無效(wsemi iseobj為false), 不寫入、不發事件
        it('a bare object whose keys are all inherited is invalid input (early return, nothing written)', async function() {
            let t = mk()
            let k = uniq()
            await t.wo.insert({ id: `seed-${k}`, name: `seed-${k}` })
            let before = await snap(t)
            t.evs.length = 0
            let row = () => Object.create({ id: `inh-${k}`, name: `n-${k}` })
            assert.deepStrictEqual(await t.wo.insert(row()), { n: 0, nInserted: 0, ok: 1 })
            assert.deepStrictEqual(await t.wo.insertBulk(row()), { n: 0, nInserted: 0, ok: 1 })
            assert.deepStrictEqual(await t.wo.save(row()), [])
            assert.deepStrictEqual(await t.wo.del(row()), [])
            assert.deepStrictEqual(await snap(t), before)
            assert.strictEqual(t.evs.length, 0)
        })

        //O5: selectByPk收純量pk; 傳入物件(帶繼承id)回null且不發事件; 純量pk照常
        it('selectByPk: object carrying an inherited id returns null without events; plain pk still works', async function() {
            let t = mk()
            let k = uniq()
            await t.wo.insert({ id: `inh-${k}`, name: `seed-${k}` })
            t.evs.length = 0
            let r = await t.wo.selectByPk(Object.assign(Object.create({ id: `inh-${k}` }), { name: 'n' }))
            assert.strictEqual(r, null)
            assert.strictEqual(t.evs.length, 0)
            assert.deepStrictEqual(await t.wo.selectByPk(`inh-${k}`), { id: `inh-${k}`, name: `seed-${k}` })
        })

    })

    //O8: 判定不多讀有效輸入
    describe('read counts (O8)', function() {
        for (let autoGenPk of [true, false]) {
            for (let api of ['insert', 'insertBulk', 'save', 'del']) {
                it(`${api}: own accessor id getter is read exactly once (autoGenPk: ${autoGenPk})`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    let cnt = 0
                    let row = { name: `n-${k}` }
                    Object.defineProperty(row, 'id', {
                        get() {
                            cnt++
                            return `acc-${k}`
                        },
                        enumerable: true,
                        configurable: true,
                    })
                    await t.wo[api]([row])
                    assert.strictEqual(cnt, 1)
                })

                //insert類入口以既有路徑取值一次(補值後落在自有資料屬性不再讀原型上之getter), 拒絕者亦一次; del既有為事先讀取與逐筆讀取共2次, 判定(propertyIsEnumerable)不增讀
                it(`${api}: inherited accessor id getter is read ${api === 'del' ? 'twice (existing pre-read and per-row read)' : 'exactly once'} (autoGenPk: ${autoGenPk})`, async function() {
                    let t = mk({ autoGenPk })
                    let k = uniq()
                    let cnt = 0
                    class C {
                        get id() {
                            cnt++
                            return `inh-${k}`
                        }
                    }
                    let row = new C()
                    row.name = `n-${k}`
                    try {
                        await t.wo[api]([row])
                    }
                    catch (err) {
                        //autoGenPk為false時insert類入口依「未帶」reject, 讀取次數仍須為1
                    }
                    assert.strictEqual(cnt, api === 'del' ? 2 : 1)
                })
            }
        }
    })

})
