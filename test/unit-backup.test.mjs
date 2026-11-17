import assert from 'assert'
import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import { fork, spawnSync } from 'child_process'
import w from 'wsemi'
import { open } from 'lmdb'
import WOrm from '../src/WOrmLmdb.mjs'


//一致性備份(w-orm-lmdb專屬擴充, 規則見src/WOrmLmdb.mjs之backup JSDoc、README與帳本R-BACKUP-01; 來源: Gitea issue #11)
//備份以lmdb-js之backup(mdb_env_copy)於讀交易下產生某一提交點之快照; 本檔驗證: 一致性(同程序與另一程序寫入中)、還原後逐筆相同、
//目標自建與只產出data.mdb、目標路徑之安全(不覆蓋非空目錄)、字串良構、錯誤路徑、備份進行中close()不崩潰
describe('backup', function() {
    let root = './test/_tmp/backup'

    //msgOf, 取reject值之訊息
    let msgOf = (err) => (err instanceof Error ? err.message : String(err))

    let seq = 0

    //mkWo, 建立來源資料表之實例(每案獨立資料夾)
    let mkWo = (extraOpt = {}) => {
        seq++
        return WOrm({ url: `${root}/src${seq}`, db: 'worm', cl: 't', ...extraOpt })
    }

    //tdir, 每案獨立之備份目標資料夾(不預先建立)
    let tdir = (name = 'tar') => {
        seq++
        return `${root}/${name}${seq}`
    }

    //rowsOf, 全部數據依id排序
    let rowsOf = async(wo) => (await wo.select()).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))

    //restore, 還原: 將data.mdb置於資料表資料夾(url/db/cl)內, 以WOrmLmdb開啟
    let restore = (fpData) => {
        seq++
        let url = `${root}/restore${seq}`
        let dir = path.join(url, 'worm', 't')
        fs.mkdirSync(dir, { recursive: true })
        fs.copyFileSync(fpData, path.join(dir, 'data.mdb'))
        let wr = WOrm({ url, db: 'worm', cl: 't' })
        wr.dirUrl = url
        return wr
    }

    //sha, 檔案之雜湊
    let sha = (fp) => crypto.createHash('sha256').update(fs.readFileSync(fp)).digest('hex')

    //observe, 記錄error與change事件
    let observe = (wo) => {
        let evs = []
        wo.on('error', (mode, data, err) => evs.push({ t: 'error', mode, data, err }))
        wo.on('change', (mode) => evs.push({ t: 'change', mode }))
        return evs
    }

    //sample, 具代表性之數據(中文、emoji、U+FFFD、巢狀、陣列、Date、數字、布林、null)
    let sample = (n) => {
        let rs = []
        for (let i = 0; i < n; i++) {
            rs.push({
                id: `id-${i}`,
                name: `名稱${i}`,
                emoji: `a\u{1F600}${i}`,
                fffd: `x�${i}`,
                nested: { a: { b: [i, `s${i}`, { c: true }] } },
                tags: ['t1', `t${i}`],
                d: new Date(Date.UTC(2026, 9, 20, 0, 0, i)),
                num: i * 1.5,
                flag: i % 2 === 0,
                nul: null,
            })
        }
        return rs
    }

    //chkPrefix, 一致性: 備份中之批資料(id為b<n>-<i>, 每批nPer筆、批號n由0起連續)須為[批0至批m-1各完整]之前綴(提交點之快照)
    let chkPrefix = (rows, nPer, minBatch = 1) => {
        let kp = new Map()
        for (let r of rows) {
            let m = /^b(\d+)-(\d+)$/.exec(r.id)
            assert.strict.ok(m, `unexpected id ${r.id}`)
            let n = Number(m[1])
            if (!kp.has(n)) {
                kp.set(n, new Set())
            }
            kp.get(n).add(Number(m[2]))
            assert.strict.equal(r.n, n)
        }
        let ns = [...kp.keys()].sort((a, b) => a - b)
        assert.strict.ok(ns.length >= minBatch, `batches ${ns.length} < ${minBatch}`)
        ns.forEach((n, i) => {
            assert.strict.equal(n, i, `batch numbers should be consecutive from 0 but got ${n} at ${i}`)
            assert.strict.equal(kp.get(n).size, nPer, `batch ${n} should be complete`)
        })
        return ns.length
    }

    //mkWriter, 同程序之持續寫入者(以wo.insertBulk, 每批nPer筆、批號n由0起連續, 故任一提交點之資料必為[批0至批m-1各完整]之前綴):
    //nIssued為已發出之批數(下一批之批號), nBatch為已提交之批數(批依序發出與提交, 故批號小於nBatch者皆已提交);
    //每批提交即通知等候者(以事件同步, 不以延時偵測), 寫入失敗則等候者reject; stop設為true後於下一批前結束, pm於結束時resolve(寫入錯誤記於err)
    let mkWriter = (wo, nPer) => {
        let pad = 'x'.repeat(2000)
        let st = { nIssued: 0, nBatch: 0, stop: false, err: null, pm: null, waitFor: null }
        let waiters = []
        let notify = () => {
            for (let x of waiters.slice()) {
                if (st.err !== null) {
                    waiters.splice(waiters.indexOf(x), 1)
                    x.reject(st.err)
                }
                else if (x.pred()) {
                    waiters.splice(waiters.indexOf(x), 1)
                    x.resolve()
                }
            }
        }
        st.waitFor = (pred) => new Promise((resolve, reject) => {
            if (st.err !== null) {
                reject(st.err)
            }
            else if (pred()) {
                resolve()
            }
            else {
                waiters.push({ pred, resolve, reject })
            }
        })
        st.pm = (async() => {
            try {
                for (;;) {
                    if (st.stop) {
                        break
                    }
                    let n = st.nIssued++
                    let docs = []
                    for (let i = 0; i < nPer; i++) {
                        docs.push({ id: `b${n}-${i}`, n, i, pad })
                    }
                    await wo.insertBulk(docs)
                    st.nBatch++
                    notify()
                    await new Promise((resolve) => setImmediate(resolve))
                }
            }
            catch (err) {
                st.err = err
                notify()
            }
        })()
        return st
    }

    before(function() {
        w.fsDeleteFolder(root)
        fs.mkdirSync(root, { recursive: true })
    })

    after(function() {
        w.fsDeleteFolder(root)
        assert.strict.ok(!fs.existsSync(root), 'test folder should be removed')
    })

    describe('基本功能', function() {
        it('目標(含上層)不存在時自建, 完成後目標內只有data.mdb, 回傳{ok,fpData,size,compact}, 且不發出任何事件', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(300))
                let evs = observe(wo)
                let t = `${tdir('a')}/x/y`
                let r = await wo.backup(t)
                assert.strict.equal(r.ok, 1)
                assert.strict.equal(r.compact, true)
                assert.strict.equal(r.fpData, path.resolve(t, 'data.mdb'))
                assert.strict.equal(r.size, fs.statSync(r.fpData).size)
                assert.strict.ok(r.size > 0)
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['data.mdb'])
                assert.strict.deepStrictEqual(evs, [])
            }
            finally {
                await wo.close()
            }
        })

        it('還原後逐筆相同: 以WOrmLmdb開啟還原之資料表, select與逐筆selectByPk皆與來源相同', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(300))
                let t = tdir()
                let r = await wo.backup(t)
                let rs = await rowsOf(wo)
                assert.strict.equal(rs.length, 300)
                let wr = restore(r.fpData)
                try {
                    assert.strict.deepStrictEqual(await rowsOf(wr), rs)
                    for (let x of rs) {
                        assert.strict.deepStrictEqual(await wr.selectByPk(x.id), x)
                    }
                }
                finally {
                    await wr.close()
                }

                //直接以lmdb開啟備份資料夾(唯讀)亦同
                let raw = open({ path: path.resolve(t), compression: true, useVersions: true, readOnly: true })
                try {
                    let n = 0
                    for (let kv of raw.getRange()) {
                        n++
                        assert.strict.deepStrictEqual(kv.value, rs.find((v) => v.id === kv.key))
                    }
                    assert.strict.equal(n, 300)
                }
                finally {
                    await raw.close()
                }
            }
            finally {
                await wo.close()
            }
        })

        it('備份不改變來源且實例照常可用: 備份後來源數據不變, 其後之insert、select、備份皆正常', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(50))
                let before = await rowsOf(wo)
                await wo.backup(tdir())
                assert.strict.deepStrictEqual(await rowsOf(wo), before)
                await wo.insert({ id: 'later', name: 'later' })
                assert.strict.equal((await wo.select()).length, 51)
                let r2 = await wo.backup(tdir())
                let wr = restore(r2.fpData)
                try {
                    assert.strict.equal((await wr.select()).length, 51)
                }
                finally {
                    await wr.close()
                }
            }
            finally {
                await wo.close()
            }
        })

        it('空資料表可備份, 還原後亦為空', async function() {
            let wo = mkWo()
            try {
                let r = await wo.backup(tdir())
                assert.strict.deepStrictEqual(fs.readdirSync(path.dirname(r.fpData)), ['data.mdb'])
                let wr = restore(r.fpData)
                try {
                    assert.strict.deepStrictEqual(await wr.select(), [])
                }
                finally {
                    await wr.close()
                }
            }
            finally {
                await wo.close()
            }
        })

        it('option.compact: 預設與僅布林true壓實, false不壓實(碎片多時較大); 非布林值視同預設', async function() {
            let wo = mkWo()
            try {
                //製造碎片: 寫入大量後刪除大部分
                let docs = []
                for (let i = 0; i < 3000; i++) {
                    docs.push({ id: `k${i}`, name: 'z'.repeat(1500), i })
                }
                await wo.insertBulk(docs)
                await wo.delAll({ i: { $gte: 100 } })
                let rs = await rowsOf(wo)
                assert.strict.equal(rs.length, 100)
                let rDef = await wo.backup(tdir())
                let rTrue = await wo.backup(tdir(), { compact: true })
                let rFalse = await wo.backup(tdir(), { compact: false })
                let rStr = await wo.backup(tdir(), { compact: 'false' })
                assert.strict.equal(rDef.compact, true)
                assert.strict.equal(rTrue.compact, true)
                assert.strict.equal(rFalse.compact, false)
                assert.strict.equal(rStr.compact, true)
                assert.strict.equal(rDef.size, rTrue.size)
                assert.strict.equal(rStr.size, rTrue.size)
                assert.strict.ok(rFalse.size > rTrue.size, `non-compact ${rFalse.size} should be larger than compact ${rTrue.size}`)

                //兩者還原後皆逐筆相同
                for (let r of [rTrue, rFalse]) {
                    let wr = restore(r.fpData)
                    try {
                        assert.strict.deepStrictEqual(await rowsOf(wr), rs)
                    }
                    finally {
                        await wr.close()
                    }
                }
            }
            finally {
                await wo.close()
            }
        })

        it('相對路徑與含中文、emoji之資料夾名皆可', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(20))
                seq++
                let t = `${root}/備份-\u{1F600}-${seq}`
                let r = await wo.backup(t)
                assert.strict.equal(path.isAbsolute(r.fpData), true)
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['data.mdb'])
                let wr = restore(r.fpData)
                try {
                    assert.strict.equal((await wr.select()).length, 20)
                }
                finally {
                    await wr.close()
                }
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('目標路徑之安全', function() {
        it('目標為已存在之空資料夾: 可備份, 資料夾沿用', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(10))
                let t = tdir()
                fs.mkdirSync(t, { recursive: true })
                let r = await wo.backup(t)
                assert.strict.equal(r.ok, 1)
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['data.mdb'])
            }
            finally {
                await wo.close()
            }
        })

        it('目標為已存在之非空資料夾: 整批reject, 不覆蓋、不寫入任何東西, 先發出一次error事件', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(10))
                let evs = observe(wo)
                let t = tdir()
                fs.mkdirSync(t, { recursive: true })
                fs.writeFileSync(path.join(t, 'keep.txt'), 'important')

                //預先檢核即拒收: 不做任何複製工作, 連暫存子資料夾都不建立(記錄fs.mkdirSync之路徑)
                let origMkdir = fs.mkdirSync
                let made = []
                fs.mkdirSync = (p, o) => {
                    made.push(String(p))
                    return origMkdir(p, o)
                }
                let rej = null
                try {
                    await wo.backup(t)
                }
                catch (err) {
                    rej = err
                }
                finally {
                    fs.mkdirSync = origMkdir
                }
                assert.strict.deepStrictEqual(made.filter((v) => /\.backup-/.test(v)), [], 'no temp folder should be created for a non-empty target')
                assert.strict.ok(rej instanceof Error)
                assert.strict.equal(rej.message, 'can not backup by target folder which is not empty')
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['keep.txt'])
                assert.strict.equal(fs.readFileSync(path.join(t, 'keep.txt'), 'utf8'), 'important')
                assert.strict.deepStrictEqual(evs, [{ t: 'error', mode: 'backup', data: null, err: rej.message }])
            }
            finally {
                await wo.close()
            }
        })

        it('目標資料夾已有data.mdb(重複備份至同一目標): reject, 前次產物位元組不變', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(30))
                let t = tdir()
                let r = await wo.backup(t)
                let h = sha(r.fpData)
                await wo.insert({ id: 'another', name: 'n' })
                await assert.rejects(() => wo.backup(t), (err) => msgOf(err) === 'can not backup by target folder which is not empty')
                assert.strict.equal(sha(r.fpData), h)
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['data.mdb'])
            }
            finally {
                await wo.close()
            }
        })

        it('目標為既有檔案: reject, 檔案不變', async function() {
            let wo = mkWo()
            try {
                let t = tdir()
                fs.writeFileSync(t, 'a file')
                await assert.rejects(() => wo.backup(t), (err) => msgOf(err) === 'can not backup by target which is not a folder')
                assert.strict.equal(fs.readFileSync(t, 'utf8'), 'a file')
            }
            finally {
                await wo.close()
            }
        })

        it('目標為來源資料表自身之資料夾或其內: reject, 來源與資料夾不變, 不建立子資料夾', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(10))
                let storage = `${root}/src${seq}/worm/t`
                let before = fs.readdirSync(storage).sort()
                await assert.rejects(() => wo.backup(storage), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                await assert.rejects(() => wo.backup(`${storage}/sub`), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                await assert.rejects(() => wo.backup(`${storage}/../t`), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                assert.strict.deepStrictEqual(fs.readdirSync(storage).sort(), before)
                assert.strict.equal((await wo.select()).length, 10)
            }
            finally {
                await wo.close()
            }
        })

        it('目標經連接點(junction)或符號連結別名指向來源資料表資料夾或其內: 依實體路徑判定而reject, 來源不變, 不建立子資料夾', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(10))
                let storage = path.resolve(`${root}/src${seq}/worm/t`)
                let before = fs.readdirSync(storage).sort()
                seq++
                let jn = path.resolve(`${root}/jn${seq}`)
                fs.symlinkSync(storage, jn, 'junction')
                try {
                    await assert.rejects(() => wo.backup(`${jn}/sub`), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                    await assert.rejects(() => wo.backup(`${jn}/a/b`), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                    await assert.rejects(() => wo.backup(jn), (err) => msgOf(err) === 'can not backup by target inside the source table folder')
                    assert.strict.deepStrictEqual(fs.readdirSync(storage).sort(), before)
                    assert.strict.equal((await wo.select()).length, 10)
                }
                finally {
                    fs.rmSync(jn, { recursive: false, force: true })
                }

                //來源之上一層(資料庫資料夾)之別名: 其下之同層新資料夾不在來源資料夾內, 允許
                let jdb = path.resolve(`${root}/jdb${seq}`)
                fs.symlinkSync(path.dirname(storage), jdb, 'junction')
                try {
                    let r = await wo.backup(`${jdb}/sibling`)
                    assert.strict.equal(r.ok, 1)
                    assert.strict.deepStrictEqual(fs.readdirSync(path.join(path.dirname(storage), 'sibling')), ['data.mdb'])
                    fs.rmSync(path.join(path.dirname(storage), 'sibling'), { recursive: true, force: true })
                }
                finally {
                    fs.rmSync(jdb, { recursive: false, force: true })
                }
            }
            finally {
                await wo.close()
            }
        })

        it('目標之上層為檔案(不可寫): reject且訊息為can not backup, 開頭, 檔案不變, 不留任何殘餘, 先發出一次error事件', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(10))
                let evs = observe(wo)
                seq++
                let fp = `${root}/afile${seq}`
                fs.writeFileSync(fp, 'x')
                let rej = null
                try {
                    await wo.backup(`${fp}/sub/deeper`)
                }
                catch (err) {
                    rej = err
                }
                assert.strict.ok(rej instanceof Error)
                assert.strict.ok(/^can not backup, /.test(rej.message), rej.message)
                assert.strict.equal(fs.readFileSync(fp, 'utf8'), 'x')
                assert.strict.deepStrictEqual(evs, [{ t: 'error', mode: 'backup', data: null, err: rej.message }])
            }
            finally {
                await wo.close()
            }
        })

        it('失敗後不留下本次建立之資料夾與暫存子資料夾(目標之上層不存在, 其最上層為檔案之下)', async function() {
            let wo = mkWo()
            try {
                seq++
                let top = `${root}/clean${seq}`
                fs.mkdirSync(top)
                let before = fs.readdirSync(top)
                fs.writeFileSync(path.join(top, 'f'), 'x')

                //目標之父層為檔案, mkdir於建立任何東西前即失敗
                await assert.rejects(() => wo.backup(path.join(top, 'f', 'x')))
                assert.strict.deepStrictEqual(fs.readdirSync(top).sort(), [...before, 'f'].sort())
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('失敗與崩潰之殘餘', function() {
        it('複製完成後移入data.mdb失敗(注入fs.renameSync失敗): reject, 目標(本次所建立)與暫存子資料夾皆移除, error事件恰一次', async function() {
            let wo = mkWo()
            let orig = fs.renameSync
            try {
                await wo.insertBulk(sample(10))
                let evs = observe(wo)
                let t = `${tdir('inject')}/up/down`
                let top = path.dirname(path.dirname(t))
                fs.renameSync = () => {
                    throw new Error('injected rename failure')
                }
                let rej = null
                try {
                    await wo.backup(t)
                }
                catch (err) {
                    rej = err
                }
                fs.renameSync = orig
                assert.strict.ok(rej instanceof Error)
                assert.strict.equal(rej.message, 'can not backup, injected rename failure')
                assert.strict.equal(fs.existsSync(path.join(top, 'up')), false, 'created folders should be removed')
                assert.strict.deepStrictEqual(evs, [{ t: 'error', mode: 'backup', data: null, err: rej.message }])

                //本次建立之上層資料夾內已有他人於其間建立之內容: 清理只移除空資料夾, 不刪非本次建立之內容
                let t1b = `${tdir('inject-sibling')}/up/down`
                let topB = path.join(path.dirname(path.dirname(t1b)), 'up')
                fs.renameSync = () => {
                    fs.writeFileSync(path.join(topB, 'sibling.txt'), 'keep')
                    throw new Error('injected rename failure')
                }
                await assert.rejects(() => wo.backup(t1b), (err) => msgOf(err) === 'can not backup, injected rename failure')
                fs.renameSync = orig
                assert.strict.equal(fs.readFileSync(path.join(topB, 'sibling.txt'), 'utf8'), 'keep', 'content created by others must survive the cleanup')
                assert.strict.deepStrictEqual(fs.readdirSync(topB), ['sibling.txt'], 'the target folder created by this call and its temp folder are removed')

                //已存在之空目標: 失敗後原樣保留(仍為空), 暫存子資料夾移除
                let t2 = tdir('inject-existing')
                fs.mkdirSync(t2, { recursive: true })
                fs.renameSync = () => {
                    throw new Error('injected rename failure')
                }
                await assert.rejects(() => wo.backup(t2), (err) => msgOf(err) === 'can not backup, injected rename failure')
                fs.renameSync = orig
                assert.strict.deepStrictEqual(fs.readdirSync(t2), [])

                //還原注入後同一實例照常可備份
                let r = await wo.backup(t2)
                assert.strict.deepStrictEqual(fs.readdirSync(t2), ['data.mdb'])
                assert.strict.equal(r.ok, 1)
            }
            finally {
                fs.renameSync = orig
                await wo.close()
            }
        })

        it('備份進行期間他處於目標內建立了檔案: 移入前再檢查, reject(不合併), 該檔案保留, 暫存子資料夾移除, 目標內無data.mdb', async function() {
            let wo = mkWo()
            let origMkdir = fs.mkdirSync
            try {
                await wo.insertBulk(sample(10))
                let t = tdir('race-file')
                fs.mkdirSync = (p, o) => {
                    let r = origMkdir(p, o)
                    if (/[\\/]\.backup-/.test(String(p))) {
                        //模擬備份進行期間他處於目標內建立檔案(於暫存夾建立之時點)
                        fs.writeFileSync(path.join(path.dirname(String(p)), 'note.txt'), 'by others')
                    }
                    return r
                }
                let rej = null
                try {
                    await wo.backup(t)
                }
                catch (err) {
                    rej = err
                }
                fs.mkdirSync = origMkdir
                assert.strict.ok(rej instanceof Error)
                assert.strict.equal(rej.message, 'can not backup by target folder which is not empty')
                assert.strict.deepStrictEqual(fs.readdirSync(t), ['note.txt'])
                assert.strict.equal(fs.readFileSync(path.join(t, 'note.txt'), 'utf8'), 'by others')
            }
            finally {
                fs.mkdirSync = origMkdir
                await wo.close()
            }
        })

        it('同一程序對同一目標並行備份: 恰一個成功且其data.mdb完整可還原, 另一個reject(非空), 無暫存殘餘', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(100))
                let rs = await rowsOf(wo)
                for (let i = 0; i < 4; i++) {
                    let t = tdir('par')

                    //凍結Date.now使兩次呼叫之毫秒時間相同(暫存夾名撞名之最壞情形), 暫存夾名須靠序號與亂數區分
                    let origNow = Date.now
                    Date.now = () => 1700000000000
                    let rr = null
                    try {
                        rr = await Promise.allSettled([wo.backup(t), wo.backup(t)])
                    }
                    finally {
                        Date.now = origNow
                    }
                    let ok = rr.filter((v) => v.status === 'fulfilled')
                    let ng = rr.filter((v) => v.status === 'rejected')
                    assert.strict.equal(ok.length, 1, `iteration ${i}: exactly one should succeed: ${JSON.stringify(rr.map((v) => v.status))}`)
                    assert.strict.equal(msgOf(ng[0].reason), 'can not backup by target folder which is not empty')
                    assert.strict.deepStrictEqual(fs.readdirSync(t), ['data.mdb'])
                    let wr = restore(ok[0].value.fpData)
                    try {
                        assert.strict.deepStrictEqual(await rowsOf(wr), rs)
                    }
                    finally {
                        await wr.close()
                    }
                }
            }
            finally {
                await wo.close()
            }
        })

        it('備份進行中程序被強制終止: 目標內不出現殘缺之data.mdb(出現者必完整可開啟), 殘留僅限暫存子資料夾', async function() {
            seq++
            let url = path.resolve(`${root}/kill${seq}`)
            let fpTar = path.resolve(`${root}/kill-tar${seq}`)
            let child = fork(path.resolve('./test/tools/killDuringBackup.mjs'), [url, fpTar], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] })
            let msg = await new Promise((resolve, reject) => {
                child.on('message', (m) => {
                    if (m.calling === true || m.done === true) {
                        resolve(m)
                    }
                })
                child.on('exit', () => resolve({ exited: true }))
                setTimeout(() => reject(new Error('timeout waiting for child')), 150000).unref()
            })
            assert.strict.equal(msg.calling, true, 'child should report calling backup')
            let pmExit = new Promise((resolve) => child.on('exit', resolve))
            child.kill('SIGKILL')
            await pmExit

            //結果: 目標內之data.mdb不存在, 或已完整(終止前恰好完成)
            let names = fs.readdirSync(fpTar)
            if (names.includes('data.mdb')) {
                //於子程序開啟(殘缺之檔於lmdb原生層可能引發存取違規而使程序崩潰)
                let r = spawnSync(process.execPath, [path.resolve('./test/tools/countRows.mjs'), fpTar], { encoding: 'utf8', timeout: 120000 })
                assert.strict.equal(r.status, 0, `a data.mdb that appears must be complete and openable (exit ${r.status} ${r.stderr})`)
                assert.strict.equal(JSON.parse(r.stdout.trim()).n, 40000, 'a data.mdb that appears must be complete')
            }
            else {
                assert.strict.ok(names.every((v) => /^\.backup-/.test(v)), `only temp folders may remain: ${names}`)
            }
        })
    })

    describe('字串良構(路徑參數之孤立代理與NUL)', function() {
        it('fpTar非字串、空字串、含孤立代理字元或NUL: reject(固定詞彙、不回顯值), 不建立任何東西, error事件恰一次', async function() {
            let wo = mkWo()
            try {
                let before = fs.readdirSync(root).sort()
                let cases = [
                    [undefined, 'invalid fpTar: not a string'],
                    [null, 'invalid fpTar: not a string'],
                    [123, 'invalid fpTar: not a string'],
                    [{ a: 1 }, 'invalid fpTar: not a string'],
                    [['x'], 'invalid fpTar: not a string'],
                    ['', 'invalid fpTar: string is empty'],
                    [`${root}/bad\uD800`, 'invalid fpTar: string contains lone surrogate'],
                    [`${root}/bad\uDC00x`, 'invalid fpTar: string contains lone surrogate'],
                    [`${root}/bad\uD800\uD800`, 'invalid fpTar: string contains lone surrogate'],
                    [`${root}/nul\u0000x`, 'invalid fpTar: string contains NUL'],
                ]
                for (let [arg, msg] of cases) {
                    let evs = observe(wo)
                    await assert.rejects(() => wo.backup(arg), (err) => msgOf(err) === msg, msg)
                    assert.strict.deepStrictEqual(evs.map((v) => [v.t, v.mode, v.err]), [['error', 'backup', msg]], msg)
                    wo.removeAllListeners('error')
                    wo.removeAllListeners('change')
                }
                assert.strict.deepStrictEqual(fs.readdirSync(root).sort(), before)
            }
            finally {
                await wo.close()
            }
        })

        it('良構之路徑(含emoji等成對代理、U+FFFD)照常接受', async function() {
            let wo = mkWo()
            try {
                await wo.insertBulk(sample(5))
                seq++
                let r = await wo.backup(`${root}/ok-\u{1F600}-�-${seq}`)
                assert.strict.equal(r.ok, 1)
            }
            finally {
                await wo.close()
            }
        })
    })

    describe('已關閉之實例', function() {
        it('close()後呼叫backup: 整批reject(closed訊息), 不建立目標, error事件恰一次', async function() {
            let wo = mkWo()
            await wo.insertBulk(sample(5))
            let evs = observe(wo)
            await wo.close()
            let t = tdir()
            let rej = null
            try {
                await wo.backup(t)
            }
            catch (err) {
                rej = err
            }
            assert.strict.ok(rej instanceof Error)
            assert.strict.equal(rej.message, 'can not backup by closed client, instance can not be used after close()')
            assert.strict.equal(fs.existsSync(t), false)
            assert.strict.deepStrictEqual(evs, [{ t: 'error', mode: 'backup', data: null, err: rej.message }])
        })

        it('已關閉之實例傳壞fpTar仍回closed訊息(已關閉先於檢核)', async function() {
            let wo = mkWo()
            await wo.close()
            await assert.rejects(() => wo.backup('x\uD800'), (err) => /closed client/.test(msgOf(err)))
        })
    })

    describe('一致性(備份時有寫入)', function() {
        //事件同步(不以延時或寫入總批數之門檻偵測): 對應src/WOrmLmdb.mjs之backup JSDoc「不致產生撕裂…亦不阻擋寫入。快照含呼叫前已await完成之寫入」與帳本R-BACKUP-01規則1
        //  ①一致性: 每份快照皆為[批0至批m-1各完整]之前綴, 且至少含備份呼叫前已提交之批(nBefore), 至多為備份結束時已提交之批加1批(進行中者), 先後各份之批數不減;
        //  ②不阻擋寫入: 「備份呼叫之後才發出之批, 於該備份結束(settle)之前已提交」(見證)——被阻擋(備份期間寫入須等備份結束: 同步複製令事件迴圈停擺、以鎖或佇列使寫入排於備份之後)時,
        //    該批必於備份結束之後才提交, 任何一次備份皆不會有見證; 未被阻擋時, 該批之提交與備份之複製並行, 是否於單次備份結束前完成取決於兩者之相對快慢(備份窗口與一次提交相近, 實測單份備份見證之機率約13%至83%),
        //    故以事件等待見證出現(每份備份各為一次機會, 逐份皆驗證一致性), nMax僅為防懸置上限(阻擋時永不出現見證), 不表達規格, 亦不隨負載調整;
        //    不以填充大量資料放大備份窗口(大型資料庫加持續寫入時備份耗時暴增, 見sgud經驗檔2026-11-18條)
        it('同程序持有環境且寫入進行中: 連續備份, 每份皆為[批0至批m-1各完整]之前綴, 且備份期間寫入不被阻擋(備份呼叫後才發出之批, 於該備份結束前已提交)', async function() {
            let wo = mkWo()
            let nPer = 40
            let wt = mkWriter(wo, nPer)
            try {
                //先寫入一些批(等寫入端提交第5批之事件)
                await wt.waitFor(() => wt.nBatch >= 5)

                let nMin = 5
                let nMax = 150
                let nTry = 0
                let nWitness = 0
                let sizes = []
                while (nTry < nMin || (nWitness === 0 && nTry < nMax)) {
                    nTry++

                    //備份呼叫當下: 已發出之批數nIssuedAt(批號小於此者已發出, 批號不小於此者皆為呼叫之後才發出)、已提交之批數nBefore
                    let nIssuedAt = wt.nIssued
                    let nBefore = wt.nBatch
                    let t = tdir()

                    //備份結束(settle)當下已提交之批數nAtSettle(於結束之回調內同步取得, 與寫入端之提交依事件先後排序)
                    let nAtSettle = -1
                    let r = await wo.backup(t).then((x) => {
                        nAtSettle = wt.nBatch
                        return x
                    })
                    assert.strict.equal(wt.err, null, 'writer should not fail')

                    //①一致性
                    let wr = restore(r.fpData)
                    try {
                        let m = chkPrefix(await wr.select(), nPer, nBefore)
                        assert.strict.ok(m <= nAtSettle + 1, `snapshot batches ${m} should not exceed written ${nAtSettle}`)
                        sizes.push(m)
                    }
                    finally {
                        await wr.close()
                        w.fsDeleteFolder(wr.dirUrl)
                        w.fsDeleteFolder(t)
                    }

                    //②見證: 批號nIssuedAt為備份呼叫之後才發出之第一批, 已提交(nBatch > nIssuedAt)而備份尚未結束
                    if (nAtSettle > nIssuedAt) {
                        nWitness++
                    }
                }
                wt.stop = true
                await wt.pm
                assert.strict.equal(wt.err, null, 'writer should not fail')

                assert.strict.ok(nTry >= nMin, `backups ${nTry} < ${nMin}`)
                assert.strict.ok(nWitness > 0, `writes issued after backup() was called should be committed before that backup() settles (blocked by backup?): witness 0 in ${nTry} backups, writer committed ${wt.nBatch} batches`)

                //各份之批數隨時間不減
                for (let i = 1; i < sizes.length; i++) {
                    assert.strict.ok(sizes[i] >= sizes[i - 1], `snapshots should not shrink: ${sizes.join(',')}`)
                }
            }
            finally {
                wt.stop = true
                await wt.pm
                await wo.close()
            }
        })

        it('另一程序持續寫入中(本程序僅備份): 連續備份, 每份皆為[批0至批m-1各完整]之前綴, 寫入程序之進度持續增加', async function() {
            let wo = mkWo()
            seq++
            let storage = path.resolve(`${root}/src${seq - 1}/worm/t`)
            let child = fork(path.resolve('./test/tools/batchWriter.mjs'), [storage], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] })
            let progress = 0
            let waiters = []
            child.on('message', (m) => {
                if (m.progress !== undefined) {
                    progress = m.progress
                }
                for (let x of waiters.slice()) {
                    if (x.pred(m)) {
                        waiters.splice(waiters.indexOf(x), 1)
                        x.resolve(m)
                    }
                }
            })
            let waitMsg = (pred, ms = 60000) => new Promise((resolve, reject) => {
                let x = { pred, resolve }
                waiters.push(x)
                setTimeout(() => reject(new Error('timeout waiting for child')), ms).unref()
            })
            try {
                await waitMsg((m) => m.ready === true)

                //填充4萬列, 於 compression: true 下資料庫檔實測約6MB(相同內容之 pad 可壓縮), 備份窗口不長, 故不依賴備份與寫入重疊之長短;
                //斷言之下界由事件閘門決定(見下), 鑑別力在檢出缺筆, 批不連續, 填充列不全(直接複製檔案之錯誤作法於本機未見撕裂, 見帳本R-BACKUP-01已知限制①)
                let nFill = 40000
                child.send({ cmd: 'fill', n: nFill })
                await waitMsg((m) => m.filled === true, 120000)
                child.send({ cmd: 'start' })
                await waitMsg((m) => m.progress >= 10)

                let nPer = 40
                let ms = []
                for (let k = 0; k < 4; k++) {
                    let pBefore = progress
                    let t = tdir()
                    let r = await wo.backup(t)
                    let wr = restore(r.fpData)
                    try {
                        let rows = await wr.select()
                        assert.strict.equal(rows.filter((v) => /^f\d+$/.test(v.id)).length, nFill, 'filler rows should be complete')
                        ms.push(chkPrefix(rows.filter((v) => /^b\d+-\d+$/.test(v.id)), nPer, pBefore))
                    }
                    finally {
                        await wr.close()
                        w.fsDeleteFolder(wr.dirUrl)
                        w.fsDeleteFolder(t)
                    }
                    await waitMsg((m) => m.progress > pBefore)
                }
                let pmDone = waitMsg((m) => m.done === true)
                child.send({ cmd: 'stop' })
                let done = await pmDone
                assert.strict.ok(done.nBatch > 10 + 5, `child should keep progressing, got ${done.nBatch}`)
                for (let i = 1; i < ms.length; i++) {
                    assert.strict.ok(ms[i] >= ms[i - 1], `snapshots should not shrink: ${ms.join(',')}`)
                }
                assert.strict.ok(ms[ms.length - 1] <= done.nBatch)
            }
            finally {
                if (child.exitCode === null) {
                    child.kill()
                }
                await wo.close()
            }
        })
    })

    describe('備份進行中close()', function() {
        it('備份尚未結束即呼叫close(): 不崩潰(離開碼0), 備份完成, close等候其結束, 其間與其後之操作皆reject(closed)', function() {
            seq++
            let url = path.resolve(`${root}/closerace${seq}`)
            let fpTar = path.resolve(`${root}/closerace-tar${seq}`)
            let r = spawnSync(process.execPath, [path.resolve('./test/tools/closeDuringBackup.mjs'), url, fpTar], { encoding: 'utf8', timeout: 170000 })
            assert.strict.equal(r.status, 0, `exit ${r.status} signal ${r.signal} ${r.stderr}`)
            let lines = r.stdout.trim().split('\n').map((v) => JSON.parse(v))
            let kp = Object.assign({}, ...lines)
            assert.strict.equal(kp.end, true)
            assert.strict.equal(kp.backup.ok, 1)
            assert.strict.ok(kp.backup.size > 0)
            assert.strict.ok(/closed client/.test(kp.selectDuringClose), kp.selectDuringClose)
            assert.strict.equal(kp.closed, true)
            assert.strict.ok(/closed client/.test(kp.selectAfterClose), kp.selectAfterClose)
            assert.strict.deepStrictEqual(fs.readdirSync(fpTar), ['data.mdb'])
        })
    })
})
