//診斷輸出測試(R-DIAG-01)之共用情境: 對WOrmLmdb實例依序執行各操作(成功與失敗), 回傳各結果、事件payload與時間軸, 供回傳契約之比對
//同一情境亦於修改前之程式(git HEAD dfa8bb7)執行以產生固定預期值(diagGolden.json, 見genDiagGolden.mjs), 故此檔不得依賴R-DIAG-01之任何新功能


let ID = 'u-secret-001@example.com'


//scenario, withBadListener為true時change與error訂閱函數皆拋錯(觸發診斷輸出之站點), 拋出之訊息含ID與opt.url
//out: 各操作之結算(resolve值, 或reject之訊息與是否為Error實例與類別名); evs: 事件payload; tl: 事件與結算交錯之時間軸(鎖住「error事件先於reject」等時機)
async function scenario(WOrm, opt, withBadListener) {
    let evs = []
    let tl = []
    let attach = (w, tag) => {
        w.on('change', (m, d, r) => {
            evs.push(['change', m, d, r])
            tl.push(`ev:${tag}:change:${m}`)
            if (withBadListener) {
                throw new Error(`listener ${ID} ${opt.url}`)
            }
        })
        w.on('error', (m, d, e) => {
            evs.push(['error', m, d, e])
            tl.push(`ev:${tag}:error:${m}`)
            if (withBadListener) {
                throw new Error(`listener ${ID} ${opt.url}`)
            }
        })
    }
    let out = []
    let settle = async(pm) => {
        let r
        try {
            r = { ok: await pm }
        }
        catch (err) {
            r = { err: String(err && err.message ? err.message : err), isError: err instanceof Error, ctor: err && err.constructor ? err.constructor.name : typeof err }
        }
        tl.push(`settle:${out.length}:${r.err === undefined ? 'ok' : 'err'}`)
        out.push(r)
    }
    let w = WOrm({ ...opt, autoGenPk: true })
    attach(w, 'a')
    let w2 = WOrm({ ...opt, cl: `${opt.cl}b`, autoGenPk: false })
    attach(w2, 'b')
    let w3 = WOrm({ ...opt, cl: `${opt.cl}c`, useCache: true })
    attach(w3, 'c')

    await settle(w.delAll({}))
    await settle(w.insert([{ id: 'a1', name: 'n1', value: 1 }, { id: 'a2', name: 'n2', value: 2 }]))
    await settle(w.insert({ id: 'a1', name: 'dup' }))
    await settle(w.insertBulk([{ id: 'a1', name: 'dup' }]))
    await settle(w.insertBulk([{ id: 'b1', name: 'bulk1' }, { id: 'b2', name: 'bulk2' }]))
    await settle(w.insert({ id: 'r1', name: 'rl' }, { returnList: true }))
    await settle(w.save([{ id: 'a1', name: 'n1x' }, { id: 'a3', name: 'n3' }]))
    await settle(w.save({ id: 'a1', other: 1 }, { merge: false }))
    await settle(w.save({ id: 'zz', name: 'noauto' }, { autoInsert: false }))
    await settle(w.save([{ id: '\ud800', name: 'bad' }, { id: 's1', name: 'ok' }]))
    await settle(w.select({ id: 'a1' }))
    await settle(w.select({}))
    await settle(w.selectByPk('a2'))
    await settle(w.selectByPk('zz'))
    await settle(w.select({ $foo: 1 }))
    await settle(w.delAll({ $foo: 1 }))
    await settle(w.del([{ id: 'a2' }, { name: 'no-id' }]))
    await settle(w.selectByPk('a2'))
    await settle(w2.insert({ name: 'no-id' }))
    await settle(w.insert({ id: '\ud800', name: 'bad' }))
    await settle(w.backup(''))
    await settle(w3.insert({ id: 'c1', name: 'cache1' }))
    await settle(w3.select({}))
    await settle(w3.insert({ id: 'c2', name: 'cache2' }))
    await settle(w3.select({}))
    await settle(w.delAll({}))
    await w.close()
    await w2.close()
    await w3.close()
    await settle(w.insert({ id: 'late' }))
    await settle(w.select({}))
    await settle(w.backup('x'))
    return { out, evs, tl }
}


export { ID, scenario }
