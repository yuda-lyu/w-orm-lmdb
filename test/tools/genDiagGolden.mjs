//產生diagGolden.json: 以修改前之程式(git HEAD dfa8bb7, 尚無getDiag)執行diagScenario, 記錄各操作之回傳、事件payload與訂閱函數拋錯時之console輸出
//用法: node test/tools/genDiagGolden.mjs [WOrmLmdb模組路徑, 預設../../src/WOrmLmdb.mjs]; 產出為固定預期值, 不得以修改後之程式重產
//console輸出中之opt.url以<URL>代替(url隨機器而異)
import fs from 'fs'
import path from 'path'
import { fileURLToPath, pathToFileURL } from 'url'
import { scenario } from './diagScenario.mjs'


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let fpMod = path.resolve(process.argv[2] || path.join(__dirname, '..', '..', 'src', 'WOrmLmdb.mjs'))
let { default: WOrm } = await import(pathToFileURL(fpMod).href)

let dir = path.join(__dirname, '..', '_tmp', 'diaglog-golden')
fs.rmSync(dir, { recursive: true, force: true })
let url = path.join(dir, 'svc-S3cretPw@host')
let opt = { url, db: 'worm', cl: 'g' }
console.log('golden from', fpMod)

//capture, 擷取console各方法之輸出
let cap = async(fn) => {
    let calls = []
    let ks = ['log', 'info', 'warn', 'error', 'debug']
    let olds = ks.map((k) => console[k])
    ks.forEach((k) => {
        console[k] = (...a) => calls.push({ method: k, args: a.map((v) => ({ type: v instanceof Error ? 'error' : typeof v, text: String(v).split(url).join('<URL>') })) })
    })
    try {
        return { r: await fn(), calls }
    }
    finally {
        ks.forEach((k, i) => {
            console[k] = olds[i]
        })
    }
}

//norm, JSON往返(與測試端比對一致)
let norm = (v) => JSON.parse(JSON.stringify(v))

let plain = await cap(() => scenario(WOrm, { ...opt, cl: 'g1' }, false))
let bad = await cap(() => scenario(WOrm, { ...opt, cl: 'g2' }, true))

let golden = {
    head: 'dfa8bb7',
    plain: { out: norm(plain.r.out), evs: norm(plain.r.evs), tl: norm(plain.r.tl), calls: plain.calls },
    bad: { out: norm(bad.r.out), evs: norm(bad.r.evs), tl: norm(bad.r.tl), calls: bad.calls },
}
let fp = path.join(__dirname, 'diagGolden.json')
fs.writeFileSync(fp, JSON.stringify(golden, null, 2))
fs.rmSync(dir, { recursive: true, force: true })
console.log('written', fp, 'plain calls', plain.calls.length, 'bad calls', bad.calls.length)
