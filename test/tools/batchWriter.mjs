//子程序: 以裸lmdb(開啟參數同src/WOrmLmdb.mjs)依主程序之IPC指令持續寫入, 供備份於[另一程序寫入中]之一致性測試使用
//每個寫交易寫入一批(nPer筆, 同批之n相同), 批號n由0起連續遞增, 故任一提交點之資料必為[批0至批m-1各完整]之前綴; 撕裂之備份將呈現缺筆或不連續之批
//啟動參數為資料表之lmdb路徑, 須以fork啟動; 指令: start(開始寫入)、stop(停止並關閉, 回傳done與已提交批數)
import { open } from 'lmdb'

let db = open({
    path: process.argv[2],
    compression: true,
    useVersions: true,
})

let nPer = 40
let pad = 'x'.repeat(2000)
let looping = false
let nBatch = 0
let pmLoop = null

//loop, looping由訊息處理函數於迴圈外改寫(迴圈內之await令其得以執行), 故於迴圈內判斷後跳出
let loop = async() => {
    for (;;) {
        if (!looping) {
            break
        }
        let n = nBatch
        await db.transaction(() => {
            for (let i = 0; i < nPer; i++) {
                let id = `b${n}-${i}`
                db.put(id, { id, n, i, pad })
            }
        })
        nBatch++
        if (nBatch % 5 === 0) {
            process.send({ progress: nBatch })
        }
        await new Promise((resolve) => setImmediate(resolve))
    }
}

process.on('message', async(m) => {
    if (m.cmd === 'fill') {
        //填充資料: 使備份有一定之複製量; 因開啟時 compression: true 而相同內容之 pad 可壓縮, 填充4萬列後資料庫檔實測僅約6MB(非未壓縮之約80MB), 備份窗口不長, 故不依賴窗口長短, 斷言之下界由事件閘門決定; 填充列id為f<序號>, 每個交易1000筆
        for (let i = 0; i < m.n; i += 1000) {
            await db.transaction(() => {
                for (let j = i; j < Math.min(i + 1000, m.n); j++) {
                    db.put(`f${j}`, { id: `f${j}`, j, pad })
                }
            })
        }
        process.send({ filled: true })
    }
    else if (m.cmd === 'start') {
        looping = true
        pmLoop = loop()
        process.send({ started: true })
    }
    else if (m.cmd === 'stop') {
        looping = false
        await pmLoop
        await db.close()
        process.send({ done: true, nBatch })
        process.exit(0)
    }
})

process.send({ ready: true })
