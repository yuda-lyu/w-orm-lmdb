//子程序: 建立大資料表後呼叫備份並立即通知主程序, 供主程序於備份進行中強制終止本程序(模擬崩潰、斷電), 驗證目標內不留下殘缺之data.mdb
//以cwd之./src/WOrmLmdb.mjs載入, 啟動參數為資料表之url與備份目標資料夾; 須以fork啟動
import path from 'path'
import { pathToFileURL } from 'url'

let url = process.argv[2]
let fpTar = process.argv[3]
let WOrm = (await import(pathToFileURL(path.resolve('./src/WOrmLmdb.mjs')).href)).default

let wo = WOrm({ url, db: 'worm', cl: 'big' })

//資料量須足以令備份於主程序收到通知並終止本程序前尚未結束
let docs = []
for (let i = 0; i < 40000; i++) {
    docs.push({ id: `k${i}`, name: 'y'.repeat(2000), i })
}
await wo.insertBulk(docs)

let pm = wo.backup(fpTar)
process.send({ calling: true })
await pm
process.send({ done: true })
