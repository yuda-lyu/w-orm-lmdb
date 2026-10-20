//子程序: 備份進行中立即呼叫close()之行為(未等候備份即close, 於lmdb-js直接為之會使行程段錯誤, 離開碼139)
//以cwd之./src/WOrmLmdb.mjs載入, 啟動參數為資料表之url與備份目標資料夾; 依序輸出一行JSON, 最後一行為{ end: true }, 以離開碼0與該行判定未崩潰
import path from 'path'
import { pathToFileURL } from 'url'

let url = process.argv[2]
let fpTar = process.argv[3]
let WOrm = (await import(pathToFileURL(path.resolve('./src/WOrmLmdb.mjs')).href)).default

let out = (o) => {
    console.log(JSON.stringify(o))
}

let wo = WOrm({ url, db: 'worm', cl: 'big' })

//資料量須足以令備份尚未結束時close()即已呼叫
let docs = []
for (let i = 0; i < 20000; i++) {
    docs.push({ id: `k${i}`, name: 'y'.repeat(2000), i })
}
await wo.insertBulk(docs)

let pmBackup = wo.backup(fpTar).then((r) => ({ ok: r.ok, size: r.size }), (err) => ({ err: err.message }))
let pmClose = wo.close()

//close()呼叫後備份尚未結束之期間, 實例已視為已關閉而拒絕新操作
let rej = await wo.select().then(() => 'resolved', (err) => err.message)
out({ selectDuringClose: rej })

out({ backup: await pmBackup })
await pmClose
out({ closed: true })

//關閉後再操作仍reject
out({ selectAfterClose: await wo.select().then(() => 'resolved', (err) => err.message) })
out({ end: true })
