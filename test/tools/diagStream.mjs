//診斷輸出之輸出串流檢查用子程序(unit-diagsite使用): 令change與error兩個訂閱函數各拋錯一次, 其診斷輸出由本程序之console寫至stdout或stderr, 由父程序分辨
//用法: node diagStream.mjs <資料夾> [logDetail]
import WOrm from '../../src/WOrmLmdb.mjs'


let dir = process.argv[2]
let logDetail = process.argv[3] === 'logDetail'

let w = WOrm({ url: dir, db: 'worm', cl: 'stream', logDetail })
w.on('change', () => {
    throw new Error('listener boom (change)')
})
w.on('error', () => {
    throw new Error('listener boom (error)')
})
await w.insert({ id: 's1' })
await w.insertBulk({ id: 's1' }).catch(() => {})
await w.close()
