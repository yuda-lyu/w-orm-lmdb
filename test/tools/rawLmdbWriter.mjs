//子程序: 以裸lmdb(開啟參數同src/WOrmLmdb.mjs)依主程序之IPC指令寫入, 供跨程序可見性與併發之測試使用
//本程序無任何讀取路徑, 故自身不受讀取快照影響, 主程序所見之異常皆歸因於主程序之讀取
//啟動參數為資料表之lmdb路徑, 須以fork啟動
//put與remove指令可帶marker檔案路徑, 寫入提交後建立該檔, 供主程序不讓出事件迴圈而同步等待提交
import fs from 'fs'
import { open } from 'lmdb'

let db = open({
    path: process.argv[2],
    compression: true,
    useVersions: true,
})

//looping, 持續寫入並取樣指定程序所占之reader slot
let looping = false
let pmLoop = null
let nCommit = 0
let maxSlots = 0
let pidWatch = ''

//sampleSlots, readerList每列首欄為程序pid, 計受測程序之列數
let sampleSlots = () => {
    let n = 0
    for (let ln of db.readerList().split('\n')) {
        if (ln.trim().split(/\s+/)[0] === pidWatch) {
            n++
        }
    }
    if (n > maxSlots) {
        maxSlots = n
    }
}

//loop, looping由訊息處理函數於迴圈外改寫(迴圈內之await令其得以執行), 故於迴圈內判斷後跳出
let loop = async () => {
    for (;;) {
        if (!looping) {
            break
        }
        let id = `w${nCommit % 500}`
        await db.put(id, { id, n: nCommit })
        nCommit++
        sampleSlots()
    }
}

process.on('message', async (m) => {
    if (m.cmd === 'put' || m.cmd === 'remove') {
        if (m.cmd === 'put') {
            await db.put(m.id, m.value)
        }
        else {
            await db.remove(m.id)
        }
        if (m.marker) {
            fs.writeFileSync(m.marker, '')
        }
        process.send({ done: m.cmd })
    }
    else if (m.cmd === 'loopStart') {
        pidWatch = String(m.pid)
        looping = true
        pmLoop = loop()
        process.send({ done: m.cmd })
    }
    else if (m.cmd === 'mark') {
        process.send({ done: m.cmd, nCommit })
    }
    else if (m.cmd === 'loopStop') {
        looping = false
        await pmLoop
        process.send({ done: m.cmd, nCommit, maxSlots })
    }
    else if (m.cmd === 'exit') {
        await db.close()
        process.disconnect()
    }
})

process.send({ ready: true })
