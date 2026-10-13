//子程序: 寫入遭整批性錯誤reject後立即close, 行程須正常結束而不得因未捕捉例外崩潰(帳本R-WRITE-02)
//以path.resolve('./src/WOrmLmdb.mjs')載入受測模組(以cwd為準), 啟動參數為情境名稱與資料庫資料夾
//正常結束時於stdout輸出end, 崩潰時離開碼非0且無end
import path from 'path'
import { pathToFileURL } from 'url'
import { open } from 'lmdb'

let sc = process.argv[2]
let url = process.argv[3]
let WOrm = (await import(pathToFileURL(path.resolve('./src/WOrmLmdb.mjs')).href)).default
let bad = { id: 'k'.repeat(2000) }
let opt = { url, db: 'worm', cl: 't' }
let swallow = () => {}

if (sc === 'insertBadFirst') {
    //壞鍵居首(該事件迴圈之第一筆寫入), reject後立即close
    let wo = WOrm(opt)
    await wo.insert([bad, { id: 'b' }]).catch(swallow)
    await wo.close()
}
else if (sc === 'insertBadFirstAfterWrites') {
    //曾寫入且寫入已完成之實例
    let wo = WOrm(opt)
    await wo.insert({ id: 'x' })
    await new Promise((resolve) => setTimeout(resolve, 20))
    await wo.insert([bad, { id: 'b' }]).catch(swallow)
    await wo.close()
}
else if (sc === 'insertBadFirstNoAwait') {
    //不await insert即close
    let wo = WOrm(opt)
    let pm = wo.insert([bad]).catch(swallow)
    await wo.close()
    await pm
}
else if (sc === 'insertBadFirstOtherOpen') {
    //同路徑另一實例保持開啟
    let woKeep = WOrm(opt)
    await woKeep.insert({ id: 'keep' })
    let wo = WOrm(opt)
    await wo.insert([bad, { id: 'b' }]).catch(swallow)
    await wo.close()
    await woKeep.close()
}
else if (sc === 'delAllIdless') {
    //值不帶id之紀錄(他方以裸lmdb寫入), delAll後立即close
    let raw = open({ path: path.resolve(`${url}/worm/t`), compression: true, useVersions: true })
    await raw.put('m', [1, 2])
    await raw.close()
    let wo = WOrm(opt)
    await wo.delAll().catch(swallow)
    await wo.close()
}

//讓出數個check與timers階段, 令已排程之批次有機會執行
await new Promise((resolve) => setTimeout(resolve, 100))
process.stdout.write('end\n')
