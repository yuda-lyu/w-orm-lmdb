//子程序: 以唯讀開啟指定之lmdb資料夾並印出筆數(JSON一行), 供主程序檢查[可能殘缺之備份檔]是否完整可開啟而不使主程序暴露於原生層崩潰
//(殘缺之data.mdb於lmdb開啟或走訪時可能引發存取違規, 故不在測試程序內直接開啟); 啟動參數為資料夾路徑
import { open } from 'lmdb'

let db = open({ path: process.argv[2], compression: true, useVersions: true, readOnly: true })
let n = 0
for (let kv of db.getRange()) {
    //值須可解碼(殘缺之檔於此拋錯或崩潰)
    if (kv.value === undefined) {
        throw new Error('undecodable value')
    }
    n++
}
console.log(JSON.stringify({ n }))
await db.close()
