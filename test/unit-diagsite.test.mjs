import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { spawnSync } from 'child_process'
import { fileURLToPath } from 'url'


//rulebook R-DIAG-01-9: 診斷輸出與對外錯誤值之站點盤點(原始碼掃描)與輸出串流(stdout/stderr)
//掃描對象為src/WOrmLmdb.mjs(去除註解後), 目的為鎖住站點全集: 任何新增之console出口、未經diag.err/outMsg之catch站點、改用錯串流之level, 皆使本檔失敗;
//串流檢查以子程序實跑, 驗證診斷輸出寫至stdout(level為log)而非stderr


let __dirname = path.dirname(fileURLToPath(import.meta.url))
let dirSrc = path.join(__dirname, '..', 'src')
let dirBase = path.join(__dirname, '_tmp', 'diagsite')

//codeOf, 去除註解後的程式碼行(整行註解與行尾註解); 行尾註解須以空白起頭之//(不誤傷://)
let codeOf = (fp) => {
    let r = []
    for (let line of fs.readFileSync(fp, 'utf8').split(/\r?\n/)) {
        let t = line.trim()
        if (t.startsWith('//') || t.startsWith('*') || t.startsWith('/*')) {
            continue
        }
        r.push(line.replace(/\s\/\/.*$/, ''))
    }
    return r
}

let lines = codeOf(path.join(dirSrc, 'WOrmLmdb.mjs'))
let text = lines.join('\n')
let count = (re) => (text.match(re) || []).length


describe('diagsite', function() {

    before(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
        fs.mkdirSync(dirBase, { recursive: true })
    })

    after(function() {
        fs.rmSync(dirBase, { recursive: true, force: true })
    })

    describe('站點盤點(原始碼)', function() {

        it('R-DIAG-01-9 src內無自寫之診斷或遮罩模組(getDiag.mjs已刪), 診斷與遮罩一律用wsemi之共用函數', function() {
            let files = fs.readdirSync(dirSrc).sort()
            assert.deepStrictEqual(files, ['WOrmLmdb.mjs', 'reqMingo.js'])
            assert.ok(text.includes('import getDiag from \'wsemi/src/getDiag.mjs\''))
            assert.ok(text.includes('import getDiagSecrets from \'wsemi/src/getDiagSecrets.mjs\''))
            assert.ok(!/getDiag\.mjs'/.test(text.replace(/wsemi\/src\/getDiag\.mjs'/g, '')), '不得匯入本地之getDiag.mjs')
            for (let name of ['maskOpenErr', 'maskPathText', 'getSecrets', 'getBriefErr', 'getOptLog', 'cpMask', 'maskText']) {
                assert.ok(!text.includes(name), `不得殘留自寫函數「${name}」`)
            }
        })

        it('R-DIAG-01-9 宣告: paths為儲存位置、patterns為fsPathTail與receivedTail、codes為lmdb、safeKeys為mode; 不宣告帳號密碼權杖與urls', function() {
            let m = text.match(/getDiag\(opt, \{([\s\S]*?)\n {4}\}\)/)
            assert.ok(m !== null, '應有getDiag(opt, {...})宣告')
            let decl = m[1].replace(/\s+/g, ' ').trim()
            assert.strictEqual(decl, 'secrets: getDiagSecrets({ paths }), patterns: [\'fsPathTail\', \'receivedTail\'], codes: [\'lmdb\'], safeKeys: [\'mode\'],')
            assert.ok(/let paths = \[url, `\$\{url\}\/\$\{db\}`, storage\]\.flatMap\(/.test(text), '儲存位置三層')
            assert.strictEqual(count(/getDiag\(/g), 1, 'getDiag只建立一個實例')
        })

        it('R-DIAG-01-9 src內無console出口(全部診斷輸出皆經diag), lmdb自身之輸出不在此範圍', function() {
            assert.strictEqual(count(/\bconsole\./g), 0)
            assert.strictEqual(count(/\bprocess\.(stdout|stderr)\b/g), 0)
        })

        it('R-DIAG-01-9 diag出口全集: 2個站點, label為固定字面值, level皆為log(stdout), safe為{ mode, ...diag.brief(err) }', function() {
            let calls = lines.filter((l) => /\bdiag\(/.test(l) && !/let diag\b/.test(l))
            assert.strictEqual(calls.length, 2)
            let re = /diag\('(change|error) handler threw', \{ mode, \.\.\.diag\.brief\((err|errEmit)\) \}, \(\) => (err|errEmit), 'log'\)/
            for (let l of calls) {
                let m = l.match(re)
                assert.ok(m !== null, `不合規格之diag呼叫: ${l.trim()}`)
                assert.strictEqual(m[2], m[3], '摘要與細節取自同一個拋出值')
            }
            assert.deepStrictEqual(calls.map((l) => l.match(re)[1]).sort(), ['change', 'error'])
        })

        it('R-DIAG-01-9 外來值之catch站點全集: 7個操作站點經outErr、2個逐筆經outMsg、編碼失敗與backup之訊息經outMsg、建構時open失敗經diag.err; 無未經遮罩之res = err', function() {
            assert.strictEqual(count(/res = outErr\(err\)/g), 7, 'select、selectByPk、insert、insertBulk、save、del、delAll')
            assert.strictEqual(count(/\berr: outMsg\(err\)/g), 2, 'save與del逐筆err')
            assert.strictEqual(count(/\$\{outMsg\(err\)\}/g), 2, '編碼失敗之訊息後綴與backup之訊息')
            assert.strictEqual(count(/throw diag\.err\(err\)/g), 1, '建構時open失敗')
            assert.strictEqual(count(/\bres = err\b/g), 0)
            assert.strictEqual(count(/\berr: getErrMsg\(err\)/g), 0)
            assert.strictEqual(count(/ownErrs\.has\(err\)/g), 3, 'outErr、outMsg與backup各一')
        })

        it('R-DIAG-01-9 本套件自身之錯誤全集: new Error僅4處(建構時良構檢核、mkErr、已關閉之立即reject、backup之reject包裝), 其餘一律mkErr登記', function() {
            let ls = lines.filter((l) => /new Error\(/.test(l)).map((l) => l.trim())
            assert.deepStrictEqual(ls, [
                'throw new Error(`invalid opt.${name}: ${r}`)', // eslint-disable-line no-template-curly-in-string
                'let err = new Error(msg)',
                'return Promise.reject(new Error(msg))',
                'return Promise.reject(new Error(msg))',
            ])
            assert.strictEqual(count(/\bthrow (?!mkErr|bad|diag\.err|err\b|new Error\()/g), 0, 'throw之對象只可為mkErr、bad、diag.err、err(重拋)')
        })

        it('R-DIAG-01-9 reject出口全集: Promise.reject之對象只可為檢核錯誤err(cvOrmFind, 2處)、已遮罩之res、或固定包裝new Error(msg)', function() {
            let args = lines.filter((l) => /Promise\.reject\(/.test(l)).map((l) => l.match(/Promise\.reject\((.*)\)\s*$/)[1])
            let n = (a) => args.filter((v) => v === a).length
            assert.strictEqual(args.length, n('err') + n('res') + n('new Error(msg)'))
            assert.strictEqual(n('err'), 2, 'select與delAll之條件檢核(本套件自身之固定詞彙錯誤)')
            assert.strictEqual(n('new Error(msg)'), 2, '已關閉(procClosed)與backup之包裝')
            assert.strictEqual(n('res'), 7, 'select、selectByPk、insert、insertBulk、save、del、delAll')
        })

        it('R-DIAG-01-9 \'error\'事件出口全集: 只有emitError一處, 其訊息引數取自getErrMsg(已遮罩之值或本套件自身之固定訊息)', function() {
            assert.strictEqual(count(/\.emit\('error'/g), 1)
            assert.strictEqual(count(/ee\.emit\('error', mode, data, getErrMsg\(err\)\)/g), 1)
        })

    })

    describe('輸出串流(子程序實跑)', function() {

        let script = path.join(__dirname, 'tools', 'diagStream.mjs')

        let run = (name, extra = []) => {
            let r = spawnSync(process.execPath, [script, path.join(dirBase, name), ...extra], { encoding: 'utf8', windowsHide: true, timeout: 120000 })
            assert.strictEqual(r.status, 0, `子程序應正常結束: ${r.stderr}`)
            return r
        }

        it('R-DIAG-01-9 預設: change與error站點之診斷輸出皆寫至stdout(console.log), stderr無輸出', function() {
            let r = run('default')
            assert.ok(r.stdout.includes('change handler threw'), r.stdout)
            assert.ok(r.stdout.includes('error handler threw'), r.stdout)
            assert.strictEqual(r.stderr, '')
            assert.ok(r.stdout.indexOf('listener boom') < 0, '預設不輸出錯誤原文')
        })

        it('R-DIAG-01-9 logDetail: 診斷輸出(含細節)同樣寫至stdout, stderr無輸出', function() {
            let r = run('detail', ['logDetail'])
            assert.ok(r.stdout.includes('change handler threw') && r.stdout.includes('error handler threw'), r.stdout)
            assert.ok(r.stdout.includes('listener boom (change)') && r.stdout.includes('listener boom (error)'), r.stdout)
            assert.strictEqual(r.stderr, '')
        })

    })

})
