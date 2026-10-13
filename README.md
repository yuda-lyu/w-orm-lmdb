# w-orm-lmdb
An operator for lmdb in nodejs.

![language](https://img.shields.io/badge/language-JavaScript-orange.svg) 
[![npm version](http://img.shields.io/npm/v/w-orm-lmdb.svg?style=flat)](https://npmjs.org/package/w-orm-lmdb) 
[![license](https://img.shields.io/npm/l/w-orm-lmdb.svg?style=flat)](https://npmjs.org/package/w-orm-lmdb) 
[![npm download](https://img.shields.io/npm/dt/w-orm-lmdb.svg)](https://npmjs.org/package/w-orm-lmdb) 
[![npm download](https://img.shields.io/npm/dm/w-orm-lmdb.svg)](https://npmjs.org/package/w-orm-lmdb) 
[![jsdelivr download](https://img.shields.io/jsdelivr/npm/hm/w-orm-lmdb.svg)](https://www.jsdelivr.com/package/npm/w-orm-lmdb)

## Keypoint

注意: 因lmdb-js綁定層限制, 跨程序同時寫入無法保證原子性, 寫入須由單一程序進行, 或自行以跨程序鎖序列化. 讀取則可見呼叫前已提交之資料, 含其他程序與同一程序其他實例之提交(select開啟useCache時除外).

注意: 主鍵id經LMDB鍵編碼後不可超過1978位元組(UTF-8, 首字元碼小於28者另計1位元組), 超過者insert與insertBulk整批reject而不寫入任何一筆, save該筆ok為0, selectByPk與del視為未命中. insert、insertBulk與save之數據元素須為物件, 否則整批reject.

### Use a single process for writing

`w-orm-lmdb` guarantees write atomicity **within a single process only**. Do not have two or more processes writing to the same collection concurrently.

Within one process, `insert` and `save` are safe under any amount of concurrency:

- `insert` uses LMDB's conditional write (`ifNoExists`), so the "check the key is absent" and "write" steps happen inside one write transaction. Concurrent `insert` calls on the same id produce exactly one `nInserted: 1`; the rest report `nInserted: 0`.
- `save` wraps its read-merge-write inside an LMDB write transaction, so concurrent `save` calls on the same id never lose an update.

Across processes these guarantees do not hold, and the limitation comes from the underlying `lmdb-js` binding rather than from this package or from LMDB itself. LMDB's own multi-process design is sound — one writer at a time, serialized through a lock file — and `lmdb-js` documents its conditional writes as resolving `true` only "if the put was successful" and `false` "if the put did not occur due to the ifVersion not matching at the time of the commit". In practice that contract was observed to break when two processes contend for the same key at the same instant.

Measured on Windows 11 with `lmdb-js` 3.5.6, under CPU load, using plain `lmdb-js` with no part of this package involved:

- 4 processes racing to create the same key, 30 attempts each, 40 rounds — a few percent of rounds ended with **two** processes both resolving `true`, where exactly one should have.
- 4 processes running an optimistic `ifVersion` increment loop, 20 attempts each, 40 rounds — reported successes exceeded the actual number of increments in 12 rounds, i.e. two increments collapsed into one.
- The same tests inside a **single process** never produced an anomaly, across every configuration tried.
- `ifNoExists`, `transaction` and `transactionSync` all showed it, and it was independent of the `compression` option, so switching API does not avoid it.

Notably, the failure needs an actual race. With the key already present before the processes start — 4 processes, 30 attempts each, 40 rounds, 4800 conditional writes in total — there was **not one** false success and **not one** overwrite. A record that already exists is never clobbered; only writes landing in the same instant can interfere.

What that means in practice when two processes write concurrently:

- `nInserted` and `nModified` can be over-reported. Code that treats `nInserted === 1` as "this record is new" — to trigger a notification, an AI call, or any other expensive downstream action — may fire more than once for the same record.
- When two processes create the same id at the same instant, both may report success and only one of the two payloads is kept.
- `save` may lose an update, keeping only one side of two concurrent merges.
- Key uniqueness and record count stay correct, and records that already exist are never overwritten. The database is not left structurally inconsistent.

If your deployment needs more than one process, serialize writes yourself: keep a single writer process, or guard writes with a cross-process lock (a lock file, or a queue).

### Reads see every commit made before the call

Every read — `select`, `selectByPk`, and the reads inside `save`, `del` and `delAll` — sees all data committed before the call started by any writer other than this instance: other processes, and other instances opened on the same path in the same process. Reads are safe from any number of processes, and the write serialization above relies on this: a process that takes its turn after another process has committed sees that commit.

The one exception is `select` with `useCache: true`, which returns its cache. The cache only reflects writes made through the same instance, so turn it on only when no other instance or process writes to the collection. `delAll` always reads the database and never uses the cache.

`lmdb-js` keeps one read snapshot per opened database instance and refreshes it on the next timers phase, or right after a commit made through that same instance. A read issued just after someone else commits, such as inside the callback that receives another process's "done" message, would otherwise still see the snapshot from before that commit. `w-orm-lmdb` therefore resets the read snapshot before every read outside a write transaction, which is the `resetReadTxn()` call that `lmdb-js` documents for multi-process use.

Measured on Windows 11 with `lmdb-js` 3.5.6, 100 runs per case: without the reset, 14 of 25 combinations of an operation and an earlier write by a second process returned results based on the old snapshot in 77–96% of runs. `select` and `selectByPk` missed the new data, `save` skipped a write because it compared against the old value, `del` reported a miss, and `delAll` deleted a row that the other process had just changed so that it no longer matched the filter. Two instances in one process failed the same way; `save` lost a later update in 64–97% of runs. With the reset, every combination was correct in every run. The reset costs about 0.4 µs per `selectByPk`.

### Rows that cannot be written are rejected before anything is written

A primary key (`id`) may take at most 1978 bytes once encoded as an LMDB key: its UTF-8 bytes, plus one byte when the first character code is below 28. `insert` and `insertBulk` check every row before writing anything. If any row has a longer key, a value that cannot be serialized (such as a `Symbol`, a `BigInt` beyond 64 bits, or a circular reference), or is not an object (such as an array, `Date`, `Map` or `Set`), the whole call rejects and nothing is written. The message names the row, for example `can not insert by data[1], Key size is larger than the maximum key size (1978)`.

`save` handles a row whose key is too long, or whose value cannot be serialized, as a failure of that row only: that row gets `ok: 0` with `err`, and the other rows are saved. `selectByPk`, `del` and the lookups inside `save` treat a key that is too long as not found, because such a key cannot exist.

## Documentation
To view documentation or get support, visit [docs](https://yuda-lyu.github.io/w-orm-lmdb/WOrm.html).

## Installation

### Using npm(ES6 module):
```alias
npm i w-orm-lmdb
```

#### Example for collection
> **Link:** [[dev source code](https://github.com/yuda-lyu/w-orm-lmdb/blob/master/g-basic.mjs)]
```alias
import _ from 'lodash-es'
// import w from 'wsemi'
import WOrm from './src/WOrmLmdb.mjs'
//import WOrm from './dist/w-orm-lmdb.umd.js'

// w.fsDeleteFolder('./_db')

let opt = {
    url: './_db',
    db: 'worm',
    cl: 'users',
}

let rs = [
    {
        id: 'id-peter',
        name: 'peter',
        value: 123,
    },
    {
        id: 'id-rosemary',
        name: 'rosemary',
        value: 123.456,
    },
    {
        id: '',
        name: 'kettle',
        value: 456,
    },
]

let rsm = [
    {
        id: 'id-peter',
        name: 'peter(modify)'
    },
    {
        id: 'id-rosemary',
        name: 'rosemary(modify)'
    },
    {
        id: '',
        name: 'kettle(modify)'
    },
]

let rsa = [
    {
        id: 'id-rosemary',
        name: 'rosemary',
        value: 654.321,
    },
]

async function test() {

    //wo
    let wo = WOrm(opt)

    //on
    wo.on('change', function(mode, data, res) {
        console.log('change', mode)
    })
    wo.on('error', function(mode, data, err) {
        console.log('error', mode, err)
    })

    //delAll
    await wo.delAll()
        .then(function(msg) {
            console.log('delAll then', msg)
        })
        .catch(function(msg) {
            console.log('delAll catch', msg)
        })

    //insert
    await wo.insert(rs)
        .then(function(msg) {
            console.log('insert then', msg)
        })
        .catch(function(msg) {
            console.log('insert catch', msg)
        })

    //save
    await wo.save(rsm, { autoInsert: false })
        .then(function(msg) {
            console.log('save then', msg)
        })
        .catch(function(msg) {
            console.log('save catch', msg)
        })

    //select all
    let ss = await wo.select()
    ss = _.sortBy(ss, 'name')
    console.log('select all', ss)

    //select
    let so = await wo.select({ id: 'id-rosemary' })
    console.log('select', so)

    //selectByPk
    let sb = await wo.selectByPk('id-rosemary')
    console.log('selectByPk', sb)

    //selectByPk by pk not existed
    let sbn = await wo.selectByPk('id-not-existed')
    console.log('selectByPk by pk not existed', sbn)

    //select by $and, $gt, $lt
    let spa = await wo.select({ '$and': [{ value: { '$gt': 123 } }, { value: { '$lt': 200 } }] })
    console.log('select by $and, $gt, $lt', spa)

    //select by $or, $gte, $lte
    let spb = await wo.select({ '$or': [{ value: { '$lte': -1 } }, { value: { '$gte': 200 } }] })
    console.log('select by $or, $gte, $lte', spb)

    //select by $or, $and, $ne, $in, $nin
    let spc = await wo.select({ '$or': [{ '$and': [{ value: { '$ne': 123 } }, { value: { '$in': [123, 321, 123.456, 456] } }, { value: { '$nin': [456, 654] } }] }, { '$or': [{ value: { '$lte': -1 } }, { value: { '$gte': 400 } }] }] })
    spc = _.sortBy(spc, 'name')
    console.log('select by $or, $and, $ne, $in, $nin', spc)

    // //select by regex //mingo不支援regex
    // let sr = await wo.select({ name: { $regex: 'PeT', $options: '$i' } })
    // console.log('selectReg', sr)

    //save
    await wo.save(rsa, { autoInsert: true })
        .then(function(msg) {
            console.log('save then', msg)
        })
        .catch(function(msg) {
            console.log('save catch', msg)
        })

    //del
    let d = ss.filter(function(v) {
        return v.name === 'kettle'
    })
    await wo.del(d)
        .then(function(msg) {
            console.log('del then', msg)
        })
        .catch(function(msg) {
            console.log('del catch', msg)
        })

    //del by data without id, 該筆無法處理故ok為0並附err, 整批仍resolve且另發出error事件
    await wo.del({ name: 'no-id' })
        .then(function(msg) {
            console.log('del by data without id then', msg)
        })
        .catch(function(msg) {
            console.log('del by data without id catch', msg)
        })

    //insertBulk, 全批視為一個單位, 無衝突時nInserted恆等於n
    await wo.insertBulk([{ id: 'id-bulk1', name: 'bulk1' }, { id: 'id-bulk2', name: 'bulk2' }])
        .then(function(msg) {
            console.log('insertBulk then', msg)
        })
        .catch(function(msg) {
            console.log('insertBulk catch', msg)
        })

    //insertBulk by data with existed id, 非insert之加速版而係衝突政策不同
    //insert於主鍵已存在時跳過該筆而整批ok為1, insertBulk則整批reject且不寫入任何一筆
    await wo.insertBulk([{ id: 'id-bulk3', name: 'bulk3' }, { id: 'id-peter', name: 'conflict' }])
        .then(function(msg) {
            console.log('insertBulk by data with existed id then', msg)
        })
        .catch(function(msg) {
            console.log('insertBulk by data with existed id catch', msg.toString())
        })

    //select all, 可見id-bulk3因整批reject而未寫入
    let sb2 = await wo.select()
    console.log('ids after insertBulk', _.map(_.sortBy(sb2, 'id'), 'id'))

    //insert by returnList, 回傳與輸入等長保序之逐筆結果, nInserted為1即該筆為新增
    //聚合計數只回答有幾筆是新的, 逐筆結果方能回答是哪幾筆, 供下游僅對新資料執行昂貴動作
    let rl = await wo.insert([{ id: 'id-peter', name: 'dup' }, { id: 'id-new1', name: 'new1' }], { returnList: true })
    console.log('insert by returnList', rl)

    //filter, 以逐筆結果對位取出新增之數據
    let fresh = [{ id: 'id-peter', name: 'dup' }, { id: 'id-new1', name: 'new1' }].filter(function(v, i) {
        return rl[i].nInserted === 1
    })
    console.log('fresh by returnList', _.map(fresh, 'id'))

}
test()
// change delAll
// delAll then { n: 5, nDeleted: 5, ok: 1 }
// change insert
// insert then { n: 3, nInserted: 3, ok: 1 }
// change save
// save then [
//   { n: 1, nInserted: 0, nModified: 1, ok: 1 },
//   { n: 1, nInserted: 0, nModified: 1, ok: 1 },
//   { n: 0, nInserted: 0, nModified: 0, ok: 1 }
// ]
// select all [
//   {
//     id: {random id},
//     name: 'kettle',
//     value: 456
//   },
//   { id: 'id-peter', name: 'peter(modify)', value: 123 },
//   { id: 'id-rosemary', name: 'rosemary(modify)', value: 123.456 }
// ]
// select [ { id: 'id-rosemary', name: 'rosemary(modify)', value: 123.456 } ]
// selectByPk { id: 'id-rosemary', name: 'rosemary(modify)', value: 123.456 }
// selectByPk by pk not existed null
// select by $and, $gt, $lt [ { id: 'id-rosemary', name: 'rosemary(modify)', value: 123.456 } ]
// select by $or, $gte, $lte [
//   {
//     id: {random id},
//     name: 'kettle',
//     value: 456
//   }
// ]
// select by $or, $and, $ne, $in, $nin [
//   {
//     id: {random id},
//     name: 'kettle',
//     value: 456
//   },
//   {
//     id: 'id-rosemary',
//     name: 'rosemary(modify)',
//     value: 123.456
//   }
// ]
// change save
// save then [ { n: 1, nInserted: 0, nModified: 1, ok: 1 } ]
// change del
// del then [ { n: 1, nDeleted: 1, ok: 1 } ]
// error del can not delete by invalid id[]
// change del
// del by data without id then [ { n: 0, nDeleted: 0, ok: 0, err: 'can not delete by invalid id[]' } ]
// change insertBulk
// insertBulk then { n: 2, nInserted: 2, ok: 1 }
// error insertBulk can not insertBulk by existed id[id-peter]
// insertBulk by data with existed id catch Error: can not insertBulk by existed id[id-peter]
// ids after insertBulk [ 'id-bulk1', 'id-bulk2', 'id-peter', 'id-rosemary' ]
// change insert
// insert by returnList [ { n: 1, nInserted: 0, ok: 1 }, { n: 1, nInserted: 1, ok: 1 } ]
// fresh by returnList [ 'id-new1' ]
```
