# w-orm-lmdb
An operator for lmdb in nodejs.

![language](https://img.shields.io/badge/language-JavaScript-orange.svg) 
[![npm version](http://img.shields.io/npm/v/w-orm-lmdb.svg?style=flat)](https://npmjs.org/package/w-orm-lmdb) 
[![license](https://img.shields.io/npm/l/w-orm-lmdb.svg?style=flat)](https://npmjs.org/package/w-orm-lmdb) 
[![npm download](https://img.shields.io/npm/dt/w-orm-lmdb.svg)](https://npmjs.org/package/w-orm-lmdb) 
[![npm download](https://img.shields.io/npm/dm/w-orm-lmdb.svg)](https://npmjs.org/package/w-orm-lmdb) 
[![jsdelivr download](https://img.shields.io/jsdelivr/npm/hm/w-orm-lmdb.svg)](https://www.jsdelivr.com/package/npm/w-orm-lmdb)

## Keypoint

注意: 因lmdb-js綁定層限制, 須使用單程序操作lmdb, 才能避免競爭條件失效.

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

If your deployment needs more than one process, serialize writes yourself: keep a single writer process, or guard writes with a cross-process lock (a lock file, or a queue). Readers are unaffected — `select` and `selectById` are safe from any number of processes.

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

    //selectById
    let sb = await wo.selectById('id-rosemary')
    console.log('selectById', sb)

    //selectById by id not existed
    let sbn = await wo.selectById('id-not-existed')
    console.log('selectById by id not existed', sbn)

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

}
test()
// change delAll
// delAll then { n: 2, nDeleted: 2, ok: 1 }
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
// selectById { id: 'id-rosemary', name: 'rosemary(modify)', value: 123.456 }
// selectById by id not existed null
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
```
