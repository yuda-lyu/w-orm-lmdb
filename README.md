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

### Consistent backup: `backup(fpTar, { compact })` (一致性備份)

Copying or compressing `data.mdb` while the table is being written can read pages in the middle of a write and give an inconsistent backup. `backup` uses the LMDB environment copy (`mdb_env_copy`, `lmdb-js` `backup(path, compact)`), which runs inside a read transaction: the result is a complete snapshot of one commit point, whether the writes come from this process or from another one, and writers are not blocked. It is an extension of `w-orm-lmdb`; the other `w-orm-*` backends have no counterpart (so it is an instance method, not part of the shared series functions; a separate exported function would also change the export shape of the UMD build).

```alias
let wo = WOrmLmdb({ url: './_db', db: 'worm', cl: 'users' })
let r = await wo.backup('./backup/users', { compact: true })
// => { ok: 1, fpData: '<absolute path>/backup/users/data.mdb', size: 61440, compact: true }
```

- **Target** `fpTar` is a folder. A missing folder (and its parents) is created. An existing folder must be empty, otherwise the call rejects (`can not backup by target folder which is not empty`) and nothing is written: it never overwrites or merges. A target that is a file, or the table's own folder or a folder inside it, is rejected too (compared on real paths, so a junction or symbolic link to it does not get around the check). When it succeeds the target holds exactly one file, `data.mdb` (no `lock.mdb`).
- **Complete or absent**: the copy is written into a temporary sub-folder of the target and `data.mdb` is moved into place only after the copy has finished, after checking once more that the target still holds nothing but that temporary sub-folder (something created in it by someone else meanwhile makes the call reject and is left untouched), so a `data.mdb` that appears in the target is always complete, even if the process is killed during the backup (only a leftover `.backup-*` sub-folder can remain, and the next backup into the same folder is refused until you remove it). On an error the temporary sub-folder is removed, and the folders this call created are removed only while they are empty (content that someone else put into them in the meantime is never deleted); an existing empty target is left as it was.
- **Snapshot**: it contains every write that had been awaited before the call; a write still in flight when `backup` is called may or may not be included. While the copy runs the environment keeps a read transaction, so pages overwritten in the meantime cannot be reused and the database file may grow temporarily.
- **`compact`** (default `true`): compacts while copying (slower, smaller file). Only a boolean takes effect; any other value means the default.
- **Restore**: put `data.mdb` into the table folder (`<url>/<db>/<cl>/data.mdb`) and open it with `WOrmLmdb`, or open the backup folder directly with `lmdb`.
- **Errors**: a closed instance, an invalid `fpTar`, an unsafe target or a file system error rejects (`can not backup by closed client, ...`, `invalid fpTar: <reason>`, `can not backup by ...`, `can not backup, <original message>`), after emitting one `error` event with mode `'backup'` and `data` `null` (the path is not echoed; no `change` event: nothing is modified). `fpTar` must be a non-empty well-formed string (no lone surrogate, no NUL); `<reason>` is `not a string`, `string is empty`, `string contains NUL` or `string contains lone surrogate` (see "Strings must be well-formed").
- **`close()` waits for a running backup**: closing the environment while it is being copied crashes the process (segmentation fault, measured with `lmdb-js` 3.5.6 on Windows). `close()` therefore waits for backups in progress, and from the moment it is called the instance already rejects new operations as closed. Still `await` the backup before closing when you can.
- **Backing up from another process** (for example a backup service): construct a `WOrmLmdb` with the same `url`, `db` and `cl` in that process and call `backup`; reading from any number of processes is safe. Constructing an instance for a table that does not exist creates an empty one, so only back up existing tables.

### Strings must be well-formed (字串良構)

A JavaScript string may hold a lone surrogate (half of a surrogate pair, e.g. `'a\uD800'`; `isWellFormed()` is `false`). LMDB cannot store it as it was written: the value encoder rewrites every lone surrogate in a value or in a field name into **three** U+FFFD characters, so what is read back differs from what was written, two field names that differ only in such a character collapse into one and the later value silently overwrites the earlier one (`{ 'k\uD800': 1, 'k���': 2 }` keeps only one field), and the `id` field read back no longer addresses the row (`selectByPk(row.id)` is `null`). `w-orm-lmdb` therefore refuses them up front, before anything is read or written, with the shared checks `chkOrmStr` and `chkOrmData` of `wsemi` (the same series-wide rule for every `w-orm-*` backend):

- **Primary key** (`id`): no lone surrogate and no NUL character (U+0000), the same standard as `find`. This applies whatever `autoGenPk` is.
- **Written data** (`insert`, `insertBulk`, `save`): no lone surrogate in any string value or field name, including nested objects, array elements, `Map`/`Set` entries, and the text of `Error`/`RegExp` values. A NUL character inside a value or a field name is not refused by this rule.
- **`opt.url`, `opt.db`, `opt.cl`**: a lone surrogate makes the constructor throw `invalid opt.<name>: string contains lone surrogate` (the path would be rewritten by the file system, so two different names would share one data folder).
- U+FFFD itself and valid surrogate pairs (emoji, CJK Extension B and above) are well-formed and accepted as before.

How a refusal is reported (the same shape as the existing "invalid primary key" cases; the message uses a fixed vocabulary and never echoes the value):

| Call | Result |
|---|---|
| `insert`, `insertBulk` | the whole call rejects and nothing is written; an `error` event is emitted once before the rejection |
| `save`, `del` | that row gets `ok: 0` with `err`; the other rows of the batch are processed as usual |
| `selectByPk` | `null` (no rejection, no event) |
| `select`, `delAll` | unchanged: `invalid find: <reason> at [<path>]` |

Messages: `invalid id: <reason> at [data[<k>].id]` and `invalid data: <reason> at [data[<k>].<path>]` (`<k>` is the index in the batch, `data[0]` for a single object; a refused field name is shown as `<invalid key>`), where `<reason>` is `string contains lone surrogate`, `string contains NUL` (primary key only), `not a string` or `string is empty` (`del` with a missing, non-string or empty `id`; this replaces the former `can not delete by invalid id[...]`, which echoed the value). The first refused row (lowest index) is reported, and within a row the primary key is checked before the data.

Existing data is not migrated. Rows written before this rule with a lone surrogate in the primary key still exist: the key itself was stored as written, but the `id` field reads back as three U+FFFD (well-formed, so `isWellFormed()` cannot find them). They are recognised by an `id` that no longer addresses its own row:

```alias
let orphans = []
for (let r of await wo.select()) {
    if (typeof r.id === 'string' && (await wo.selectByPk(r.id)) === null) {
        orphans.push(r)
    }
}
```

The same snippet also finds rows written earlier whose id contains a NUL character: the id reads back intact, but it can no longer be used to address the row now that a NUL in a primary key is refused. Remove a lone-surrogate row with `await wo.delAll({ id: row.id })` (it matches by value and deletes by the real key; a row whose id really is those U+FFFD characters would be matched too). A row whose id contains NUL cannot be removed that way (`find` refuses NUL too, `invalid find: string contains NUL`): remove it by another field of the row, for example `delAll({ name: row.name })`, or with `delAll()` for the whole table. A lone surrogate that was already rewritten inside a value or a field name cannot be told apart from a genuine U+FFFD afterwards. Reading (`select`) keeps working for all existing rows.

Known limitations: a `String` wrapper object (`new String('...')`) is checked as a string value but is not unwrapped; `toJSON` or other custom serialization output is not checked.

### `save` merges by default; `save(data, { merge: false })` overwrites the whole record

By default `save` deep-merges each row into the existing record with the same `id`: fields that the row does not give are kept, array fields are merged element by element, and there is no way to remove a field. To replace a record as a whole, pass `merge: false`:

```alias
await wo.save({ id: 'u1', name: 'peter' }, { merge: false })
```

- The record that ends up stored is exactly the row you passed, as encoded by LMDB (the fidelity is the same as for `insert` and the default `save`: for example a `Set` reads back as an array, and the own keys `__proto__` and `__proto_` collapse into one). Keys of the old record that the row does not have are gone, array fields are replaced as a whole, and nested objects are not merged.
- The overwrite is a single `put` inside the same LMDB write transaction that `save` already uses; there is no delete step. There is therefore no window in which the record is missing, a concurrent reader sees either the old or the new whole record, and if the write fails (for example the value cannot be serialized) the transaction is aborted and the old record is still there. Do not emulate this with `del` followed by `insert`: that is two writes, and a failure or a concurrent `insert` in between loses or replaces data.
- `merge` takes effect only when it is exactly `false`. Omitted, `true` or any other value keeps the default deep merge, because an overwrite discards data.
- `autoInsert` works the same way: when the id does not exist, `autoInsert: true` (default) inserts the row, `autoInsert: false` writes nothing and the result is `{ n: 0, nInserted: 0, nModified: 0, ok: 1 }`. `autoInsert` has no effect on a record that already exists.
- Result shape, ordering, per-row failure (`ok: 0` with `err`, without rejecting), batch-level errors and events (`insert` for an inserted row, then one `change` with mode `save`, `error` per failed row) are the same as the default `save`.
- "Unchanged" is judged against the row itself: if the row equals the existing record, nothing is written and `nModified` is `0`. A row that is a subset of the existing record is a change (it removes keys) and `nModified` is `1`.
- `merge` is an option of `w-orm-lmdb`; the other `w-orm-*` backends do not provide it.

### Condition (`find`) whitelist (條件(find)白名單)

`select` and `delAll` take the same `find` condition and check it the same way, at the entry of the call: after the closed check and before anything is read or deleted. The check is `cvOrmFind` of `wsemi`, shared by the whole `w-orm-*` series, and the query and the deletion use only the checked copy, never the object the caller passed in. A condition outside the whitelist rejects the whole call with `invalid find: <reason> at [<path>]`, an `error` event is emitted once before the rejection, nothing is read or deleted and no `change` event is emitted.

Accepted:

- No condition (omitted, `undefined` or `{}`): all rows. `delAll()` with no condition deletes every row, so use `delAll` carefully and only when needed; `select(find)` shows what `delAll(find)` will delete (unless `useCache` is on and another instance or process has written since: `select` may then return the cache, while `delAll` always reads the database).
- Field equality `{ field: scalar }`, several fields are ANDed. A scalar is a string (without NUL characters or lone surrogates), a finite number, a boolean or a valid `Date`. `null` is allowed only in field equality, `$eq` and `$ne`.
- Field operators `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` (scalar value) and `$in`, `$nin` (array of scalars; `$in: []` matches nothing, `$nin: []` is rejected). Several operators on one field are ANDed.
- Document-level `$and` and `$or`: a non-empty array of non-empty condition objects, nestable.
- Field names match `^[\p{L}_][\p{L}\p{M}\p{N}_]*$`, at most 128 characters, and are not `__proto__`; a dotted path such as `'o.k'` is not a field name.
- Limits: nesting depth 32, 1000 items in `$and`/`$or`, 10000 items in `$in`/`$nin`, 100000 nodes in total.

Rejected as a whole: a value that is not a condition object (`null`, array, string, number, boolean, function, `Date`, `RegExp`, `Map`, `Set`, class instance, ...); any other operator (`$not`, `$nor`, `$regex`, `$options`, `$exists`, `$where`, `$expr`, `$type`, `$mod`, `$elemMatch`, `$size`, `$all`, ...); field-level `$and`/`$or`; `undefined`, `NaN`, `Infinity`, invalid `Date`, `BigInt`, function, `Symbol` or `RegExp` values; array or nested-object equality; `$nin: []`, `$and: []`, `$or: []`, `{}` inside `$and`/`$or`, `{ field: {} }`; `null` in `$gt`/`$gte`/`$lt`/`$lte`/`$in`/`$nin`; symbol keys, getters, non-enumerable keys, sparse arrays.

Semantics are MongoDB-style: `{ f: null }` matches rows where `f` is `null` or missing, and `$ne: x` (x is not `null`) and `$nin` also match rows where the field is `null` or missing, while `$ne: null` excludes them. A field name that is also a member of `Object.prototype` (`constructor`, `toString`, `hasOwnProperty`, ...) is treated as missing in rows that do not have it as an own field. A scalar or `$in` condition on an array-valued field still matches by element (`{ tags: 'x' }` matches `tags: ['x', 'y']`). For the same result on every `w-orm-*` backend, write it out explicitly: exclude null/missing with `{ $and: [{ f: { $ne: null } }, { f: { $ne: x } }] }`, include them with `{ $or: [{ f: null }, { f: { $ne: x } }] }`.

### Breaking changes (破壞性變更: condition whitelist)

Conditions that used to be accepted, or silently let through, are now rejected. There is no transition period.

- Removed: every `mingo` operator other than the whitelist above (`$not`, `$nor`, `$regex`, `$options`, `$exists`, `$type`, `$mod`, `$elemMatch`, `$size`, `$all`, `$expr`, `$where`, `$bitsAllSet`, ...); `RegExp` values (`{ name: /x/i }`, `$in: [/x/]`); array-literal equality and nested-object equality (`{ tags: ['x', 'y'] }`, `{ o: { k: 1 } }`); dotted paths (`{ 'o.k': 1 }`). To filter on these, `select()` and filter in memory.
- Now rejected, previously let through: a `find` that is not a condition object (`null`, `[]`, `'x'`, `123`, `true`, a function, `Date`, `Map`, `Set`, an object with only symbol keys, an object whose condition sits on its prototype). `select(find)` used to return every row and `delAll(find)` used to delete every row. A class instance with own keys was already refused by `mingo` (`query criteria must be an object`) and now gets the `invalid find` message too. Likewise `undefined` values (`{ age: undefined }` and `$eq: undefined` used to match rows where the field is null or missing; `$in: [undefined]` matched nothing; a hole in a sparse array was treated as a missing field), `NaN`, `Infinity`, function and `Symbol` values, `$nin: []`, `$and: []`, `$or: []`, `$and: [{}]`, `$or: [{}]`, `{ age: {} }`, field names outside the grammar, getters and non-enumerable keys, nesting deeper than 32 and `$in` beyond 10000 items.
- The error message is `invalid find: <reason> at [<path>]`; a `find` that threw an error of `mingo` before now gets this message instead.

### Diagnostic output: `logDetail` and `funLog` (診斷輸出)

The only diagnostic line this package writes is the one printed when a subscriber of the `change` or `error` event throws (the error is caught and never affects the operation). By default it prints a fixed label and a summary that carries no personal data:

```alias
change handler threw { mode: 'insert', errType: 'Error' }
error handler threw { mode: 'del', errType: 'TypeError', errCode: 'ERR_X' }
```

`mode` is the operation, `errType` is the constructor name of the thrown value (`string`, `object`, `null`, ... for values that are not an `Error`), and `errCode` is the `code` of a thrown `Error` (never of another kind of object, such as a data row thrown by mistake) if it looks like a machine code: `ENOENT`, `EAI_AGAIN`, `E2BIG`, `ERR_xxx`, `MDB_xxx`, or an integer from -99999 to 99999. The message of the thrown error, ids, data values and user identifiers are not printed by default. The option names and semantics are the same as in `w-serv-orm` and `w-orm-reladb`:

- `logDetail: true` restores the previous output: one `console.log` of the value the subscriber threw (no label). The value is printed as a masked copy (see `detail` below), so its message and stack are there, but the class of an `Error` subclass is reduced to its name, and `Map`, `Set`, binary data and anything deeper than 6 levels become marker strings.
- `funLog: (label, safe, detail) => {}` replaces `console`: the diagnostic goes only to this function and nothing is written to `console`, even together with `logDetail`. `safe` is the summary above; `detail` is a masked copy of the thrown value, meant for your own redaction or forwarding (plain-object and `Error` copy, original untouched; wrapper objects such as `new String(...)` count as their primitive; functions, binary data, `Map`, `Set`, `RegExp`, `Promise` and accessors become marker strings; bounded to depth 6 and 2000 nodes). An error thrown by `funLog`, or a promise it returns that rejects, is swallowed.
- Both options are read once when the instance is created. A `logDetail` that is not `true` and a `funLog` that is not a function are ignored (default output), the same as the other options of the constructor.
- Neither option changes the resolved or rejected value of any operation, nor the payload or timing of any event.

The storage location (`url`, `url/db`, `url/db/cl`, their normalized and absolute paths in the usual escaped forms and with the drive letter in either case, and the credentials of a `scheme://user:pass@host` form) is never printed: in every channel (default, `logDetail` and `funLog`) it is replaced by `***`. One exception is deliberate: a form shorter than 4 characters (for example `url: 'db'`) is not masked on its own, because that would blank unrelated text; the full storage path and the absolute path are still masked. The same holds for the error thrown by the constructor when LMDB cannot open the storage folder. Such an error used to echo the resolved path, so a `url` that carried credentials reached whatever logged the error; now the message keeps its prefix and only the path is replaced, for example `ENOENT: no such file or directory, mkdir '***'`. The class (`Error`, or `TypeError` for a `url` with a NUL character), `code`, `errno` and `syscall` are unchanged, `path` is `'***'`, and properties other than these (such as `cause`) are not carried over.

Not covered by these options: what the underlying `lmdb-js` driver writes itself. Its JavaScript layer calls `console.error` or `console.warn` on commit failures, an invalid read transaction and the like; its native layer writes straight to `stderr` (not through `console`). Almost all of these lines are fixed texts, flags or addresses. The exception is a record that cannot be decompressed (a damaged database file, or a record written by another writer whose first byte looks like a compression marker): the native layer then prints `Failed to decompress data ... bytes:` followed by the byte values of that record, which can include the row's contents. Neither `logDetail` nor `funLog` can capture or mask this; treat the `stderr` of a process that reads a damaged database as sensitive.

### Breaking changes (破壞性變更: diagnostic output)

- Changed: a throwing `change` or `error` subscriber used to print `console.log(<the thrown value>)`, that is its message, stack and whatever data the error carried. It now prints only `change handler threw { mode, errType[, errCode] }` or `error handler threw { mode, errType[, errCode] }`. To get the old line back pass `logDetail: true` (the storage location in it is masked as `***`), or take the full detail from `funLog`.
- Changed: the error thrown by the constructor when `open()` fails no longer contains the storage path (`***` instead); prefix, class, `code`, `errno` and `syscall` are unchanged; `cause` and other extra properties are not carried over.
- Unchanged: every resolved or rejected value, every event payload and the timing of events.

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

    // //select by regex //系列條件白名單不支援$regex(含$options), 會整批reject(invalid find: unsupported operator), 須改為select後自行過濾
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
// error del invalid id: not a string at [data[0].id]
// change del
// del by data without id then [ { n: 0, nDeleted: 0, ok: 0, err: 'invalid id: not a string at [data[0].id]' } ]
// change insertBulk
// insertBulk then { n: 2, nInserted: 2, ok: 1 }
// error insertBulk can not insertBulk by existed id[id-peter]
// insertBulk by data with existed id catch Error: can not insertBulk by existed id[id-peter]
// ids after insertBulk [ 'id-bulk1', 'id-bulk2', 'id-peter', 'id-rosemary' ]
// change insert
// insert by returnList [ { n: 1, nInserted: 0, ok: 1 }, { n: 1, nInserted: 1, ok: 1 } ]
// fresh by returnList [ 'id-new1' ]
```
