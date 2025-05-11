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
//   { n: 1, nModified: 1, ok: 1 },
//   { n: 1, nModified: 1, ok: 1 },
//   { n: 0, nModified: 0, ok: 1 }
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
// change del
// del then [ { n: 1, nDeleted: 1, ok: 1 } ]


//node g-basic.mjs
