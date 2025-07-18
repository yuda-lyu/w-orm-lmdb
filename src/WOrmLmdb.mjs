import { open } from 'lmdb'
import { Query } from 'mingo'
import size from 'lodash-es/size.js'
import get from 'lodash-es/get.js'
import each from 'lodash-es/each.js'
import map from 'lodash-es/map.js'
import merge from 'lodash-es/merge.js'
import isEqual from 'lodash-es/isEqual.js'
import cloneDeep from 'lodash-es/cloneDeep.js'
import isestr from 'wsemi/src/isestr.mjs'
import isarr from 'wsemi/src/isarr.mjs'
import isearr from 'wsemi/src/isearr.mjs'
import iseobj from 'wsemi/src/iseobj.mjs'
import haskey from 'wsemi/src/haskey.mjs'
import evem from 'wsemi/src/evem.mjs'
import genIDSeq from 'wsemi/src/genIDSeq.mjs'
import pmSeries from 'wsemi/src/pmSeries.mjs'
import waitFun from 'wsemi/src/waitFun.mjs'


/**
 * 操作資料庫(LMDB)
 *
 * @class
 * @param {Object} [opt={}] 輸入設定物件，預設{}
 * @param {String} [opt.url='_db'] 輸入資料庫用資料夾字串，預設'_db'
 * @param {String} [opt.db='worm'] 輸入使用資料庫名稱字串，預設'worm'
 * @param {String} [opt.cl='test'] 輸入使用資料表名稱字串，預設'test'
 * @returns {Object} 回傳操作資料庫物件，各事件功能詳見說明
 */
function WOrmLmdb(opt = {}) {

    //url
    let url = get(opt, 'url')
    if (!isestr(url)) {
        url = './_db'
    }

    //db
    let db = get(opt, 'db')
    if (!isestr(db)) {
        db = 'worm'
    }

    //cl
    let cl = get(opt, 'cl')
    if (!isestr(cl)) {
        cl = 'test'
    }

    //storage
    let storage = `${url}/${db}/${cl}`
    // console.log('storage',storage)

    // client
    let client = open({
        path: storage,
        compression: true,
        useVersions: true,
    })

    //ee
    let ee = evem()

    //getData
    let getData = async() => {
        let errTemp = null

        //waitFun
        await waitFun(() => {
            if (client.status === 'closed') {
                console.log(`client.status[${client.status}], level is closed`)
            }
            return client.status === 'open'
        })

        // console.log('client.status',client.status)
        let ltdt = []
        for await (let { value: dt } of client.getRange()) {
            // console.log('dt',dt)
            ltdt.push(dt)
        }

        if (errTemp !== null) {
            return Promise.reject(errTemp)
        }
        return ltdt
    }

    //getValue
    let getValue = async (key) => {
        let value = null
        try {
            value = await client.get(key)
        }
        catch (err) {
            // if (err.notFound) {
            //     console.log('資料不存在')
            // }
            // else {
            //     console.log('其他錯誤:', err)
            // }
        }
        return value
    }

    /**
     * 查詢數據
     *
     * @memberOf WOrmLmdb
     * @param {Object} [find={}] 輸入查詢條件物件
     * @returns {Promise} 回傳Promise，resolve回傳數據，reject回傳錯誤訊息
     */
    async function select(find = {}) {
        let isErr = false
        let res = null

        try {

            //ltdt
            let ltdt = await getData()
            // console.log('select ltdt',ltdt)

            //filter
            if (iseobj(find)) {

                //q
                let q = new Query(find)
                // console.log('q', q)

                //find
                res = q.find(ltdt).all()
                // console.log('select(find) ltdt',res)

            }
            else {
                res = ltdt
            }

            //check
            if (!isarr(res)) {
                isErr = true
                res = `can not select by find[${JSON.stringify(find)}]`
            }

        }
        catch (err) {
            isErr = true
            res = err
        }
        // console.log('res', res)

        //check
        if (isErr) {
            return Promise.reject(res)
        }

        return res
    }

    /**
     * 插入數據
     *
     * @memberOf WOrmLmdb
     * @param {Object|Array} data 輸入數據物件或陣列
     * @returns {Promise} 回傳Promise，resolve回傳插入結果，reject回傳錯誤訊息
     */
    async function insert(data) {
        let isErr = false

        //check
        if (!iseobj(data) && !isearr(data)) {
            return {
                n: 0,
                nInserted: 0,
                ok: 1,
            }
        }

        //cloneDeep, 與外部數據脫勾
        data = cloneDeep(data)

        //res
        let res = null
        try {

            //check
            if (!isarr(data)) {
                data = [data]
            }

            //check id
            data = map(data, function(v) {
                if (!isestr(v.id)) {
                    v.id = genIDSeq()
                }
                return v
            })

            //each
            let nAll = size(data)
            let nPush = 0
            for (let v of data) {
                // console.log(v)

                //查找資料表內v.id
                let vv = await getValue(v.id) //不會有catch
                // console.log('getValue',vv)

                //check
                if (!iseobj(vv)) {
                    //未存在v.id

                    //put
                    await client.put(v.id, v)

                    nPush++
                }
                else {
                    //已存在v.id則不push
                }

            }

            //res
            res = {
                n: nAll,
                nInserted: nPush,
                ok: 1,
            }

            //emit
            ee.emit('change', 'insert', data, res)

        }
        catch (err) {
            isErr = true
            res = err
        }

        if (isErr) {
            return Promise.reject(res)
        }
        return res
    }

    /**
     * 儲存數據
     *
     * @memberOf WOrmLmdb
     * @param {Object|Array} data 輸入數據物件或陣列
     * @param {Object} [option={}] 輸入設定物件，預設為{}
     * @param {boolean} [option.autoInsert=true] 輸入是否於儲存時發現原本無數據，則自動改以插入處理，預設為true
     * @returns {Promise} 回傳Promise，resolve回傳儲存結果，reject回傳錯誤訊息
     */
    async function save(data, option = {}) {
        let isErr = false

        //check
        if (!iseobj(data) && !isearr(data)) {
            return []
        }

        //cloneDeep, 與外部數據脫勾
        data = cloneDeep(data)

        //autoInsert
        let autoInsert = get(option, 'autoInsert', true)

        //res
        let res = null
        try {

            //check
            if (!isarr(data)) {
                data = [data]
            }

            //check id
            data = map(data, function(v) {
                if (!isestr(v.id)) {
                    v.id = genIDSeq()
                }
                return v
            })

            //pmSeries
            res = await pmSeries(data, async(v) => {

                //rest
                let rest = null

                //查找資料表內v.id
                let vv = await getValue(v.id) //不會有catch

                //check
                if (iseobj(vv)) {
                    //已存在v.id
                    if (isEqual(v, vv)) {
                        //內容相同不更新
                    }
                    else {
                        //內容不同須更新

                        //merge and put
                        await client.put(v.id, merge(vv, v))

                        rest = { update: true }
                    }
                }

                //rest
                if (iseobj(rest)) {
                    rest = {
                        n: 1,
                        nModified: 1,
                        ok: 1,
                    }
                }
                else {
                    rest = {
                        n: 0,
                        nModified: 0,
                        ok: 1,
                    }
                }

                //autoInsert
                if (autoInsert && rest.n === 0) {
                    rest = await insert(v)
                }

                return rest
            })

            //emit
            ee.emit('change', 'save', data, res)

        }
        catch (err) {
            isErr = true
            res = err
        }

        if (isErr) {
            return Promise.reject(res)
        }
        return res
    }

    /**
     * 刪除數據
     *
     * @memberOf WOrmLmdb
     * @param {Object|Array} data 輸入數據物件或陣列
     * @returns {Promise} 回傳Promise，resolve回傳刪除結果，reject回傳錯誤訊息
     */
    async function del(data) {
        let isErr = false

        //check
        if (!iseobj(data) && !isearr(data)) {
            return []
        }

        //cloneDeep, 與外部數據脫勾
        data = cloneDeep(data)

        //res
        let res = null
        try {

            //check
            if (!isarr(data)) {
                data = [data]
            }

            //pmSeries
            res = await pmSeries(data, async(v) => {

                //rest
                let rest = null

                //id
                let id = get(v, 'id', '')

                //check
                if (isestr(id)) {

                    //查找資料表內v.id
                    let vv = await getValue(v.id) //不會有catch

                    //check
                    if (iseobj(vv)) {
                        //已存在v.id則須刪除

                        //del
                        await client.del(v.id)

                        //rest
                        rest = {
                            n: 1,
                            nDeleted: 1,
                            ok: 1,
                        }

                    }
                    else {
                        //不存在v.id則不刪除

                        //rest
                        rest = {
                            n: 1,
                            nDeleted: 0,
                            ok: 1,
                        }

                    }

                }
                else {
                    //未給v.id則不刪除

                    //rest
                    rest = {
                        n: 1,
                        nDeleted: 0,
                        ok: 0, //未給v.id視為有問題數據, 故ok給0
                    }

                }

                return rest
            })

            //emit
            ee.emit('change', 'del', data, res)

        }
        catch (err) {
            isErr = true
            res = err
        }

        if (isErr) {
            return Promise.reject(res)
        }
        return res
    }

    /**
     * 刪除全部數據，需與del分開，避免未傳數據導致直接刪除全表
     *
     * @memberOf WOrmLmdb
     * @param {Object} [find={}] 輸入刪除條件物件
     * @returns {Promise} 回傳Promise，resolve回傳刪除結果，reject回傳錯誤訊息
     */
    async function delAll(find = {}) {
        let isErr = false

        //res
        let res = null
        try {

            //ltdt
            let ltdt = await getData()

            //filter
            let nAll = size(ltdt)
            let nDel = 0
            if (iseobj(find)) {

                //q
                let q = new Query(find)
                // console.log('q', q)

                //find
                let _res = q.find(ltdt).all()
                // console.log('_res', _res)

                //nDel
                nDel = size(_res)
                // console.log('nDel', nDel)

                if (nDel === 0) {
                    //未有find結果等於不刪除
                }
                else if (nAll === nDel) {
                    //全在find結果內等於全部刪除

                    //empty
                    for (let v of ltdt) {
                        await client.del(v.id)
                    }

                }
                else {
                    //部份在find結果內

                    //_kp
                    let _kp = {}
                    each(_res, (v, k) => {
                        _kp[v.id] = { k, v }
                    })

                    //del
                    for (let v of ltdt) {
                        if (haskey(_kp, v.id)) {
                            //在find結果內代表須刪除
                            await client.del(v.id)
                        }
                    }

                }
            }
            else {

                //nDel
                nDel = nAll

                //empty
                for (let v of ltdt) {
                    await client.del(v.id)
                }

            }

            //res
            res = {
                n: nAll,
                nDeleted: nDel,
                ok: 1,
            }

            //emit
            ee.emit('change', 'delAll', null, res)

        }
        catch (err) {
            isErr = true
            res = err
        }

        if (isErr) {
            return Promise.reject(res)
        }
        return res
    }

    //save
    ee.select = select
    ee.insert = insert
    ee.save = save
    ee.del = del
    ee.delAll = delAll

    return ee
}


export default WOrmLmdb
