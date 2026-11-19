//hostile.mjs: 敵意輸入工廠(帳本R-PKGIVEN-01、R-READABLE-01之測試共用; 每次呼叫皆回全新值, 不跨案共用)
//不可讀: 讀取即拋錯之值(已撤銷之Proxy、Symbol.toStringTag拋錯者、ownKeys陷阱拋錯者); 其拋出之文字皆含標記字串, 供斷言「訊息不回顯原例外文字」


//genRevoked, 已撤銷之Proxy: 任何讀取皆拋TypeError(Object.prototype.toString、Array.isArray、Reflect.ownKeys皆拋)
export let genRevoked = () => {
    let r = Proxy.revocable({}, {})
    r.revoke()
    return r.proxy
}

//genTagThrow, Symbol.toStringTag之存取器拋錯: Object.prototype.toString(判斷函數之標籤讀取)拋錯, 一般屬性讀取不受影響
export let genTagThrow = () => {
    return {
        get [Symbol.toStringTag]() {
            throw new Error('TAG-SECRET')
        }
    }
}

//genOwnKeysThrow, ownKeys陷阱拋錯之Proxy: 標籤可讀([object Object])、屬性讀取可行, 僅列舉自有鍵(for...in、Reflect.ownKeys)拋錯
export let genOwnKeysThrow = () => {
    return new Proxy({ a: 1 }, {
        ownKeys() {
            throw new Error('KEYS-SECRET')
        }
    })
}

//genGetterElem0, 長度為1之陣列, 其[0]為存取器且讀取即拋錯(chkReadable不呼叫存取器, 須由呼叫端自讀)
export let genGetterElem0 = () => {
    let a = [0]
    Object.defineProperty(a, '0', {
        get() {
            throw new Error('ELEM0-SECRET')
        },
        enumerable: true,
        configurable: true,
    })
    return a
}

//標記字串(不可出現在任何對外訊息、事件與回傳值)
export let secrets = ['TAG-SECRET', 'KEYS-SECRET', 'ELEM0-SECRET', 'OPT-SECRET', 'SECRET-OBJ', 'SECRET-TS', 'SECRET-ARR', 'SYM-ID']

//不可讀值之全集(名稱與工廠)
export let unreadables = [
    { name: 'revoked Proxy', make: genRevoked },
    { name: 'Symbol.toStringTag throws', make: genTagThrow },
]

//僅ownKeys拋錯者: 標籤可讀, 判斷函數(isestr、isbol等typeof式)不讀其內容, 依既有處理(視為型別不符)
export let ownKeysThrows = { name: 'ownKeys throws', make: genOwnKeysThrow }
