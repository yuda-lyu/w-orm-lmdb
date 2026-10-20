import rollupFiles from 'w-package-tools/src/rollupFiles.mjs'
import getFiles from 'w-package-tools/src/getFiles.mjs'


let fdSrc = './src'
let fdTar = './dist'


rollupFiles({
    fns: 'WOrmLmdb.mjs',
    fdSrc,
    fdTar,
    nameDistType: 'kebabCase',
    globals: {
        'lmdb': 'lmdb',
        'mingo': 'mingo',
        'fs': 'fs',
        'path': 'path',
    },
    external: [
        'lmdb',
        'mingo',
        'fs',
        'path',
    ],
})

