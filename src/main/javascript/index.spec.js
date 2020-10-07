const index = require('./index.js')

function testCartsChild() {
    var cartItem = require('./examples/offendingCartItem.json')
    var result = index.transformCartsArray(JSON.stringify(cartItem))
    console.log(result)
} 

function testTopLevelTx() {
    var tx = require('./examples/offendingTx.json')
    var result = index.transformTransactionTopLevel(JSON.stringify(tx))
    console.log(typeof result)
    console.log(result)
}

// testCartsChild()
testTopLevelTx()