/* eslint-disable no-unused-vars */
/***
 * This file contains UDF transforms for the transaction Dataflow pipelines
 */


/**
 * transforms the inJson transaction into a top level transaction representation
 * and calls other functions to map its RECORD child elements
 * @param {*} inJson
 */
function transformFlatTableTransaction(inJson) {
    var result = {}
    var obj = JSON.parse(inJson)
    result.updated_at = isoDateToBQDate(obj.updated_at.iso)
    result.update_id = obj.update_id
    result.dev_uuid = obj.dev_uuid
    result.transaction_number = obj.transaction_number
    result.balance_number = obj.balance_number
    result.register_number = obj.register_number
    result.guid = obj.guid
    result.date = isoDateToBQDate(obj.date)
    result.cashier_staff_number = obj.cashier_staff_number
    result.branch_fa_ident = obj.branch_fa_ident
    result.geo_data = obj.geo_data
    result.currency_iso_code = obj.currency_iso_code
    result.total = obj.total
    result.change = obj.change
    result.fr_receipt_id = obj.fr_receipt_id
    result.customer_id = obj.customer_id
    result.id = obj.id
    result.type_name = obj.type_name
    result.receipt_text = obj.receipt_text
    result.created_at = isoDateToBQDate(obj.created_at.iso)
    result.customer_number = obj.customer_number
    result.refunded_at = obj.refunded_at
    result.temporary_id = obj.temporary_id
    result._id = obj._id
    result.customer_receipt = obj.customer_receipt
    result.merchant_receipt = obj.merchant_receipt
    result.barcode = obj.barcode
    result.customer_description = obj.customer_description
    result.signed_transactions = obj.signed_transactions
    result.client_id = obj.client_id
    result.cashier_staff = obj.cashier_staff
    result.client = obj.client
    result.customer = obj.customer
    result.timezone = obj.timezone
    result.has_promotion = obj.has_promotion
    result.register = obj.register
    result._type = obj._type
    result._custom_id = obj._custom_id
    result._staff = obj._staff
    result._external_reference_id = obj._external_reference_id
    result._customer_external_reference_id = obj._customer_external_reference_id
    result.branch = obj._branch
    result.branch_number = obj.branch_number

    // Map complex child elements:
    result.cartitems = transformCartsArray(obj.cartitems).results //RECORD/REPEATED
    result._context = transform_ContextsArray(obj._context) //RECORD/NULLABLE
    result.context = transformContextsArray(obj.context).results //RECORD/REPEATED
    result.expense = transformExpense(obj.expense) //RECORD/NULLABLE
    result.payments = transformPaymentsArray(obj.payments).results //RECORD/REPEATED
    result.related_transactions = transformRelationsArray(obj.related_transactions).results //RECORD/REPEATED

    return JSON.stringify({results: [result]});
}

/**
 * transforms the cart items. As with any child array, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformCartsArray(cartItems) {
    var itemsArr = []
    if (cartItems === null) {
        return {results: []}
    }
    cartItems.forEach(function (cartItem) {
        var currItem = {}
        currItem.id = cartItem.id //INTEGER
        currItem.updated_at = isoDateToBQDate(cartItem.updated_at.iso) //DATETIME
        currItem.update_id = cartItem.update_id //INTEGER
        currItem.fr_transaction_id = cartItem.fr_transaction_id //INTEGER
        currItem.position = cartItem.position //INTEGER
        currItem.quantity = intToFloat(cartItem.quantity, 2) //FLOAT
        currItem.article_number = cartItem.article_number //INTEGER
        currItem.article_attr_desc = cartItem.article_attr_desc //STRING
        currItem.article_price = intToFloat(cartItem.article_price, 2) //FLOAT
        currItem.selling_price = intToFloat(cartItem.selling_price, 2) //FLOAT
        currItem.selling_price_total = intToFloat(cartItem.selling_price_total, 2) //FLOAT
        currItem.article_fa_account_number = cartItem.article_fa_account_number //STRING
        currItem.vat_percentage = intToFloat(cartItem.vat_percentage, 2) //FLOAT
        currItem.vat_fa_account_number = cartItem.vat_fa_account_number //STRING
        currItem.vat_amount = intToFloat(cartItem.vat_amount, 2) //FLOAT
        currItem.salesman_staff_number = cartItem.salesman_staff_number //STRING
        currItem.product_number = cartItem.product_number //STRING
        currItem.article_id = cartItem.article_id //INTEGER
        currItem.created_at = isoDateToBQDate(cartItem.created_at.iso) //DATETIME
        currItem.vat_amount_euro = intToFloat(cartItem.vat_amount_euro, 2) //FLOAT
        currItem.discount_amount = intToFloat(cartItem.discount_amount, 2) //FLOAT
        currItem.product_name = cartItem.product_name //STRING
        currItem.guid = cartItem.guid //STRING
        currItem.product_group_number = cartItem.product_group_number //STRING
        currItem.product_group_id = cartItem.product_group_id //INTEGER
        currItem.is_refund = cartItem.is_refund //INTEGER
        currItem.temporary_id = cartItem.temporary_id //STRING
        currItem._id = cartItem._id //STRING
        currItem.related_transaction_number = cartItem.related_transaction_number //INTEGER
        currItem.related_transaction_date = isoDateToBQDate(cartItem.related_transaction_date) //DATETIME
        currItem.related_transaction_type = cartItem.related_transaction_type //STRING
        currItem.client_id = cartItem.client_id //STRING
        currItem.article = cartItem.article //STRING
        currItem.attributes_description = cartItem.attributes_description //STRING
        currItem.salesman_staff = cartItem.salesman_staff //STRING
        currItem.is_tip = cartItem.is_tip //BOOLEAN
        currItem.is_owner = cartItem.is_owner //BOOLEAN
        currItem.reference_cartitem_client_id = cartItem.reference_cartitem_client_id //STRING
        currItem.configuration = cartItem.configuration //RECORD
        currItem.is_service = cartItem.is_service //BOOLEAN
        currItem.used_barcode = cartItem.used_barcode //STRING
        currItem.is_voucher = cartItem.is_voucher //BOOLEAN
        currItem.type = cartItem.type //STRING
        currItem.branch = cartItem.branch //STRING
        currItem.register = cartItem.register //STRING
        currItem._qty = cartItem._qty //INTEGER
        currItem._unit = cartItem._unit //STRING
        currItem._currency = cartItem._currency //STRING
        currItem._product = cartItem._product //STRING
        currItem._custom_id = cartItem._custom_id //STRING
        currItem._tax = cartItem._tax //STRING
        currItem._vat_rate = intToFloat(cartItem._vat_rate, 2) //FLOAT
        currItem._taxes = cartItem._taxes //RECORD/REPEATED
        currItem._account = cartItem._account //STRING
        currItem._product_group = cartItem._product_group //STRING
        currItem._tax_amount = intToFloat(cartItem._tax_amount, 2) //FLOAT
        currItem._tax_amount_total = intToFloat(cartItem._tax_amount_total, 2) //FLOAT
        currItem._discount_amount = intToFloat(cartItem._discount_amount, 2) //FLOAT
        currItem._discount_amount_total = intToFloat(cartItem._discount_amount_total, 2) //FLOAT
        currItem._promotion_amount = intToFloat(cartItem._promotion_amount, 2) //FLOAT
        currItem._promotion_amount_total = intToFloat(cartItem._promotion_amount_total, 2) //FLOAT
        currItem._discounts = cartItem._discounts //RECORD/REPEATED
        currItem._origins = cartItem._origins //RECORD/REPEATED
        currItem._depends_on = cartItem._depends_on //RECORD/REPEATED
        currItem._related_to = cartItem._related_to //RECORD/REPEATED
        currItem._product_service_answers = cartItem._product_service_answers //RECORD/REPEATED
        currItem._comments = cartItem._comments //STRING
        currItem._external_reference_id = cartItem._external_reference_id //STRING
        currItem._amount_net = intToFloat(cartItem._amount_net, 2) //FLOAT
        currItem._amount_gross = intToFloat(cartItem._amount_gross, 2) //FLOAT
        currItem._amount_total_net = intToFloat(cartItem._amount_total_net, 2) //FLOAT
        currItem._amount_total_gross = intToFloat(cartItem._amount_total_gross, 2) //FLOAT
        currItem._amount_unit_net = intToFloat(cartItem._amount_unit_net, 2) //FLOAT
        currItem._amount_unit_gross = intToFloat(cartItem._amount_unit_gross, 2) //FLOAT
        // currItem._context = cartItem._context //MAP SEPARATELY!
        currItem.product_supplier_name = cartItem.product_supplier_name //STRING
        currItem.custom_properties = JSON.stringify(cartItem.custom_properties) //STRING - SCHEMALESS!
        currItem.product = cartItem.product //STRING
        currItem.account = cartItem.account //STRING
        currItem.tax = cartItem.tax //STRING
        currItem.product_group = cartItem.product_group //STRING
        currItem.discounts = cartItem.discounts //RECORD/REPEATED
        currItem.context = cartItem.context //RECORD/NULLABLE
        currItem.expense = cartItem.expense //RECORD/NULLABLE

        itemsArr.push(currItem)
    })
    return {results: itemsArr};

}

/**
 * transforms the _contexts items. As with any child array/object, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transform_ContextsArray(context) {
    if (context === null) {
        return null
    }
    return context
}

/**
 * transforms the contexts items. As with any child array/object, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformContextsArray(con) {
    var itemsArr = []
    if (con === null) {
        return {results: []}
    }
    var item = {}
    item.name = con.name
    item.value = con.value
    item.fr_transaction_id = con.fr_transaction_id
    item.updated_at = isoDateToBQDate(con.updated_at)
    item.created_at = isoDateToBQDate(con.created_at)
    item.update_id = con.update_id
    item._id = con._id

    itemsArr.push(item)
    return {results: itemsArr};
}

/**
 * transforms the expenses items. As with any child array, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformExpense(ex) {
    if (ex === null) {
        return null
    }
    var item = {}
    item.id = ex.id
    item.fr_transaction_id = ex.fr_transaction_id
    item.expense_account_name = ex.expense_account_name
    item.expense_account_type = ex.expense_account_type
    item.expense_fa_account = ex.expense_fa_account
    item.updated_at = isoDateToBQDate(ex.updated_at.iso)
    item.created_at = isoDateToBQDate(ex.created_at.iso)
    item.update_id = ex.update_id
    item.guid = ex.guid
    item.safe_id = ex.safe_id
    item.temporary_id = ex.temporary_id
    item._id = ex._id

    return item;
}

/**
 * transforms the payments items. As with any child array, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformPaymentsArray(payments) {
    var itemsArr = []
    if (payments === null) {
        return {results: []}
    }
    payments.forEach(function (py) {
        var item = {}
        item.id = py.id
        item.updated_at = isoDateToBQDate(py.updated_at.iso)
        item.created_at = isoDateToBQDate(py.created_at.iso)
        item.update_id = py.update_id
        item.fr_transaction_id = py.fr_transaction_id
        item.fa_account_number = py.fa_account_number
        item.cost_center = py.cost_center
        item._note = py._note
        item.amount = intToFloat(py.amount, 2)
        item.currency_iso_code = py.currency_iso_code
        item.exchange_rate = intToFloat(py.exchange_rate, 2)
        item.payment_option_name = py.payment_option_name
        item.payment_option_id = py.payment_option_id
        item.guid = py.guid
        item.temporary_id = py.temporary_id
        item._id = py._id
        item.client_id = py.client_id
        item.payment_option = py.payment_option
        item.date = isoDateToBQDate(py.date)
        item.tip = intToFloat(py.tip, 2)
        item.branch = py.branch
        item.register = py.register
        item._type = py._type
        item._position = py._position
        item._currency = py._currency
        item._amount_total = intToFloat(py._amount_total, 2)
        item._tip_total = intToFloat(py._tip_total, 2)
        item._amount_given = intToFloat(py._amount_given, 2)
        item._amount_requested = intToFloat(py._amount_requested, 2)
        item._amount_change = intToFloat(py._amount_change, 2)
        item._account = py._account
        item._account_change = intToFloat(py._account_change, 2)
        item._comments = py._comments
        item.terminal_response = py.terminal_response
        if (item.context) {
            item.context = py.context
            item.context.updated_at = isoDateToBQDate(item.context.updated_at)
            item.context.created_at = isoDateToBQDate(item.context.created_at)
        }

        if(item._context) {
            item._context = py._context
            item._context.updated_at = isoDateToBQDate(item._context.updated_at)
            item._context.created_at = isoDateToBQDate(item._context.created_at)
            if (item._context.voucher) {
                item._context.voucher.delta_amount = intToFloat(item._context.voucher.delta_amount, 2)
            }
        }
        if (item._origins) {
            item._origins = py._origins
            if (item._origins.date) {
              item._origins.date = isoDateToBQDate(item._origins.date)
            }
        }

        if (item._depends_on) {
            item._depends_on = py._depends_on
            if (item._depends_on.date) {
              item._depends_on.date = isoDateToBQDate(item._depends_on.date)
            }
        }
        item._related_to = py._related_to
        item._external_reference_id = py._external_reference_id

        itemsArr.push(item)
    })
    return {results: itemsArr};
}

/**
 * transforms the relations items. As with any child array, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformRelationsArray(related) {
    var itemsArr = []
    if (related === null) {
        return {results: []}
    }
    related.forEach(function (rel) {
        var item = {}
        item.fr_transaction_id = rel.fr_transaction_id
        item.reference_fr_transaction_id = rel.reference_fr_transaction_id
        item.fr_transaction_guid = rel.fr_transaction_guid
        item.updated_at = isoDateToBQDate(rel.updated_at.iso)
        item.created_at = isoDateToBQDate(rel.created_at.iso)
        item.update_id = rel.update_id
        item._id = rel._id

        itemsArr.push(item)
    })
    return {results: itemsArr};
}

function isoDateToBQDate(dateStr) {
    if (!dateStr) return null
    if (typeof dateStr === 'object') {
        dateStr = dateStr.iso
    }
    var dateObj = new Date(dateStr)
    return dateObj.toISOString().split('.')[0]
}

function intToFloat(num, decPlaces) {
    if (!num) return null
    var floatAsStr = num.toFixed(decPlaces)
    return floatAsStr + '@@@FLOAT'
}

// Uncomment this only when testing failing transactions (the engine uses vanilla JS)
//module.exports.transformCartsArray = transformCartsArray
//module.exports.transformTransactionTopLevel = transformTransactionTopLevel