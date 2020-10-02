/* eslint-disable no-unused-vars */
/***
 * This file contains UDF transforms for the transaction Dataflow pipelines
 */


/**
 * transforms the inJson transaction into a top level transaction representation
 * (removing its child elements)
 * @param {*} inJson
 */
function transformTransactionTopLevel(inJson) {
    var result = {}
    var obj = JSON.parse(inJson)
    result.updated_at = isoDateToBQDate(obj.updated_at.iso) //date
    result.update_id = obj.update_id //string
    result.dev_uuid = obj.dev_uuid //string
    result.transaction_number = obj.transaction_number //number
    result.balance_number = obj.balance_number //number
    result.register_number = obj.register_number //number
    result.guid = obj.guid //string
    result.date = isoDateToBQDate(obj.date) // Date
    result.cashier_staff_number = obj.cashier_staff_number //string
    result.branch_fa_ident = obj.branch_fa_ident //string
    result.geo_data = obj.geo_data //string
    result.currency_iso_code = obj.currency_iso_code //string
    result.total = obj.total //number
    result.change = obj.change //number
    result.fr_receipt_id = obj.fr_receipt_id //string
    result.customer_id = obj.customer_id //string
    result.id = obj.id //number
    result.type_name = obj.type_name //string
    result.receipt_text = obj.receipt_text //string
    result.created_at = isoDateToBQDate(obj.created_at.iso) //date
    result.customer_number = obj.customer_number //number
    result.refunded_at = obj.refunded_at //string
    result.temporary_id = obj.temporary_id //string
    result._id = obj._id //string
    result.customer_receipt = obj.customer_receipt //string
    result.merchant_receipt = obj.merchant_receipt //string
    result.barcode = obj.barcode //string
    result.customer_description = obj.customer_description //string
    result.signed_transactions = obj.signed_transactions //string
    result.client_id = obj.client_id //string
    result.cashier_staff = obj.cashier_staff //string
    result.client = obj.client //string
    result.customer = obj.customer //string
    result.timezone = obj.timezone //string
    result.has_promotion = obj.has_promotion //boolean
    result.register = obj.register //string
    result._type = obj._type //string
    result._custom_id = obj._custom_id //string
    result._staff = obj._staff //string
    result._external_reference_id = obj._external_reference_id //string
    result._customer_external_reference_id = obj._customer_external_reference_id //string
    result.branch = obj._branch //string
    result.context = obj.context //string
    result.expense = obj.expense //string

    return JSON.stringify(result);
}

/**
 * transforms the cart items. As with any child array, the results
 * should be organized as an output object in the form of {results: [...]}
 * @param {*} inJson
 */
function transformCartsArray(inJson) {
    var itemsArr = []
    var parsed = JSON.parse(inJson)
    var cartItems = parsed.cartitems
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
        currItem.quantity = cartItem.quantity //INTEGER
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
        currItem.discounts = cartItem.discounts //STRING
        currItem.context = cartItem.context //RECORD/NULLABLE
        currItem.expense = cartItem.expense //RECORD/NULLABLE

        itemsArr.push(currItem)
    })
    return JSON.stringify({results: itemsArr});

}

function isoDateToBQDate(dateStr) {
    if (!dateStr) return null
    var dateObj = new Date(dateStr)
    return dateObj.toISOString().split('.')[0]
}

function intToFloat(num, decPlaces) {
    if (!num) return null
    var floatAsStr = num.toFixed(decPlaces)
    return floatAsStr + '@@@FLOAT'
}

