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
    cartItems.forEach(function (cartItem) {
        var currItem = {}
        currItem.id = cartItem.id //number
        currItem.updated_at = isoDateToBQDate(cartItem.updated_at.iso) //date
        currItem.update_id = cartItem.update_id //number
        currItem.fr_transaction_id = cartItem.fr_transaction_id //number
        currItem.position = cartItem.position //number
        currItem.quantity = cartItem.quantity //number
        currItem.article_number = cartItem.article_number //string?
        currItem.article_attr_desc = cartItem.article_attr_desc //number?
        currItem.article_price = cartItem.article_price //number
        currItem.selling_price = cartItem.selling_price //number
        currItem.selling_price_total = cartItem.selling_price_total //number
        currItem.article_fa_account_number = cartItem.article_fa_account_number //string
        currItem.vat_percentage = cartItem.vat_percentage //number
        currItem.vat_fa_account_number = cartItem.vat_fa_account_number //string
        currItem.vat_amount = cartItem.vat_amount //number
        currItem.salesman_staff_number = cartItem.salesman_staff_number //string
        currItem.product_number = cartItem.product_number //string
        currItem.article_id = cartItem.article_id //string?
        currItem.created_at = isoDateToBQDate(cartItem.created_at.iso) //date
        currItem.vat_amount_euro = cartItem.vat_amount_euro //float
        currItem.discount_amount = cartItem.discount_amount //number
        currItem.product_name = cartItem.product_name //string
        currItem.guid = cartItem.guid //string
        currItem.product_group_number = cartItem.product_group_number //string?
        currItem.product_group_id = cartItem.product_group_id //string?
        currItem.is_refund = cartItem.is_refund //number
        currItem.temporary_id = cartItem.temporary_id //string?
        currItem._id = cartItem._id //string
        currItem.related_transaction_number = cartItem.related_transaction_number //number
        currItem.related_transaction_date = isoDateToBQDate(cartItem.related_transaction_date) //date
        currItem.related_transaction_type = cartItem.related_transaction_type //string
        currItem.client_id = cartItem.client_id //string
        currItem.article = cartItem.article //String
        currItem.attributes_description = cartItem.attributes_description //string
        currItem.salesman_staff = cartItem.salesman_staff //string
        currItem.is_tip = cartItem.is_tip //boolean
        currItem.is_owner = cartItem.is_owner //boolean
        currItem.reference_cartitem_client_id = cartItem.reference_cartitem_client_id //string?
        currItem.configuration__action = cartItem.configuration.action //string?
        currItem.configuration__tax_switched = cartItem.configuration.tax_switched //boolean
        currItem.is_service = cartItem.is_service //boolean
        currItem.used_barcode = cartItem.used_barcode //string?
        currItem.is_voucher = cartItem.is_voucher //boolean
        currItem.type = cartItem.type //string
        currItem.branch = cartItem.branch //string?
        currItem.register = cartItem.register //string?
        currItem._qty = cartItem._qty //number?
        currItem._unit = cartItem._unit //number?
        currItem._currency = cartItem._currency //string?
        currItem._product = cartItem._product //string?
        currItem._custom_id = cartItem._custom_id //string?
        currItem._tax = cartItem._tax //string
        currItem._vat_rate = cartItem._vat_rate //string?
        currItem._taxes = JSON.stringify(cartItem._taxes) //string
        currItem._account = cartItem._account //string
        currItem._product_group = cartItem._product_group //string?
        currItem._tax_amount = cartItem._tax_amount //number
        currItem._tax_amount_total = cartItem._tax_amount_total //number
        currItem._discount_amount = cartItem._discount_amount //number
        currItem._discount_amount_total = cartItem._discount_amount_total //number
        currItem._promotion_amount = cartItem._promotion_amount //number
        currItem._promotion_amount_total = cartItem._promotion_amount_total //number
        currItem._discounts = JSON.stringify(cartItem._discounts) //json
        currItem._origins = JSON.stringify(cartItem._origins) //json
        currItem._depends_on = JSON.stringify(cartItem._depends_on) //json
        currItem._related_to = JSON.stringify(cartItem._related_to) //json
        currItem._product_service_answers = JSON.stringify(cartItem._product_service_answers) //json
        currItem._comments = cartItem._comments //string?
        currItem._external_reference_id = cartItem._external_reference_id //string?
        currItem._amount_net = cartItem._amount_net //number
        currItem._amount_gross = cartItem._amount_gross //number
        currItem._amount_total_net = cartItem._amount_total_net //number
        currItem._amount_total_gross = cartItem._amount_total_gross //number
        currItem._amount_unit_net = cartItem._amount_unit_net //number
        currItem._amount_unit_gross = cartItem._amount_unit_gross //number
        currItem._context = JSON.stringify(cartItem._context) //json
        currItem.product_supplier_name = cartItem.product_supplier_name //string
        currItem.custom_properties = cartItem.custom_properties //string?
        currItem.product = cartItem.product //string
        currItem.account = cartItem.account //string
        currItem.tax = cartItem.tax //string
        currItem.product_group = cartItem.product_group //string
        currItem.discounts = cartItem.discounts //string?
        currItem.context = cartItem.context //string

        itemsArr.push(currItem)
    })
    return JSON.stringify({results: itemsArr});

}

function isoDateToBQDate(dateStr) {
    var dateObj = new Date(dateStr)
    return dateObj.toISOString().split('.')[0]
}
