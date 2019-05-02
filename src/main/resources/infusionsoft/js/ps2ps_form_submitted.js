function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toSpecific(event));
    pubsub.events.push(toCategory(event));
    pubsub.events.push(toAny(event));

    return JSON.stringify(pubsub);
}

function toSpecific(event) {
    var specific = {};

    specific.account_id = event.accountId;
    specific.contact_id = event.contactId;
    specific.channel_name = 'pages';
    specific.event_type = 'form_submitted';
    specific.source_type = 'form';
    specific.source_id = event.formId;
    specific.event_data = toEventData(event);

    return specific;
}

function toCategory(event) {
    var category = {};

    category.account_id = event.accountId;
    category.contact_id = event.contactId;
    category.channel_name = 'pages';
    category.event_type = toEventType(event);
    category.event_data = toEventData(event);

    return category;
}

function toAny(event) {
    var any = {};

    any.account_id = event.accountId;
    any.contact_id = event.contactId;
    any.channel_name = 'pages';
    any.event_type = 'form_submitted';
    any.event_data = toEventData(event);

    return any;
}

function toEventType(event) {
    var eventType;

    if ("LeadCapture" === event.formType) {
        eventType = "lead_capture_form_submitted";

    } else if ("Feedback" === event.formType) {
        eventType = "feedback_form_submitted";

    } else if ("Referral" === event.formType) {
        eventType = "referral_form_submitted";

    } else {
        throw new Error("Unsupported formType: " + event.formType);
    }

    return eventType;
}

function toEventData(event) {
    var eventData = {};

    if (event.headers || event.cookies || event.data) {

        if (event.headers) {
            eventData.headers = event.headers;

        } else {
            eventData.headers = {};
        }

        if (event.cookies) {
            eventData.cookies = event.cookies;

        } else {
            eventData.cookies = {};
        }

        if (event.data) {
            eventData.data = event.data;

        } else {
            eventData.data = {};
        }
    }

    return eventData;
}