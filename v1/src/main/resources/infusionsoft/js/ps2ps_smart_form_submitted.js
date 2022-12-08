function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toSpecific(event));

    return JSON.stringify(pubsub);
}

function toSpecific(event) {
    var specific = {};

    specific.account_id = event.accountId;
    specific.contact_id = event.contactId;
    specific.channel_name = 'smart_forms';
    specific.event_type = 'smart_form_submitted';
    specific.source_type = 'smart_form';
    specific.source_id = event.formInstanceId;
    specific.event_data = toEventData(event);

    return specific;
}

function toEventData(event) {
    var eventData = {};

    if (event.data) {
        eventData.data = event.data;

    } else {
        eventData.data = {};
    }

    return eventData;
}