function transform(message) {
    var event = JSON.parse(message);
    var eventType = toEventType(event.type);
    var pubsub = {};

    pubsub.events = [];

    if (eventType && event.contact_id) {
        var canonical = {};

        canonical.account_id = event.app_id;
        canonical.contact_id = event.contact_id;
        canonical.channel_name = 'communications';
        canonical.event_type = eventType;

        pubsub.events.push(canonical);
    }

    return JSON.stringify(pubsub);
}

function toEventType(type) {
    var eventType = null;

    if ("message-received" === type) {
        eventType = "incoming_sms";

    } else if ("message-sent" === type) {
        eventType = "outgoing_sms";

    } else if ("call-incoming-complete" === type) {
        eventType = "incoming_call";

    } else if ("call-outgoing-complete" === type) {
        eventType = "outgoing_call";
    }

    return eventType;
}