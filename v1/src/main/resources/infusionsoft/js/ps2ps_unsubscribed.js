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
    specific.channel_name = 'email';
    specific.event_type = 'unsubscribed';
    specific.source_type = 'unsubscribe_link';
    specific.source_id = event.linkId;

    return specific;
}