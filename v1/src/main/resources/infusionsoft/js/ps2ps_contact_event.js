function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toCanonical(event));

    return JSON.stringify(pubsub);
}

function toCanonical(event) {
    var canonical = {};
    var eventData = event.source_data;

    canonical.account_id = event.account_id;
    canonical.contact_id = event.contact_id;
    canonical.channel_name = event.channel_name;
    canonical.event_type = event.event_type;
    canonical.source_type = event.source_type;
    canonical.source_id = event.source_id;
    canonical.event_data = eventData;

    if (eventData) {
        if (eventData.quote_id) {
            canonical.actor_type = 'quote';
            canonical.actor_id = eventData.quote_id;

        } else if (eventData.invoice_id) {
            canonical.actor_type = 'invoice';
            canonical.actor_id = eventData.invoice_id;
        }
    }

    return canonical;
}
