function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toCanonical(event));

    return JSON.stringify(pubsub);
}

function toCanonical(event) {
    var canonical = {};

    canonical.account_id = event.account_id;
    canonical.contact_id = event.contact_id;
    canonical.channel_name = event.channel_name;
    canonical.event_type = event.event_type;
    canonical.source_type = event.source_type;
    canonical.source_id = event.source_id;

    return canonical;
}