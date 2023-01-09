function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toInto(event));

    if (event.previous_stage) {
        pubsub.events.push(toFrom(event));
    }

    return JSON.stringify(pubsub);
}

function toInto(event) {
    var into = {};

    into.account_id = event.account_id;
    into.contact_id = event.contact_id;
    into.channel_name = 'contacts';
    into.event_type = 'moved_into_contact_stage';
    into.source_type = 'contact_stage';
    into.source_id = event.current_stage;

    return into;
}

function toFrom(event) {
    var from = {};

    from.account_id = event.account_id;
    from.contact_id = event.contact_id;
    from.channel_name = 'contacts';
    from.event_type = 'moved_from_contact_stage';
    from.source_type = 'contact_stage';
    from.source_id = event.previous_stage;

    return from;
}