function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];

    if ("AddedToList" === event.status) {
        pubsub.events.push(toInto(event));

    } else if ("RemovedFromList" === event.status) {
        pubsub.events.push(toFrom(event));
    }

    return JSON.stringify(pubsub);
}

function toInto(event) {
    var into = {};

    into.account_id = event.accountId;
    into.contact_id = event.contactId;
    into.channel_name = 'smart_lists';
    into.event_type = 'added_to_smart_list';
    into.source_type = 'smart_list';
    into.source_id = event.listId;

    return into;
}

function toFrom(event) {
    var from = {};

    from.account_id = event.accountId;
    from.contact_id = event.contactId;
    from.channel_name = 'smart_lists';
    from.event_type = 'removed_from_smart_list';
    from.source_type = 'smart_list';
    from.source_id = event.listId;

    return from;
}