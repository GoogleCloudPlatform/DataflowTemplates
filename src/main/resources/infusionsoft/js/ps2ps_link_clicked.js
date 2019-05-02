function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];
    pubsub.events.push(toSpecific(event));
    pubsub.events.push(toAny(event));

    if (event.category) {
        pubsub.events.push(toCategory(event));
    }

    return JSON.stringify(pubsub);
}

function toSpecific(event) {
    var specific = {};

    specific.account_id = event.accountId;
    specific.contact_id = event.contactId;
    specific.channel_name = 'email';
    specific.event_type = 'link_clicked';
    specific.source_type = 'tracking_link';
    specific.source_id = event.linkId;

    return specific;
}

function toCategory(event) {
    var category = {};

    category.account_id = event.accountId;
    category.contact_id = event.contactId;
    category.channel_name = 'email';
    category.event_type = 'link_clicked';
    category.source_type = 'email_category';
    category.source_id = event.category;

    return category;
}

function toAny(event) {
    var any = {};

    any.account_id = event.accountId;
    any.contact_id = event.contactId;
    any.channel_name = 'email';
    any.event_type = 'link_clicked';

    return any;
}