function transform(message) {
    var event = JSON.parse(message);
    var pubsub = {};

    pubsub.events = [];

    if (event.attendees) {
        var numAttendees = event.attendees.length;

        if (numAttendees > 0) {
            var accountId = event.appId;
            var eventType = toEventType(event);
            var eventData = toEventData(event);

            for (var i = 0; i < numAttendees; i++) {
                var attendee = event.attendees[i];

                if (attendee && attendee.contactId) {
                    var contact = {};

                    contact.account_id = accountId;
                    contact.contact_id = attendee.contactId;
                    contact.channel_name = 'appointments';
                    contact.event_type = eventType;
                    contact.event_data = eventData;

                    pubsub.events.push(contact);
                }
            }
        }
    }

    return JSON.stringify(pubsub);
}

function toEventType(event) {
    var eventType;

    if ("CREATED" === event.action) {
        eventType = "appointment_created";

    } else if ("UPDATED" === event.action) {
        eventType = "appointment_updated";

    } else if ("DELETED" === event.action) {
        eventType = "appointment_deleted";

    } else {
        throw new Error("Unsupported action: " + event.action);
    }

    return eventType;
}

function toEventData(event) {
    var eventData = {};

    eventData.appointment_id = event.appointmentId;
    eventData.title = event.summary;
    eventData.description = event.description;
    eventData.start_date = event.start;
    eventData.end_date = event.end;

    return eventData;
}