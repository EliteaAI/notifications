import json

from pylon.core.tools import web, log
from tools import db, serialize

from pydantic.v1 import ValidationError
from ..models.all import Notification
from ..models.pd.notification import NotificationBaseModel, NotificationCreateModel
from ...elitea_core.utils.sio_utils import get_event_room, SioEvents


class Event:
    @web.event('notifications_stream')
    def notifications_stream(self, context, event, payload):
        log.info(f'notifications_stream {event=}')
        log.info(f'notifications_stream {payload=}')

        parsed = NotificationCreateModel.parse_obj(payload)

        with db.get_session() as session:
            notification = Notification(
                **parsed.dict()
            )
            session.add(notification)
            session.commit()

            room_id = str(payload['user_id'])
            room = get_event_room(
                event_name='notifications',
                room_id=room_id
            )

            self.context.sio.emit(
                event=SioEvents.notifications_notify,
                data=serialize(NotificationBaseModel.from_orm(notification)),
                room=room,
            )
