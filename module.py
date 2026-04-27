import logging

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import module
from tools import auth

from .tasks import db_tasks

from ..elitea_core.utils.sio_utils import get_event_room


class Module(module.ModuleModel):
    def __init__(self, context, descriptor):
        self.context = context
        self.descriptor = descriptor

    def init(self):
        # self.init_db()
        self.descriptor.init_all()
        self.context.sio.on("connect", handler=self.sio_connect)
        self.context.sio.on("disconnect", handler=self.sio_disconnect)

    def ready(self):
        try:
            from tools import this  # pylint: disable=E0401,C0415
            this.for_module("admin").module.register_admin_task(
                "create_notifications_user_id_index", db_tasks.create_notifications_user_id_index
            )
            this.for_module("admin").module.register_admin_task(
                "notifications_backfill_messages", db_tasks.notifications_backfill_messages
            )
        except Exception as e:
            log.exception("Failed to register admin tasks: %s", e)

    def deinit(self):
        try:
            from tools import this  # pylint: disable=E0401,C0415
            this.for_module("admin").module.unregister_admin_task(
                "create_notifications_user_id_index", db_tasks.create_notifications_user_id_index
            )
            this.for_module("admin").module.unregister_admin_task(
                "notifications_backfill_messages", db_tasks.notifications_backfill_messages
            )
        except Exception as e:
            log.exception("Failed to unregister admin tasks: %s", e)
        self.descriptor.deinit_all()

    # def init_db(self):
    #     from .models import all
    #     project_list = self.context.rpc_manager.call.project_list(filter_={'create_success': True})
    #     for i in project_list:
    #         with db.get_session(i['id']) as tenant_db:
    #             db.get_all_metadata().create_all(bind=tenant_db.connection())
    #             tenant_db.commit()

    @auth.decorators.sio_connect()
    def sio_connect(self, sid, environ):
        """ Connect handler """
        current_user = auth.current_user(
            auth_data=auth.sio_users[sid]
        )
        room_id = str(current_user['id'])
        room = get_event_room(
            event_name='notifications',
            room_id=room_id
        )
        logging.info(f'SIO CONNECT room: {room}')
        if room:
            self.context.sio.enter_room(sid, room)

    @auth.decorators.sio_disconnect()
    def sio_disconnect(self, sid, *args, **kwargs):
        """ Disconnect handler """
