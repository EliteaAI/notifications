import logging

from pylon.core.tools import module
from tools import db, auth

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

    def deinit(self):
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
