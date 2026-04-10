import json

from ...models.all import Notification
from ...models.pd.notification import NotificationBaseModel
from ....elitea_core.utils.constants import PROMPT_LIB_MODE

from tools import api_tools, auth, config as c, db


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.notifications.notification.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, notification_id: int):
        with db.get_session(project_id) as session:
            notification = session.query(Notification).filter(
                Notification.id == notification_id
            ).first()
        if not notification:
            return {"ok": False, "error": "Notification is not found"}, 400
        return NotificationBaseModel.from_orm(notification).model_dump(mode='json'), 200

    @auth.decorators.check_api({
        "permissions": ["models.notifications.notification.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id, notification_id):
        with db.get_session(project_id) as session:
            if notification := session.query(Notification).get(notification_id):
                session.delete(notification)
                session.commit()
                return None, 204
            return {"ok": False, "error": "Notification is not found"}, 400

    @auth.decorators.check_api(
        {
            "permissions": ["models.notifications.notification.update"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, notification_id: int):
        with db.get_session(project_id) as session:
            notification = session.query(Notification).get(notification_id)

            if not notification:
                return {"ok": False, "error": "Notification is not found"}, 400

            notification.is_seen = True
            session.commit()

            return NotificationBaseModel.from_orm(notification).model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:notification_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
