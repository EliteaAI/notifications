from flask import request
from tools import api_tools, auth, db, config as c, serialize

from sqlalchemy import desc, asc, or_, cast, String
from ...models.all import Notification
from ...models.pd.notification import (
    NotificationBaseModel,
    NotificationBulkUpdateModel,
    NotificationBulkDeleteModel,
    NotificationBulkUpdateResponseModel,
    NotificationBulkDeleteResponseModel,
)
from ....elitea_core.utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.notifications.notifications.list"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        },
    })
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        with db.get_session() as session:
            user_id = auth.current_user().get("id")
            limit = request.args.get('limit', default=10, type=int)
            offset = request.args.get('offset', default=0, type=int)
            sort_by = request.args.get('sort_by', default='created_at')
            sorting_by = getattr(Notification, sort_by)
            sort_order = request.args.get('sort_order', default='desc')
            sorting = desc if sort_order == 'desc' else asc
            only_new = request.args.get('only_new', False)
            only_total = request.args.get('only_total', False)
            search = request.args.get('search', default=None, type=str)
            event_type = request.args.get('event_type', default=None, type=str)

            query = session.query(
                Notification
            ).filter(
                Notification.user_id == user_id,
            )
            if only_new:
                query = query.filter(
                    Notification.is_seen == False
                )
            if search:
                search_pattern = f'%{search}%'
                query = query.filter(
                    or_(
                        Notification.event_type.ilike(search_pattern),
                        cast(Notification.meta, String).ilike(search_pattern),
                    )
                )
            if event_type:
                query = query.filter(Notification.event_type == event_type)

            total = query.count()
            if only_total:
                return {'total': total}, 200

            result = query.order_by(sorting(sorting_by)).limit(limit).offset(offset).all()
            serialized = [
                serialize(NotificationBaseModel.from_orm(i)) for i in result
            ]

            return {
                'total': total,
                'rows': serialized
            }, 200

    @auth.decorators.check_api({
        "permissions": ["models.notifications.notification.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def put(self, project_id: int, **kwargs):
        try:
            payload = NotificationBulkUpdateModel(**request.json)
        except Exception as e:
            return {"ok": False, "error": str(e)}, 400
        with db.get_session() as session:
            user_id = auth.current_user().get("id")
            notifications = session.query(Notification).filter(
                Notification.id.in_(payload.ids),
                Notification.user_id == user_id,
            ).all()
            for notification in notifications:
                notification.is_seen = payload.is_seen
            session.commit()
            return NotificationBulkUpdateResponseModel(updated=len(notifications)).dict(), 200

    @auth.decorators.check_api({
        "permissions": ["models.notifications.notification.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, **kwargs):
        try:
            payload = NotificationBulkDeleteModel(**request.json)
        except Exception as e:
            return {"ok": False, "error": str(e)}, 400
        with db.get_session() as session:
            user_id = auth.current_user().get("id")
            notifications = session.query(Notification).filter(
                Notification.id.in_(payload.ids),
                Notification.user_id == user_id,
            ).all()
            count = len(notifications)
            for notification in notifications:
                session.delete(notification)
            session.commit()
            return NotificationBulkDeleteResponseModel(deleted=count).dict(), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
