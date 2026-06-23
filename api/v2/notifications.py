from flask import request
from tools import api_tools, auth, db, config as c, serialize, register_openapi

from sqlalchemy import desc, asc
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
    @register_openapi(
        name="List Notifications",
        description="List notifications for the current user with pagination, filtering, and sorting.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"},
             "description": "Project identifier."},
            {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 10},
             "description": "Maximum number of results to return."},
            {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0},
             "description": "Pagination offset."},
            {"name": "sort_by", "in": "query", "schema": {"type": "string", "default": "created_at"},
             "description": "Field to sort by."},
            {"name": "sort_order", "in": "query", "schema": {"type": "string", "default": "desc"},
             "description": "Sort order (asc or desc)."},
            {"name": "only_new", "in": "query", "schema": {"type": "boolean"},
             "description": "Return only unseen notifications."},
            {"name": "only_total", "in": "query", "schema": {"type": "boolean"},
             "description": "Return only the total count, not the rows."},
            {"name": "search", "in": "query", "schema": {"type": "string"},
             "description": "Filter by message text (case-insensitive)."},
            {"name": "event_type", "in": "query", "schema": {"type": "string"},
             "description": "Filter by event type."},
        ],
        available_to_users=True,
    )
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
                words = search.strip().split()
                for word in words:
                    escaped = word.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
                    query = query.filter(
                        Notification.meta['message'].astext.ilike(f'%{escaped}%', escape='\\')
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

    @register_openapi(
        name="Bulk Update Notifications",
        description="Bulk mark notifications as seen or unseen.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"},
             "description": "Project identifier."},
        ],
        request_body={
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "required": ["ids", "is_seen"],
                        "properties": {
                            "ids": {
                                "oneOf": [
                                    {"type": "array", "items": {"type": "integer"}},
                                    {"type": "string", "enum": ["all"]},
                                ],
                                "description": "List of notification IDs, or \"all\" to update all notifications.",
                            },
                            "is_seen": {
                                "type": "boolean",
                                "description": "Mark as seen (true) or unseen (false).",
                            },
                        },
                    },
                    "examples": {
                        "specific_ids": {
                            "summary": "Mark specific notifications as seen",
                            "value": {"ids": [1, 2, 3], "is_seen": True},
                        },
                        "all": {
                            "summary": "Mark all notifications as seen",
                            "value": {"ids": "all", "is_seen": True},
                        },
                    },
                }
            },
        },
        available_to_users=True,
    )
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
            if payload.ids == "all":
                updated_count = session.query(Notification).filter(
                    Notification.user_id == user_id,
                    Notification.is_seen != payload.is_seen,
                ).update({Notification.is_seen: payload.is_seen}, synchronize_session=False)
            else:
                updated_count = session.query(Notification).filter(
                    Notification.id.in_(payload.ids),
                    Notification.user_id == user_id,
                    Notification.is_seen != payload.is_seen,
                ).update({Notification.is_seen: payload.is_seen}, synchronize_session=False)
            session.commit()
            return NotificationBulkUpdateResponseModel(updated=updated_count).dict(), 200

    @register_openapi(
        name="Bulk Delete Notifications",
        description="Bulk delete notifications by a list of ids.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"},
             "description": "Project identifier."},
        ],
        request_body={
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "required": ["ids"],
                        "properties": {
                            "ids": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "description": "List of notification IDs to delete.",
                            }
                        },
                    },
                    "example": {"ids": [1, 2, 3]},
                }
            },
        },
        available_to_users=True,
    )
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
