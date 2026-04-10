from flask import request
from tools import api_tools, auth, db, config as c, serialize

from sqlalchemy import desc, asc
from ...models.all import Notification
from ...models.pd.notification import NotificationBaseModel
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

            query = session.query(
                Notification
            ).filter(
                Notification.user_id == user_id,
            )
            if only_new:
                query = query.filter(
                    Notification.is_seen == False
                )

            total = query.count()
            if only_total:
                return {'total': total}, 200

            result = query.order_by(sorting(sorting_by)).limit(limit).offset(offset).all()
            serialized = [
                serialize(NotificationBaseModel.from_orm(i)) for i in result
            ]

            if not only_new:
                for notification in query:
                    notification.is_seen = True
            session.commit()

            return {
                'total': total,
                'rows': serialized
            }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
