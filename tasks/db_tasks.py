"""Database maintenance tasks for notifications."""

from sqlalchemy import text  # pylint: disable=E0401
from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from tools import db  # pylint: disable=E0401


def create_notifications_user_id_index(*args, **kwargs):
    """Create index on notifications.user_id for query performance. Idempotent."""
    sql = "CREATE INDEX IF NOT EXISTS ix_notifications_user_id ON centry.notifications (user_id);"
    try:
        with db.get_session() as session:
            session.execute(text(sql))
            session.commit()
        log.info("create_notifications_user_id_index: index created (or already existed)")
        return {"status": "ok", "sql": sql}
    except Exception as e:  # pylint: disable=W0703
        log.exception("create_notifications_user_id_index: failed")
        return {"status": "error", "error": str(e)}
