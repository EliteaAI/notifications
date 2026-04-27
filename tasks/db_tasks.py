"""Database maintenance tasks for notifications."""

import json

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
    except Exception as e:  # pylint: disable=W0703
        log.exception("create_notifications_user_id_index: failed")


def _build_message_for_row(event_type, meta):
    """Synthesise meta['message'] for a single row. Returns None if unable to build."""
    et = event_type or ''

    if et == 'index_data_changed':
        index_name = meta.get('index_name') or 'Index'
        error = (meta.get('error') or '').strip()
        reindex = meta.get('reindex')
        indexed = meta.get('indexed') or 0
        updated = meta.get('updated') or 0
        initiator = meta.get('initiator', '')
        link = f'[{index_name}]()'
        if error:
            return f'Index {link} is failed.'
        if reindex:
            scheduled_text = ' by schedule' if initiator == 'schedule' else ''
            return f'Index {link} is successfully reindexed{scheduled_text}. {{"reindexed": {updated}, "indexed": {indexed}}}'
        return f'Index {link} is successfully created: {{"indexed": {indexed}}}'

    if et == 'chat_user_added':
        conv = meta.get('conversation_name') or 'chat'
        initiator = meta.get('initiator_name')
        link = f'[{conv}]()'
        if initiator:
            return f'{initiator} added you to {link}'
        return f'You were added to {link}'

    if et == 'private_project_created':
        return 'Project was successfully created'

    if et == 'personal_access_token_expiring':
        token = meta.get('token_name') or 'your token'
        return (
            f'Your personal access token {token} will expire in 24 hours. '
            f'After expiration, it will no longer work. '
            f'You can delete and recreate a new token if needed. '
            f'[Manage Personal Access Tokens]()'
        )

    if et == 'bucket_expiration_warning':
        bucket = meta.get('bucket_name') or 'bucket'
        return (
            f'Bucket [{bucket}]() will start deleting files '
            f'in 24 hours according to its retention policy (files are removed based '
            f"on each file's creation date; the bucket itself will remain)."
        )

    if et == 'agent_unpublished':
        version_id = meta.get('source_version_id')
        app_id = meta.get('source_application_id')
        project_id = meta.get('project_id') or meta.get('source_project_id')
        reason = meta.get('reason') or ''
        reason_suffix = f' Reason: {reason}' if reason else ''
        if app_id and version_id:
            version_ref = f'[{version_id}]()'
        else:
            version_ref = str(version_id) if version_id else 'unknown'
        return f'Unpublished agent version id: {version_ref} from project id: {project_id}.{reason_suffix}'

    # Legacy moderation types
    if et in ('moderator_approval_of_version', 'prompt_moderation_approve'):
        name = meta.get('prompt_name') or meta.get('name') or 'Prompt'
        return f'{name} is published.'
    if et in ('moderator_reject_of_version', 'moderator_unpublish', 'prompt_moderation_reject'):
        name = meta.get('prompt_name') or meta.get('name') or 'Prompt'
        return f'{name} is rejected.'
    if et == 'author_approval':
        name = meta.get('prompt_name') or meta.get('name') or 'Prompt'
        approver = meta.get('approver_name') or ''
        suffix = f' by {approver}' if approver else ''
        return f'{name} is approved{suffix} for publishing.'
    if et == 'author_reject':
        name = meta.get('prompt_name') or meta.get('name') or 'Prompt'
        approver = meta.get('approver_name') or ''
        suffix = f' by {approver}' if approver else ''
        return f'{name} is rejected{suffix}.'
    if et in ('token_expiring', 'token_is_expired'):
        token = meta.get('token_name') or 'token'
        verb = 'is expired' if et == 'token_is_expired' else 'will expire in 5 days'
        return f'Token {token} {verb}. For more details view your Configuration.'
    if et in ('spending_limit_expiring', 'spending_limit_is_expired'):
        verb = 'is expired' if et == 'spending_limit_is_expired' else 'is expiring'
        return f'Your spending limit {verb}. For more details view your settings section.'
    if et == 'rates':
        count = meta.get('rates_count', '')
        name = meta.get('prompt_name') or 'prompt'
        return f'{count} new rate(s) on {name}.'
    if et == 'comments':
        count = meta.get('comments_count', '')
        name = meta.get('prompt_name') or 'prompt'
        return f'{count} new comment(s) on {name}.'
    if et == 'reward_new_level':
        level = meta.get('new_level') or ''
        return f"Congratulations! You've got the {level} level of prompt expert!"
    if et == 'contributor_request_for_publish_approve':
        author = meta.get('author_name') or ''
        name = meta.get('prompt_name') or 'prompt'
        return f'{author} requested publish approval for {name}.'
    if et == 'user_was_added_to_some_project_as_teammate':
        users = meta.get('users') or []
        project = meta.get('project_name') or 'project'
        users_str = ', '.join(users) if users else 'You'
        verb = 'are' if len(users) > 1 else 'is'
        return f'{users_str} {verb} added into {project}.'

    return None


def notifications_backfill_messages(*args, **kwargs):
    """Backfill meta['message'] for notifications; params: 'force' re-generates existing, 'dry_run' previews without writing."""
    param = kwargs.get("param", "")
    force = "force" in param
    dry_run = "dry_run" in param
    updated = 0
    skipped = 0
    errors = 0

    try:
        if force and not dry_run:
            with db.get_session() as session:
                result = session.execute(
                    text(
                        "UPDATE centry.notifications "
                        "SET meta = meta - 'message' "
                        "WHERE meta ? 'message'"
                    )
                )
                session.commit()
            log.info("notifications_backfill_messages: force reset cleared message from %d rows", result.rowcount)

        sql = text(
            "SELECT id, event_type, meta FROM centry.notifications"
            if (force and dry_run) else
            "SELECT id, event_type, meta FROM centry.notifications "
            "WHERE meta->>'message' IS NULL"
        )

        batch_size = 500
        pending = []
        sample_messages = []

        def _flush_batch(batch):
            nonlocal updated, errors
            try:
                with db.get_session() as session:
                    session.execute(
                        text(
                            "UPDATE centry.notifications "
                            "SET meta = jsonb_set(meta, '{message}', CAST(:msg AS jsonb)) "
                            "WHERE id = :id"
                        ),
                        batch,
                    )
                    session.commit()
                updated += len(batch)
            except Exception as e:  # pylint: disable=W0703
                log.warning("notifications_backfill_messages: batch failed, retrying row-by-row: %s", e)
                for item in batch:
                    try:
                        with db.get_session() as session:
                            session.execute(
                                text(
                                    "UPDATE centry.notifications "
                                    "SET meta = jsonb_set(meta, '{message}', CAST(:msg AS jsonb)) "
                                    "WHERE id = :id"
                                ),
                                item,
                            )
                            session.commit()
                        updated += 1
                    except Exception as row_e:  # pylint: disable=W0703
                        log.warning("notifications_backfill_messages: failed row %d: %s", item["id"], row_e)
                        errors += 1

        log.info("notifications_backfill_messages: starting (batch_size=%d)", batch_size)

        with db.get_session() as session:
            cursor = session.execute(sql)
            while True:
                chunk = cursor.fetchmany(batch_size)
                if not chunk:
                    break
                for row in chunk:
                    row_id, event_type, meta = row
                    if not isinstance(meta, dict):
                        skipped += 1
                        continue
                    message = _build_message_for_row(event_type, meta)
                    if message is None:
                        skipped += 1
                        continue
                    if dry_run:
                        if len(sample_messages) < 20:
                            sample_messages.append({"id": row_id, "event_type": event_type, "message": message})
                        updated += 1
                        continue
                    pending.append({"id": row_id, "msg": json.dumps(message)})
                    if len(pending) >= batch_size:
                        _flush_batch(pending)
                        pending = []

        if pending:
            _flush_batch(pending)

        if dry_run:
            log.info(
                "notifications_backfill_messages: dry_run — would_update=%d would_skip=%d",
                updated, skipped,
            )
            for sample in sample_messages:
                log.info(
                    "  [%d] %s → %s",
                    sample["id"], sample["event_type"], sample["message"],
                )
            return

        log.info(
            "notifications_backfill_messages: done — updated=%d skipped=%d errors=%d",
            updated, skipped, errors,
        )

    except Exception as e:  # pylint: disable=W0703
        log.exception("notifications_backfill_messages: fatal error")
