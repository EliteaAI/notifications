"""Database maintenance tasks for notifications."""

import json
import re

from sqlalchemy import text  # pylint: disable=E0401
from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from tools import db, config as c  # pylint: disable=E0401


SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_LEGACY_SCHEMA = 'centry'


def _is_valid_schema_name(schema_name):
    """Return True when schema name is safe to interpolate into SQL identifiers."""
    return bool(schema_name and SCHEMA_NAME_RE.fullmatch(schema_name))


def _extract_legacy_schema_param(param):
    """Extract optional legacy/source schema override from admin task param string."""
    for segment in [item.strip() for item in (param or '').split(';') if item.strip()]:
        segment_lower = segment.lower()
        if segment_lower.startswith('legacy_schema='):
            return segment[len('legacy_schema='):].strip()
        if segment_lower.startswith('source_schema='):
            return segment[len('source_schema='):].strip()
    return None


def _has_dry_run_flag(param):
    """Return True when admin task param requests dry-run mode."""
    return any(
        segment.strip().lower() == 'dry_run'
        for segment in (param or '').split(';')
        if segment.strip()
    )


def _detect_legacy_notifications_schema(target, explicit_legacy):
    """Resolve source schema for notifications migration."""
    if explicit_legacy:
        if not _is_valid_schema_name(explicit_legacy):
            log.error(
                "notifications_migrate_schema: invalid legacy schema override '%s'",
                explicit_legacy,
            )
            return None
        return explicit_legacy

    candidate_counts = _get_notifications_schema_candidates(target)

    if not candidate_counts:
        log.info(
            "notifications_migrate_schema: no legacy notifications schema found outside target '%s'",
            target,
        )
        return None

    centry_candidate = next(
        (item for item in candidate_counts if item[0] == DEFAULT_LEGACY_SCHEMA),
        None,
    )
    if centry_candidate and target != DEFAULT_LEGACY_SCHEMA:
        schema_name, row_count = centry_candidate
        log.info(
            "notifications_migrate_schema: preferring legacy schema '%s' (rows=%d) over other candidates %s",
            schema_name, row_count, candidate_counts,
        )
        return schema_name

    if len(candidate_counts) == 1:
        schema_name, row_count = candidate_counts[0]
        log.info(
            "notifications_migrate_schema: auto-detected legacy schema '%s' (rows=%d)",
            schema_name, row_count,
        )
        return schema_name

    non_empty_candidates = [item for item in candidate_counts if item[1] > 0]
    if len(non_empty_candidates) == 1:
        schema_name, row_count = non_empty_candidates[0]
        log.info(
            "notifications_migrate_schema: auto-detected legacy schema '%s' from non-empty candidates %s",
            schema_name, candidate_counts,
        )
        return schema_name

    log.error(
        "notifications_migrate_schema: could not auto-detect legacy schema for target '%s'; candidates=%s. "
        "Re-run with param='legacy_schema=<schema>'",
        target,
        candidate_counts,
    )
    return None


def _get_notifications_schema_candidates(target):
    """Return non-target notifications schemas and their row counts."""
    with db.get_session() as session:
        candidate_schemas = session.execute(text(
            "SELECT table_schema "
            "FROM information_schema.tables "
            "WHERE table_name = 'notifications' "
            "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
            "AND table_schema <> :target "
            "ORDER BY table_schema"
        ), {"target": target}).scalars().all()

        candidate_counts = []
        for schema_name in candidate_schemas:
            if not _is_valid_schema_name(schema_name):
                log.warning(
                    "notifications_migrate_schema: ignoring schema with invalid identifier '%s'",
                    schema_name,
                )
                continue
            row_count = session.execute(text(
                f"SELECT COUNT(*) FROM {schema_name}.notifications"
            )).scalar()
            candidate_counts.append((schema_name, row_count))

    return candidate_counts


def notifications_migrate_schema(*args, **kwargs):
    """Move notifications table from legacy schema into c.POSTGRES_SCHEMA, params: legacy_schema=<schema>[;dry_run]."""
    param = kwargs.get("param", "") or ""
    dry_run = _has_dry_run_flag(param)
    target = c.POSTGRES_SCHEMA
    if not _is_valid_schema_name(target):
        log.error("notifications_migrate_schema: invalid target schema '%s'", target)
        return

    explicit_legacy = _extract_legacy_schema_param(param)
    if target == DEFAULT_LEGACY_SCHEMA and not explicit_legacy:
        candidate_counts = _get_notifications_schema_candidates(target)
        if candidate_counts:
            log.info(
                "notifications_migrate_schema: target schema '%s' is already the legacy default, but other notifications schemas exist: %s",
                target,
                candidate_counts,
            )
            log.info(
                "notifications_migrate_schema: to migrate from another schema, re-run with param='legacy_schema=<schema>' (add ';dry_run' to preview first)",
            )
            return
        log.info(
            "notifications_migrate_schema: target schema '%s' is already the legacy default; no migration needed",
            target,
        )
        return

    legacy = _detect_legacy_notifications_schema(target, explicit_legacy)
    if legacy is None:
        return

    if target == legacy:
        log.info("notifications_migrate_schema: POSTGRES_SCHEMA is '%s', no migration needed", target)
        return

    try:
        legacy_row_count = 0
        target_exists = False
        target_row_count = 0
        with db.get_session() as session:
            legacy_exists = session.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = 'notifications')"
            ), {"schema": legacy}).scalar()

            if not legacy_exists:
                log.info(
                    "notifications_migrate_schema: '%s'.notifications not found, nothing to migrate",
                    legacy,
                )
                return

            legacy_row_count = session.execute(text(
                f"SELECT COUNT(*) FROM {legacy}.notifications"
            )).scalar()

            target_exists = session.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = 'notifications')"
            ), {"schema": target}).scalar()

            if target_exists:
                target_row_count = session.execute(text(
                    f"SELECT COUNT(*) FROM {target}.notifications"
                )).scalar()

        if dry_run:
            if not target_exists:
                log.info(
                    "notifications_migrate_schema: dry_run — would move '%s'.notifications to '%s' (legacy_rows=%d)",
                    legacy, target, legacy_row_count,
                )
            else:
                log.info(
                    "notifications_migrate_schema: dry_run — would drop '%s'.notifications (rows=%d) and move '%s'.notifications into '%s' (legacy_rows=%d)",
                    target, target_row_count, legacy, target, legacy_row_count,
                )
            return

        with db.get_session() as session:
            if target_exists:
                if target_row_count > 0:
                    log.warning(
                        "notifications_migrate_schema: dropping '%s'.notifications which contains %d rows",
                        target, target_row_count,
                    )
                else:
                    log.info(
                        "notifications_migrate_schema: dropping empty '%s'.notifications before migration",
                        target,
                    )
                session.execute(text(f"DROP TABLE {target}.notifications"))
            session.execute(text(
                f"ALTER TABLE {legacy}.notifications SET SCHEMA {target}"
            ))
            session.commit()

        log.info(
            "notifications_migrate_schema: moved notifications from '%s' to '%s' (rows=%d)",
            legacy, target, legacy_row_count,
        )
    except Exception:  # pylint: disable=W0703
        log.exception(
            "notifications_migrate_schema: failed to migrate 'notifications' table from legacy schema '%s' to target schema '%s'",
            legacy,
            target,
        )


def create_notifications_user_id_index(*args, **kwargs):
    """Create index on notifications.user_id for query performance. Idempotent."""
    sql = f"CREATE INDEX IF NOT EXISTS ix_notifications_user_id ON {c.POSTGRES_SCHEMA}.notifications (user_id);"
    try:
        with db.get_session() as session:
            session.execute(text(sql))
            session.commit()
        log.info("create_notifications_user_id_index: index created (or already existed)")
    except Exception as e:  # pylint: disable=W0703
        log.exception("create_notifications_user_id_index: failed")


DEFAULT_INDEX_ITEM_LABELS = {'singular': 'document', 'plural': 'documents'}
DEFAULT_INDEX_DEPENDENT_LABELS = {'singular': 'attachment', 'plural': 'attachments'}
INDEX_RETENTION_CLAIM = 'Previously indexed data remains available for search.'


def _index_noun(count, labels, default):
    labels = labels or default
    return labels.get('singular' if count == 1 else 'plural', default['plural'])


def _summarize_index_error(error):
    """Single-line error summary capped at ~200 characters.

    The retention claim belongs to this layer, which decides it from the live chunk count:
    an SDK message that already ends with the sentence would otherwise render it twice. The
    sentence the builder wraps the summary in supplies the closing period too.
    """
    summary = ' '.join(str(error or '').split())
    if summary.endswith(INDEX_RETENTION_CLAIM):
        summary = summary[:-len(INDEX_RETENTION_CLAIM)].rstrip()
    summary = summary.rstrip('.')
    if len(summary) > 200:
        summary = summary[:200].rstrip() + '…'
    return summary


def _index_retains_data(meta):
    """The single retention predicate: `indexed_chunks` is the live pending-excluded
    count recomputed at failure time, so it is the only field that proves searchable
    rows exist. Never gate a retention claim on `reindex` — that is a remembered
    history fact which stays truthy over an EMPTY index (a zero-chunk completed first
    run, a whole-index delete)."""
    try:
        return float(meta.get('indexed_chunks') or 0) > 0
    except (TypeError, ValueError):
        return False


def _build_index_data_message(meta):
    """Message for a finished indexing run.

    TWIN of elitea_core's ``utils/indexing_report.py``, which writes these messages
    live; this copy only backfills rows stored before that existed. Keep both in step.
    """
    index_name = meta.get('index_name') or 'Index'
    link = f'[{index_name}]()'
    state = meta.get('state') or ''
    if state == 'discarded':
        return None

    report = meta.get('report')
    if isinstance(report, str) and report.strip():
        try:
            report = json.loads(report)
        except (TypeError, ValueError):
            report = None
    totals = (report or {}).get('totals') or {}
    item_labels = (report or {}).get('item_labels')

    error = _summarize_index_error(meta.get('error'))

    if state == 'partly_indexed':
        failed = totals.get('failed') or meta.get('failed') or 0
        failed_noun = _index_noun(failed, item_labels, DEFAULT_INDEX_ITEM_LABELS)
        # A partly_indexed run can carry no failure count at all: the SDK's damaged-doc
        # path fills none of the report's FAILED groups, and a backfilled row stored no
        # report either. Name the quantity vaguely rather than claim a zero.
        subject = f'{failed} {failed_noun}' if failed else f'some {failed_noun}'
        verb = 'reindexed' if meta.get('reindex') else 'indexed'
        # The retention clause speaks about the FAILED items, whose chunks this run never
        # wrote, so `indexed_chunks` — which counts the run's own promoted chunks — cannot
        # carry it alone: on a first index those items have no earlier generation at all.
        retained = (
            ' Their previously indexed data remains available for search.'
            if meta.get('reindex') and _index_retains_data(meta) else ''
        )
        return (
            f'Index {link} was partially {verb}: {subject} could not be updated'
            f' ({error}).{retained}'
        )

    if state == 'failed' or (not state and error):
        retained = (
            ' Previously indexed data remains available for search.'
            if _index_retains_data(meta) else ''
        )
        if not meta.get('reindex'):
            return f'Indexing of {link} failed: {error}.{retained}'
        if meta.get('initiator', '') == 'schedule':
            return f'Index {link} scheduled reindex failed: {error}.{retained}'
        return f'Index {link} reindex failed: {error}.{retained}'

    scheduled_text = ' by schedule' if meta.get('initiator', '') == 'schedule' else ''
    unchanged = totals.get('unchanged') or 0
    # Only a report can establish that a run changed nothing; a bare count cannot.
    if report and (totals.get('indexed') or 0) == 0 and not (totals.get('failed') or 0) and unchanged > 0:
        unchanged_noun = _index_noun(unchanged, item_labels, DEFAULT_INDEX_ITEM_LABELS)
        return f'Index {link} is up to date{scheduled_text} — {unchanged} {unchanged_noun} unchanged.'

    indexed = totals.get('indexed', meta.get('indexed') or 0) or 0
    parts = [f'{indexed} {_index_noun(indexed, item_labels, DEFAULT_INDEX_ITEM_LABELS)} indexed']
    for key, wording in (('skipped', 'skipped'), ('not_indexed', 'not indexed'), ('failed', 'failed')):
        count = totals.get(key) or 0
        if count > 0:
            parts.append(f'{count} {wording}')
    if unchanged:
        parts.append(f'{unchanged} unchanged')
    breakdown = ', '.join(parts)

    dependent = totals.get('dependent_not_indexed') or 0
    if dependent:
        dependent_noun = _index_noun(
            dependent, (report or {}).get('dependent_labels'), DEFAULT_INDEX_DEPENDENT_LABELS)
        breakdown += f' ({dependent} {dependent_noun} not indexed)'

    if meta.get('reindex'):
        return f'Index {link} is successfully reindexed{scheduled_text}. {breakdown}.'
    return f'Index {link} is successfully created. {breakdown}.'


def _build_message_for_row(event_type, meta):
    """Synthesise meta['message'] for a single row. Returns None if unable to build."""
    et = event_type or ''

    if et == 'index_data_changed':
        return _build_index_data_message(meta)

    if et == 'chat_user_added':
        conv = meta.get('conversation_name') or 'chat'
        initiator = meta.get('initiator_name')
        link = f'[{conv}]()'
        if initiator:
            return f'{initiator} added you to {link}'
        return f'You were added to {link}'

    if et == 'chat_user_mentioned':
        conv = meta.get('conversation_name') or 'chat'
        initiator = meta.get('initiator_name')
        link = f'[{conv}]()'
        if initiator:
            return f'{initiator} mentioned you in {link}'
        return f'You were mentioned in {link}'

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
                        f"UPDATE {c.POSTGRES_SCHEMA}.notifications "
                        "SET meta = meta - 'message' "
                        "WHERE meta ? 'message'"
                    )
                )
                session.commit()
            log.info("notifications_backfill_messages: force reset cleared message from %d rows", result.rowcount)

        sql = text(
            f"SELECT id, event_type, meta FROM {c.POSTGRES_SCHEMA}.notifications"
            if (force and dry_run) else
            f"SELECT id, event_type, meta FROM {c.POSTGRES_SCHEMA}.notifications "
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
                            f"UPDATE {c.POSTGRES_SCHEMA}.notifications "
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
                                    f"UPDATE {c.POSTGRES_SCHEMA}.notifications "
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
