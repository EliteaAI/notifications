def test_added_project_membership_message_uses_the_stored_name(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'project_name': 'Research [Archive]'},
    ) == 'You have been added to Research [Archive] project.'


def test_added_project_membership_message_keeps_legacy_metadata_without_snapshot(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'users': ['Alex']},
    ) == 'Alex is added into project.'


def test_removed_project_membership_message_uses_the_stored_name(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_removed_from_project',
        {'project_name': 'Research [Archive]'},
    ) == 'You have been removed from Research [Archive] project.'


def test_removed_project_membership_message_without_snapshot_is_skipped(db_tasks):
    assert db_tasks._build_message_for_row('user_was_removed_from_project', {}) is None
